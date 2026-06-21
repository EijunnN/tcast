//! Real-time one-way voice for tcast: the host captures its microphone and
//! streams raw PCM to viewers, who play it back. No codec, no C toolchain —
//! just `cpal` (pure-Rust device I/O) plus a lock-free jitter buffer.
//!
//! ## Performance model
//!
//! Audio device callbacks run on a high-priority OS thread and must never block
//! or allocate. So:
//!
//! * The `cpal` stream lives on its **own dedicated thread** (streams aren't
//!   `Send`, and this keeps them off the async runtime).
//! * Capture: the input callback downmixes to mono i16 and emits fixed 20 ms
//!   frames over an unbounded channel — one small alloc per 20 ms, never blocks.
//! * Playback: the network thread pushes samples into a **lock-free SPSC ring**
//!   ([`ringbuf`]); the output callback pops from it with no lock and no alloc.
//!   An underrun yields silence (and re-arms a small prebuffer) rather than a
//!   glitch; an overrun drops the oldest samples. Audio timing is therefore
//!   fully decoupled from the UI render loop and the WebSocket.
//!
//! The wire format is fixed at 48 kHz, mono, signed 16-bit little-endian.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{
    FromSample, Sample, SampleFormat, SampleRate, SizedSample, StreamConfig, SupportedStreamConfig,
};
use ringbuf::traits::{Consumer, Observer, Producer, Split};
use ringbuf::HeapRb;
use tokio::sync::mpsc::UnboundedSender;

/// Fixed wire sample rate (Hz).
pub const SAMPLE_RATE: u32 = 48_000;
/// Samples per 20 ms frame at [`SAMPLE_RATE`], mono.
pub const FRAME_SAMPLES: usize = (SAMPLE_RATE as usize / 1000) * 20; // 960

/// Playback prebuffer / jitter target: ~60 ms before audio starts flowing.
const PREBUFFER_SAMPLES: usize = FRAME_SAMPLES * 3;
/// Playback ring capacity: 1 s of audio (overrun drops oldest).
const RING_CAPACITY: usize = SAMPLE_RATE as usize;

/// Encode mono i16 samples to little-endian bytes for the wire.
pub fn encode_frame(samples: &[i16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(samples.len() * 2);
    for &s in samples {
        out.extend_from_slice(&s.to_le_bytes());
    }
    out
}

/// Decode little-endian wire bytes back to mono i16 samples.
pub fn decode_frame(bytes: &[u8]) -> Vec<i16> {
    bytes
        .chunks_exact(2)
        .map(|c| i16::from_le_bytes([c[0], c[1]]))
        .collect()
}

/// Pick a supported config that can run at [`SAMPLE_RATE`].
fn config_at_48k(
    configs: impl Iterator<Item = cpal::SupportedStreamConfigRange>,
) -> Option<SupportedStreamConfig> {
    for range in configs {
        if range.min_sample_rate().0 <= SAMPLE_RATE && SAMPLE_RATE <= range.max_sample_rate().0 {
            return Some(range.with_sample_rate(SampleRate(SAMPLE_RATE)));
        }
    }
    None
}

// ─────────────────────────────── Capture ────────────────────────────────

/// A running microphone capture. Dropping it stops and releases the device.
pub struct Capture {
    enabled: Arc<AtomicBool>,
    _stop: std::sync::mpsc::Sender<()>,
}

impl Capture {
    /// Shared push-to-talk flag — set `true` to actually transmit. Capture runs
    /// continuously but emits frames only while this is `true`.
    pub fn enabled(&self) -> Arc<AtomicBool> {
        self.enabled.clone()
    }

    /// Open the default input device and stream 20 ms mono-i16 frames to `tx`.
    pub fn start(tx: UnboundedSender<Vec<i16>>) -> Result<Capture> {
        let enabled = Arc::new(AtomicBool::new(false));
        let enabled_thread = enabled.clone();
        let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
        let (init_tx, init_rx) = std::sync::mpsc::channel::<Result<(), String>>();

        std::thread::spawn(move || {
            match build_input(tx, enabled_thread) {
                Ok(stream) => {
                    if let Err(e) = stream.play() {
                        let _ = init_tx.send(Err(e.to_string()));
                        return;
                    }
                    let _ = init_tx.send(Ok(()));
                    // Park until the Capture handle is dropped (sender closed).
                    let _ = stop_rx.recv();
                    // `stream` drops here, releasing the device.
                }
                Err(e) => {
                    let _ = init_tx.send(Err(e.to_string()));
                }
            }
        });

        init_rx
            .recv()
            .map_err(|_| anyhow!("audio capture thread died"))?
            .map_err(|e| anyhow!("audio capture: {e}"))?;
        Ok(Capture {
            enabled,
            _stop: stop_tx,
        })
    }
}

fn build_input(tx: UnboundedSender<Vec<i16>>, enabled: Arc<AtomicBool>) -> Result<cpal::Stream> {
    let host = cpal::default_host();
    let device = host
        .default_input_device()
        .context("no default input device")?;
    let supported = config_at_48k(
        device
            .supported_input_configs()
            .context("querying input configs")?,
    )
    .context("input device does not support 48 kHz")?;
    let sample_format = supported.sample_format();
    let config: StreamConfig = supported.config();
    let channels = config.channels.max(1) as usize;

    match sample_format {
        SampleFormat::I8 => input_stream::<i8>(&device, &config, channels, tx, enabled),
        SampleFormat::I16 => input_stream::<i16>(&device, &config, channels, tx, enabled),
        SampleFormat::I32 => input_stream::<i32>(&device, &config, channels, tx, enabled),
        SampleFormat::U8 => input_stream::<u8>(&device, &config, channels, tx, enabled),
        SampleFormat::U16 => input_stream::<u16>(&device, &config, channels, tx, enabled),
        SampleFormat::U32 => input_stream::<u32>(&device, &config, channels, tx, enabled),
        SampleFormat::F32 => input_stream::<f32>(&device, &config, channels, tx, enabled),
        SampleFormat::F64 => input_stream::<f64>(&device, &config, channels, tx, enabled),
        other => Err(anyhow!("unsupported input sample format: {other:?}")),
    }
}

fn input_stream<T>(
    device: &cpal::Device,
    config: &StreamConfig,
    channels: usize,
    tx: UnboundedSender<Vec<i16>>,
    enabled: Arc<AtomicBool>,
) -> Result<cpal::Stream>
where
    T: SizedSample,
    i16: FromSample<T>,
{
    let mut acc: Vec<i16> = Vec::with_capacity(FRAME_SAMPLES * 2);
    let stream = device.build_input_stream(
        config,
        move |data: &[T], _| {
            if !enabled.load(Ordering::Relaxed) {
                return;
            }
            // Downmix interleaved channels to a single mono i16 stream.
            for frame in data.chunks(channels) {
                let mut sum: i32 = 0;
                for &s in frame {
                    sum += i16::from_sample(s) as i32;
                }
                acc.push((sum / channels as i32) as i16);
            }
            while acc.len() >= FRAME_SAMPLES {
                let frame: Vec<i16> = acc.drain(..FRAME_SAMPLES).collect();
                let _ = tx.send(frame);
            }
        },
        |e| eprintln!("audio input error: {e}"),
        None,
    )?;
    Ok(stream)
}

// ────────────────────────────── Playback ────────────────────────────────

/// A running speaker playback. The network side pushes samples in; a dedicated
/// audio thread plays them through a jitter buffer. Dropping it stops playback.
pub struct Playback {
    prod: ringbuf::HeapProd<i16>,
    _stop: std::sync::mpsc::Sender<()>,
}

impl Playback {
    /// Open the default output device and start a silent jitter-buffered stream.
    pub fn start() -> Result<Playback> {
        let rb = HeapRb::<i16>::new(RING_CAPACITY);
        let (prod, cons) = rb.split();

        let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
        let (init_tx, init_rx) = std::sync::mpsc::channel::<Result<(), String>>();

        std::thread::spawn(move || {
            match build_output(cons) {
                Ok(stream) => {
                    if let Err(e) = stream.play() {
                        let _ = init_tx.send(Err(e.to_string()));
                        return;
                    }
                    let _ = init_tx.send(Ok(()));
                    let _ = stop_rx.recv();
                }
                Err(e) => {
                    let _ = init_tx.send(Err(e.to_string()));
                }
            }
        });

        init_rx
            .recv()
            .map_err(|_| anyhow!("audio playback thread died"))?
            .map_err(|e| anyhow!("audio playback: {e}"))?;
        Ok(Playback {
            prod,
            _stop: stop_tx,
        })
    }

    /// Queue decoded samples for playback. Overrun drops the oldest audio (the
    /// ring is bounded), which is the right behaviour for real-time voice.
    pub fn push(&mut self, samples: &[i16]) {
        self.prod.push_slice(samples);
    }
}

fn build_output(cons: ringbuf::HeapCons<i16>) -> Result<cpal::Stream> {
    let host = cpal::default_host();
    let device = host
        .default_output_device()
        .context("no default output device")?;
    let supported = config_at_48k(
        device
            .supported_output_configs()
            .context("querying output configs")?,
    )
    .context("output device does not support 48 kHz")?;
    let sample_format = supported.sample_format();
    let config: StreamConfig = supported.config();
    let channels = config.channels.max(1) as usize;

    match sample_format {
        SampleFormat::I8 => output_stream::<i8>(&device, &config, channels, cons),
        SampleFormat::I16 => output_stream::<i16>(&device, &config, channels, cons),
        SampleFormat::I32 => output_stream::<i32>(&device, &config, channels, cons),
        SampleFormat::U8 => output_stream::<u8>(&device, &config, channels, cons),
        SampleFormat::U16 => output_stream::<u16>(&device, &config, channels, cons),
        SampleFormat::U32 => output_stream::<u32>(&device, &config, channels, cons),
        SampleFormat::F32 => output_stream::<f32>(&device, &config, channels, cons),
        SampleFormat::F64 => output_stream::<f64>(&device, &config, channels, cons),
        other => Err(anyhow!("unsupported output sample format: {other:?}")),
    }
}

fn output_stream<T>(
    device: &cpal::Device,
    config: &StreamConfig,
    channels: usize,
    mut cons: ringbuf::HeapCons<i16>,
) -> Result<cpal::Stream>
where
    T: SizedSample + FromSample<i16>,
{
    // Reusable scratch so the realtime callback never allocates.
    let mut scratch = vec![0i16; 8192];
    let mut playing = false;
    let stream = device.build_output_stream(
        config,
        move |out: &mut [T], _| {
            let frames = out.len() / channels.max(1);
            // Wait for a small prebuffer before starting, to absorb jitter.
            if !playing && cons.occupied_len() >= PREBUFFER_SAMPLES {
                playing = true;
            }
            let want = frames.min(scratch.len());
            let got = if playing {
                cons.pop_slice(&mut scratch[..want])
            } else {
                0
            };
            for (f, chunk) in out.chunks_mut(channels.max(1)).enumerate() {
                let v = if f < got { scratch[f] } else { 0i16 };
                let sample = T::from_sample(v);
                for s in chunk.iter_mut() {
                    *s = sample;
                }
            }
            // Sustained underrun: re-arm the prebuffer instead of stuttering.
            if playing && got < frames {
                playing = false;
            }
        },
        |e| eprintln!("audio output error: {e}"),
        None,
    )?;
    Ok(stream)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trips() {
        let samples: Vec<i16> = vec![0, 1, -1, 32767, -32768, 1234];
        let bytes = encode_frame(&samples);
        assert_eq!(bytes.len(), samples.len() * 2);
        assert_eq!(decode_frame(&bytes), samples);
    }
}
