# Imports the MSVC (Visual Studio Build Tools) developer environment into the
# CURRENT PowerShell session so `cargo`/`rustc` can find `link.exe`, the Windows
# SDK headers and the import libraries.
#
# Usage (dot-source so the env survives for the commands that follow):
#   . .\tools\msvcenv.ps1; cargo build
#
# The Claude Code PowerShell tool starts a fresh shell per call and does not
# persist environment variables, so dot-source this at the start of every call
# that needs to compile.

$ErrorActionPreference = 'Stop'

$vswhere = "C:\Program Files (x86)\Microsoft Visual Studio\Installer\vswhere.exe"
if (-not (Test-Path $vswhere)) {
    throw "vswhere.exe not found; is Visual Studio / Build Tools installed?"
}

$vsPath = & $vswhere -products * -latest `
    -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 `
    -property installationPath
if (-not $vsPath) {
    throw "No VS install with the VC++ x64 toolchain was found."
}

$vcvars = Join-Path $vsPath 'VC\Auxiliary\Build\vcvars64.bat'
if (-not (Test-Path $vcvars)) {
    throw "vcvars64.bat not found at $vcvars"
}

# Run vcvars in cmd, then dump the resulting environment and import each var.
cmd /c "`"$vcvars`" >nul 2>&1 && set" | ForEach-Object {
    if ($_ -match '^([^=]+)=(.*)$') {
        [Environment]::SetEnvironmentVariable($matches[1], $matches[2])
    }
}

if (-not (Get-Command link.exe -ErrorAction SilentlyContinue)) {
    throw "MSVC env imported but link.exe still not on PATH."
}
Write-Host "[msvcenv] MSVC toolchain ready: $vsPath" -ForegroundColor Green
