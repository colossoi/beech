param([string]$Compiler = 'thrift')
$ErrorActionPreference = 'Stop'
if ((& $Compiler -version) -ne 'Thrift version 0.24.0') { throw 'Use Apache Thrift 0.24.0' }
$outputDirectory = Join-Path $PSScriptRoot '../src/generated'
& $Compiler --gen rs -out $outputDirectory (Join-Path $PSScriptRoot 'plan.thrift')
if ($LASTEXITCODE -ne 0) { throw 'Thrift generation failed' }
$outputPath = Join-Path $outputDirectory 'plan.rs'
$source = Get-Content -Raw -LiteralPath $outputPath
# No services are defined; this unconditional generated import needs the unused
# Thrift server feature. The old rustfmt attribute is unsupported by modern Rust.
$source = [regex]::Replace($source, '(?m)^use thrift::server::TProcessor;\r?\n', '')
$source = [regex]::Replace($source, '(?m)^#!\[cfg_attr\(rustfmt, rustfmt_skip\)\]\r?\n', '')
$source = $source.Replace('thrift::', 'beech_core::thrift::')
Set-Content -LiteralPath $outputPath -Value $source -NoNewline
rustfmt --edition 2024 $outputPath
if ($LASTEXITCODE -ne 0) { throw 'Formatting generated Rust failed' }
