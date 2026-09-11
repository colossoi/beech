param([string]$Compiler = 'thrift')
$ErrorActionPreference = 'Stop'
if ((& $Compiler -version) -ne 'Thrift version 0.24.0') { throw 'Use Apache Thrift 0.24.0' }
$outputDirectory = Join-Path $PSScriptRoot '../src/codec/generated'
& $Compiler --gen rs -out $outputDirectory (Join-Path $PSScriptRoot 'beech.thrift')
if ($LASTEXITCODE -ne 0) { throw 'Thrift generation failed' }
$outputPath = Join-Path $outputDirectory 'beech.rs'
$source = Get-Content -Raw -LiteralPath $outputPath
# Thrift 0.24.0 incorrectly boxes elements when reading list<union>, although
# the declared Vec element type is unboxed. Keep this workaround reproducible.
if ([regex]::Matches($source, [regex]::Escape('val.push(Box::new(elem))')).Count -ne 1) {
    throw 'Expected exactly one list<Scalar> generator defect; review the output'
}
$source = $source.Replace('val.push(Box::new(elem))', 'val.push(elem)')
# No services are defined; this unconditional generated import needs the unused
# Thrift server feature. The old rustfmt attribute is unsupported by modern Rust.
$source = [regex]::Replace($source, '(?m)^use thrift::server::TProcessor;\r?\n', '')
$source = [regex]::Replace($source, '(?m)^#!\[cfg_attr\(rustfmt, rustfmt_skip\)\]\r?\n', '')
Set-Content -LiteralPath $outputPath -Value $source -NoNewline
rustfmt --edition 2024 $outputPath
if ($LASTEXITCODE -ne 0) { throw 'Formatting generated Rust failed' }
