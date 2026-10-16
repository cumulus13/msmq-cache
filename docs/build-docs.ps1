# Exports the documentation to dist\docs (md, html, docx, pdf). Requires pandoc; see build-docs.sh for optional tools.
$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot
$out = Join-Path $PSScriptRoot '..\dist\docs'; New-Item -ItemType Directory -Force $out | Out-Null
$files = Get-ChildItem '[0-9][0-9]-*.md' | Sort-Object Name
$combined = Join-Path $out 'MsmqCache-docs.md'
Get-Content ..\README.md, $files.FullName -Encoding UTF8 | Set-Content $combined -Encoding UTF8
$f = @(); if (Get-Command mermaid-filter -ErrorAction SilentlyContinue) { $f = @('--filter','mermaid-filter') } else { Write-Warning 'mermaid-filter not found; diagrams stay as code blocks' }
pandoc $combined -o (Join-Path $out 'MsmqCache-docs.docx') --toc @f
pandoc $combined -o (Join-Path $out 'MsmqCache-docs.html') --toc --standalone --metadata title=MsmqCache @f
if (Get-Command xelatex -ErrorAction SilentlyContinue) { pandoc $combined -o (Join-Path $out 'MsmqCache-docs.pdf') --toc --pdf-engine=xelatex @f } else { Write-Warning 'xelatex not found; PDF skipped' }
Get-ChildItem $out
