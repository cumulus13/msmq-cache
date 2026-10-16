#!/usr/bin/env bash
# Exports the documentation to dist/docs: combined Markdown, HTML, DOCX and PDF.
# Requires pandoc. Mermaid diagrams are rendered when 'mermaid-filter' (npm i -g mermaid-filter) is on PATH;
# PDF needs a LaTeX engine (xelatex). Missing optional tools are reported, not silently skipped.
set -euo pipefail
rc=0
cd "$(dirname "$0")"
out=../dist/docs; mkdir -p "$out"
files=$(ls [0-9][0-9]-*.md | sort)
filter=(); if command -v mermaid-filter >/dev/null; then filter=(--filter mermaid-filter); else echo "WARN: mermaid-filter not found; diagrams stay as code blocks"; fi
cat ../README.md $files > "$out/MsmqCache-docs.md"
pandoc "$out/MsmqCache-docs.md" -o "$out/MsmqCache-docs.docx" --toc "${filter[@]}"
pandoc "$out/MsmqCache-docs.md" -o "$out/MsmqCache-docs.html" --toc --standalone --metadata title="MsmqCache" "${filter[@]}"
if command -v xelatex >/dev/null; then
  pandoc "$out/MsmqCache-docs.md" -o "$out/MsmqCache-docs.pdf" --toc --pdf-engine=xelatex "${filter[@]}" || { echo "ERROR: PDF export failed (see LaTeX log above; install lmodern/texlive packages)" >&2; rc=1; }
else echo "WARN: xelatex not found; PDF skipped"; fi
ls -l "$out"
exit $rc
