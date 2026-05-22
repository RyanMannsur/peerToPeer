import sys

try:
    from PyPDF2 import PdfReader
except Exception:
    raise SystemExit('PyPDF2 not installed')

if len(sys.argv) < 2:
    print('Usage: python extract_pdf_text.py <pdf-path> [out-path]')
    sys.exit(1)

path = sys.argv[1]
out_path = sys.argv[2] if len(sys.argv) > 2 else None
reader = PdfReader(path)
texts = []
for p in reader.pages:
    t = p.extract_text()
    if t:
        texts.append(t)

full = '\n\n'.join(texts)
if out_path:
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(full)
else:
    print(full)
