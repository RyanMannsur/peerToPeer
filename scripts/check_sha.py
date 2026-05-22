import hashlib, os

def sha(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()

sample = 'tests/sample10.bin'
out = 'tests/out_1.bin'

print('sample exists', os.path.exists(sample))
print('out exists', os.path.exists(out))
if os.path.exists(sample):
    print('sample sha:', sha(sample))
if os.path.exists(out):
    print('out sha   :', sha(out))
