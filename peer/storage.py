"""Fragmentação de arquivo, remontagem e controle de blocos.

Funções utilitárias para dividir um arquivo em blocos, remontar a partir de
blocos e calcular checksums SHA-256. Também fornece uma estrutura `Bitfield`
simples para rastrear quais blocos o peer possui.
"""
from typing import List
import hashlib

DEFAULT_BLOCK_SIZE = 1024

def fragment_file(path: str, block_size: int = DEFAULT_BLOCK_SIZE) -> List[bytes]:
    """Divide `path` em blocos de `block_size` bytes e retorna lista de bytes."""
    blocks = []
    with open(path, 'rb') as f:
        while True:
            b = f.read(block_size)
            if not b:
                break
            blocks.append(b)
    return blocks

def reassemble_file(blocks: List[bytes], out_path: str) -> None:
    """Escreve a lista de blocos em `out_path` na ordem dada."""
    with open(out_path, 'wb') as f:
        for b in blocks:
            f.write(b)

def sha256_bytes(data: bytes) -> str:
    """Retorna o hash SHA-256 de um buffer de bytes."""
    return hashlib.sha256(data).hexdigest()

def sha256_file(path: str) -> str:
    """Calcula SHA-256 de um arquivo lido em streaming."""
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()

class Bitfield:
    """Estrutura simples que marca quais índices de bloco o peer possui.

    Uso: `b = Bitfield(total); b.set(i); b.has(i)`
    """
    def __init__(self, total: int):
        self.total = total
        self.have = [False] * total

    def set(self, idx: int):
        self.have[idx] = True

    def has(self, idx: int) -> bool:
        return self.have[idx]

    def missing_indices(self):
        return [i for i,v in enumerate(self.have) if not v]
