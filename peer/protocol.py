"""Auxiliares do protocolo baseado em texto.

Mensagens (linhas UTF-8) utilizadas neste esqueleto:
- METADATA <total_blocks> <block_size> <sha256>\n
- GET <index>\n
- OK <length>\n<raw bytes>
- ERR\n
O servidor envia METADATA quando a conexão é estabelecida.
"""

from typing import Tuple

def build_metadata(total_blocks: int, block_size: int, sha256: str) -> bytes:
    return f"METADATA {total_blocks} {block_size} {sha256}\n".encode()

def parse_metadata(line: bytes) -> Tuple[int,int,str]:
    parts = line.decode().strip().split()
    if parts[0] != 'METADATA':
        raise ValueError('Não é METADATA')
    return int(parts[1]), int(parts[2]), parts[3]

def build_get(idx: int) -> bytes:
    return f"GET {idx}\n".encode()

def build_ok(length: int) -> bytes:
    return f"OK {length}\n".encode()

def build_bitfield(indices: list) -> bytes:
    """Constroi uma linha BITFIELD com índices separados por vírgula."""
    if not indices:
        return b"BITFIELD \n"
    s = ','.join(str(i) for i in indices)
    return f"BITFIELD {s}\n".encode()

def parse_bitfield(line: bytes) -> list:
    """Retorna lista de índices a partir de uma linha BITFIELD."""
    parts = line.decode().strip().split(maxsplit=1)
    if parts[0] != 'BITFIELD':
        raise ValueError('Não é BITFIELD')
    if len(parts) == 1 or not parts[1].strip():
        return []
    return [int(x) for x in parts[1].split(',') if x]
