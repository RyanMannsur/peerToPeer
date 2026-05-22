"""Cliente asyncio que busca blocos faltantes de vizinhos de forma concorrente."""
import asyncio
from .protocol import parse_metadata, build_get
from .storage import reassemble_file
from typing import List, Tuple

import logging

logger = logging.getLogger('peer.client')


class PeerClient:
    def __init__(self, neighbors: List[Tuple[str,int]], out_path: str, total_blocks: int, existing_blocks: List = None, concurrency: int = 8):
        self.neighbors = neighbors
        self.out_path = out_path
        self.total_blocks = total_blocks
        self.blocks = existing_blocks if existing_blocks is not None else [None] * total_blocks
        self.concurrency = concurrency
        # mapping neighbor -> set(indices)
        self.neighbor_have = {}

    async def probe_neighbor(self, host: str, port: int):
        """Conecta ao vizinho, lê METADATA e BITFIELD, e grava disponibilidade."""
        try:
            reader, writer = await asyncio.open_connection(host, port)
            meta = await reader.readline()
            if not meta:
                return
            tb, bs, sha = parse_metadata(meta)
            # tenta ler BITFIELD (se enviado)
            bf_line = await reader.readline()
            have = set()
            try:
                from .protocol import parse_bitfield
                have = set(parse_bitfield(bf_line))
            except Exception:
                # se bitfield não for enviado, assume que o servidor pode servir tudo conhecido
                have = set(range(tb))
            self.neighbor_have[(host, port)] = have
            logger.info('Vizinho %s:%d tem %d blocos', host, port, len(have))
        except Exception as e:
            logger.debug('Falha ao sondar vizinho %s:%d — %s', host, port, e)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def fetch_block_from(self, idx: int, neighbor: Tuple[str,int]):
        host, port = neighbor
        try:
            reader, writer = await asyncio.open_connection(host, port)
            # descarta metadata/bitfield
            await reader.readline()
            await reader.readline()
            writer.write(build_get(idx))
            await writer.drain()
            header = await reader.readline()
            if not header:
                return False
            if header.startswith(b'OK '):
                length = int(header.decode().split()[1])
                data = await reader.readexactly(length)
                if self.blocks[idx] is None:
                    self.blocks[idx] = data
                    logger.info('Recebido bloco %d de %s:%d', idx, host, port)
                return True
            return False
        except Exception as e:
            logger.debug('Erro fetch bloco %d from %s:%d — %s', idx, host, port, e)
            return False
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def run(self):
        # sondar vizinhos para montar disponibilidade
        await asyncio.gather(*[self.probe_neighbor(h,p) for h,p in self.neighbors])
        missing = [i for i,v in enumerate(self.blocks) if v is None]
        if not missing:
            logger.info('Nenhum bloco faltando')
            return

        # computa raridade (quantos vizinhos tem cada bloco)
        availability = {i:0 for i in missing}
        for nb, have in self.neighbor_have.items():
            for i in missing:
                if i in have:
                    availability[i] += 1

        # ordena blocos por raridade ascendente
        blocks_by_rarity = sorted(missing, key=lambda x: (availability.get(x,0), x))

        sem = asyncio.Semaphore(self.concurrency)
        async def worker(idx, neighbor):
            async with sem:
                return await self.fetch_block_from(idx, neighbor)

        tasks = []
        # atribui cada bloco ao primeiro vizinho que possui
        for idx in blocks_by_rarity:
            assigned = False
            # escolhe vizinho com menor carga (simplista: primeira que tem)
            for nb in self.neighbor_have:
                if idx in self.neighbor_have[nb]:
                    tasks.append(asyncio.create_task(worker(idx, nb)))
                    assigned = True
                    break
            if not assigned:
                logger.warning('Nenhum vizinho tem o bloco %d', idx)

        if tasks:
            await asyncio.gather(*tasks)

        # verificação final
        if all(b is not None for b in self.blocks):
            if self.out_path:
                reassemble_file(self.blocks, self.out_path)
                logger.info('Arquivo remontado em %s', self.out_path)
        else:
            missing = [i for i,v in enumerate(self.blocks) if v is None]
            logger.warning('Blocos faltando após execução: %s', missing)
