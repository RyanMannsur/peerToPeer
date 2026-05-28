"""Cliente asyncio que busca blocos faltantes de vizinhos de forma concorrente.

Estratégia: faz probe periódico dos vizinhos para descobrir disponibilidade
(bitfield), prioriza blocos mais raros e baixa em paralelo. Repete o ciclo
até todos os blocos serem obtidos ou esgotar o número de tentativas.
"""
import asyncio
import logging
from typing import List, Tuple, Optional

from .protocol import parse_metadata, parse_bitfield, build_get
from .storage import reassemble_file, sha256_file

logger = logging.getLogger('peer.client')


class PeerClient:
    def __init__(
        self,
        neighbors: List[Tuple[str, int]],
        out_path: Optional[str],
        total_blocks: int,
        existing_blocks: Optional[List] = None,
        concurrency: int = 8,
        max_idle_passes: int = 30,
        idle_sleep: float = 0.2,
    ):
        self.neighbors = neighbors
        self.out_path = out_path
        self.total_blocks = total_blocks
        self.blocks = existing_blocks if existing_blocks is not None else [None] * total_blocks
        self.concurrency = concurrency
        self.max_idle_passes = max_idle_passes
        self.idle_sleep = idle_sleep
        self.neighbor_have: dict = {}
        self.expected_sha: Optional[str] = None

    async def probe_neighbor(self, host: str, port: int):
        """Conecta ao vizinho, lê METADATA e BITFIELD, atualiza disponibilidade."""
        writer = None
        try:
            reader, writer = await asyncio.open_connection(host, port)
            meta = await reader.readline()
            if not meta:
                return
            tb, bs, sha = parse_metadata(meta)
            if self.expected_sha is None and sha:
                self.expected_sha = sha
            bf_line = await reader.readline()
            try:
                have = set(parse_bitfield(bf_line))
            except Exception:
                have = set(range(tb))
            self.neighbor_have[(host, port)] = have
            logger.info('Probe %s:%d -> %d blocos disponíveis', host, port, len(have))
        except Exception as e:
            logger.debug('Falha ao sondar %s:%d — %s', host, port, e)
            self.neighbor_have.setdefault((host, port), set())
        finally:
            if writer is not None:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass

    async def probe_all(self):
        await asyncio.gather(*[self.probe_neighbor(h, p) for h, p in self.neighbors])

    async def fetch_block_from(self, idx: int, neighbor: Tuple[str, int]) -> bool:
        host, port = neighbor
        writer = None
        try:
            reader, writer = await asyncio.open_connection(host, port)
            # descarta METADATA e BITFIELD
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
            # ERR: vizinho ainda não tem esse bloco; remove do bitfield local
            self.neighbor_have.get(neighbor, set()).discard(idx)
            return False
        except Exception as e:
            logger.debug('Erro fetch bloco %d de %s:%d — %s', idx, host, port, e)
            return False
        finally:
            if writer is not None:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass

    def _missing(self) -> List[int]:
        return [i for i, v in enumerate(self.blocks) if v is None]

    def _plan_assignments(self, missing: List[int]) -> List[Tuple[int, Tuple[str, int]]]:
        """Para cada bloco faltante, escolhe um vizinho que o possui (raridade asc)."""
        availability = {i: 0 for i in missing}
        for nb, have in self.neighbor_have.items():
            for i in missing:
                if i in have:
                    availability[i] += 1
        ordered = sorted(missing, key=lambda x: (availability[x], x))
        # round-robin entre vizinhos que possuem o bloco, para distribuir carga
        load = {nb: 0 for nb in self.neighbor_have}
        assignments: List[Tuple[int, Tuple[str, int]]] = []
        for idx in ordered:
            candidates = [nb for nb in self.neighbor_have if idx in self.neighbor_have[nb]]
            if not candidates:
                continue
            chosen = min(candidates, key=lambda nb: load[nb])
            assignments.append((idx, chosen))
            load[chosen] += 1
        return assignments

    async def run(self) -> bool:
        sem = asyncio.Semaphore(self.concurrency)

        async def worker(idx, neighbor):
            async with sem:
                return await self.fetch_block_from(idx, neighbor)

        idle_passes = 0
        while True:
            await self.probe_all()
            missing = self._missing()
            if not missing:
                break

            assignments = self._plan_assignments(missing)
            if not assignments:
                idle_passes += 1
                if idle_passes >= self.max_idle_passes:
                    logger.warning(
                        'Esgotado: %d blocos faltantes sem vizinho disponível: %s',
                        len(missing), missing,
                    )
                    break
                await asyncio.sleep(self.idle_sleep)
                continue

            results = await asyncio.gather(*[worker(idx, nb) for idx, nb in assignments])
            if any(results):
                idle_passes = 0
            else:
                idle_passes += 1
                if idle_passes >= self.max_idle_passes:
                    logger.warning(
                        'Sem progresso após %d tentativas; blocos faltantes: %s',
                        idle_passes, self._missing(),
                    )
                    break
                await asyncio.sleep(self.idle_sleep)

        missing = self._missing()
        if missing:
            logger.warning('Blocos faltando após execução: %s', missing)
            return False

        if self.out_path:
            reassemble_file(self.blocks, self.out_path)
            logger.info('Arquivo remontado em %s', self.out_path)
            if self.expected_sha:
                actual = sha256_file(self.out_path)
                if actual == self.expected_sha:
                    logger.info('SHA-256 verificado OK: %s', actual)
                else:
                    logger.error(
                        'SHA-256 divergente! esperado=%s obtido=%s',
                        self.expected_sha, actual,
                    )
                    return False
        return True
