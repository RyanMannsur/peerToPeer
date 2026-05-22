"""Nó híbrido (servidor + cliente) que compartilha o mesmo armazenamento de blocos.

Este nó inicia um servidor asyncio (`PeerServer`) e, concorrentemente, executa
fetchers que solicitam blocos faltantes aos vizinhos e os escrevem na lista
compartilhada `blocks`. Assim que um bloco chega, o servidor passa a servi-lo.
"""
import asyncio
from typing import List, Tuple, Optional
from .server import PeerServer
from .protocol import build_get, parse_metadata
from .storage import fragment_file, sha256_file, Bitfield


class PeerNode:
    def __init__(self, host: str, port: int, neighbors: List[Tuple[str,int]],
                 file: Optional[str] = None, out: Optional[str] = None,
                 block_size: int = 1024, total_blocks: Optional[int] = None):
        self.host = host
        self.port = port
        self.neighbors = neighbors
        self.file = file
        self.out = out
        self.block_size = block_size
        self.total_blocks = total_blocks

        if file:
            # seeder inicial: fragmenta o arquivo e marca todos os blocos como presentes
            self.blocks = fragment_file(file, block_size)
            self.total_blocks = len(self.blocks)
            self.bitfield = Bitfield(self.total_blocks)
            for i in range(self.total_blocks):
                self.bitfield.set(i)
            self.sha = sha256_file(file)
        else:
            # nó que começa sem o arquivo completo: precisa conhecer total_blocks
            assert total_blocks is not None, 'Forneça --total-blocks para nodes que não são seeder'
            self.total_blocks = total_blocks
            self.blocks: List[Optional[bytes]] = [None] * self.total_blocks
            self.bitfield = Bitfield(self.total_blocks)
            self.sha = None

        # o servidor usará a lista `blocks` compartilhada
        self.server = PeerServer(self.host, self.port, self.blocks, self.block_size, self.sha or '')

        # cliente que opera sobre os mesmos blocos (reutiliza PeerClient)
        from .client import PeerClient
        self.client = PeerClient(self.neighbors, self.out, self.total_blocks, existing_blocks=self.blocks)

    async def fetch_from(self, host: str, port: int):
        try:
            reader, writer = await asyncio.open_connection(host, port)
            meta = await reader.readline()
            if not meta:
                return
            tb, bs, sha = parse_metadata(meta)
            # atualiza sha se desconhecido
            if self.sha is None:
                self.sha = sha
            # solicita blocos faltantes a este vizinho
            for idx in range(self.total_blocks):
                if self.blocks[idx] is not None:
                    continue
                writer.write(build_get(idx))
                await writer.drain()
                header = await reader.readline()
                if not header:
                    break
                if header.startswith(b'OK '):
                    length = int(header.decode().split()[1])
                    data = await reader.readexactly(length)
                    # escreve no armazenamento compartilhado e atualiza bitfield
                    if self.blocks[idx] is None:
                        self.blocks[idx] = data
                        self.bitfield.set(idx)
                else:
                    # vizinho respondeu ERR
                    continue
        except Exception:
            return
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def run_fetchers(self):
        tasks = [asyncio.create_task(self.fetch_from(h,p)) for h,p in self.neighbors]
        await asyncio.gather(*tasks)

    async def start(self):
        # inicia o servidor
        srv_task = asyncio.create_task(self.server.start())
        # espera breve para o bind do servidor
        await asyncio.sleep(0.1)
        # executar o cliente que faz sondagem e download com scheduler
        await self.client.run()
        # se todos os blocos estiverem presentes, remontar se requisitado
        if self.out and all(b is not None for b in self.blocks):
            from .storage import reassemble_file
            reassemble_file(self.blocks, self.out)
        # cancela o servidor (em execuções CLI encerramos aqui)
        srv_task.cancel()
        try:
            await srv_task
        except asyncio.CancelledError:
            pass
