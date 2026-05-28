"""Nó híbrido (servidor + cliente) compartilhando o mesmo armazenamento de blocos.

O nó inicia um `PeerServer` que, concorrentemente com o `PeerClient`, opera
sobre a mesma lista `blocks`. Conforme o cliente recebe blocos, o servidor
passa imediatamente a servi-los — cumprindo o "tornando-se um Seeder" do
enunciado.

O servidor permanece em execução até ser cancelado externamente (ex.:
SIGTERM do runner), garantindo que o nó continue compartilhando blocos
após completar seu próprio download.
"""
import asyncio
import logging
from typing import List, Tuple, Optional

from .server import PeerServer
from .client import PeerClient
from .storage import fragment_file, sha256_file, Bitfield

logger = logging.getLogger('peer.node')


class PeerNode:
    def __init__(
        self,
        host: str,
        port: int,
        neighbors: List[Tuple[str, int]],
        file: Optional[str] = None,
        out: Optional[str] = None,
        block_size: int = 1024,
        total_blocks: Optional[int] = None,
        seed_after_complete: bool = True,
        concurrency: int = 8,
    ):
        self.host = host
        self.port = port
        self.neighbors = neighbors
        self.file = file
        self.out = out
        self.block_size = block_size
        self.seed_after_complete = seed_after_complete

        if file:
            self.blocks: List[Optional[bytes]] = list(fragment_file(file, block_size))
            self.total_blocks = len(self.blocks)
            self.bitfield = Bitfield(self.total_blocks)
            for i in range(self.total_blocks):
                self.bitfield.set(i)
            self.sha = sha256_file(file)
        else:
            assert total_blocks is not None, 'Forneça --total-blocks para nodes que não são seeder'
            self.total_blocks = total_blocks
            self.blocks = [None] * self.total_blocks
            self.bitfield = Bitfield(self.total_blocks)
            self.sha = ''

        self.server = PeerServer(self.host, self.port, self.blocks, self.block_size, self.sha)
        self.client = PeerClient(
            self.neighbors,
            self.out,
            self.total_blocks,
            existing_blocks=self.blocks,
            concurrency=concurrency,
        )

    async def start(self):
        srv_task = asyncio.create_task(self.server.start())
        # pequeno yield para o bind do servidor
        await asyncio.sleep(0.1)

        if self.neighbors and any(b is None for b in self.blocks):
            ok = await self.client.run()
            # mantém bitfield em sincronia
            for i, b in enumerate(self.blocks):
                if b is not None:
                    self.bitfield.set(i)
            logger.info('Download finalizado (success=%s)', ok)
        else:
            logger.info('Sem vizinhos ou nada a baixar; permanecendo como seeder')

        if self.seed_after_complete:
            logger.info('Permanecendo ativo como seeder em %s:%d', self.host, self.port)
            try:
                await srv_task
            except asyncio.CancelledError:
                pass
        else:
            srv_task.cancel()
            try:
                await srv_task
            except asyncio.CancelledError:
                pass
