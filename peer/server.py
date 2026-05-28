"""Servidor asyncio do peer: envia METADATA e responde a GET <index>."""
import asyncio
import logging
from .protocol import build_metadata, build_ok, build_bitfield

logger = logging.getLogger('peer.server')


class PeerServer:
    def __init__(self, host: str, port: int, blocks: list, block_size: int, sha256: str):
        self.host = host
        self.port = port
        self.blocks = blocks
        self.block_size = block_size
        self.sha256 = sha256

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info('peername')
        # envia metadata assim que a conexão é aceita
        writer.write(build_metadata(len(self.blocks), self.block_size, self.sha256))
        # envia bitfield com os índices atualmente disponíveis
        have = [i for i, b in enumerate(self.blocks) if b is not None]
        writer.write(build_bitfield(have))
        await writer.drain()
        logger.info('Conexão de %s; anunciando %d blocos', peer, len(have))
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                cmd = line.decode().strip()
                if cmd.startswith('GET '):
                    try:
                        idx = int(cmd.split()[1])
                    except (ValueError, IndexError):
                        writer.write(b'ERR\n')
                        await writer.drain()
                        continue
                    if 0 <= idx < len(self.blocks):
                        blk = self.blocks[idx]
                        if blk is None:
                            # ainda não temos esse bloco; respondemos ERR
                            writer.write(b'ERR\n')
                        else:
                            writer.write(build_ok(len(blk)))
                            writer.write(blk)
                            logger.info('Enviado bloco %d para %s', idx, peer)
                    else:
                        writer.write(b'ERR\n')
                else:
                    writer.write(b'ERR\n')
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def start(self):
        server = await asyncio.start_server(self.handle, self.host, self.port)
        addrs = ', '.join(str(s.getsockname()) for s in server.sockets)
        print(f'Atendendo em {addrs}')
        async with server:
            await server.serve_forever()
