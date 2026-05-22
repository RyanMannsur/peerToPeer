"""Servidor asyncio do peer: envia METADATA e responde a GET <index>."""
import asyncio
from .protocol import build_metadata, build_ok

class PeerServer:
    def __init__(self, host: str, port: int, blocks: list, block_size: int, sha256: str):
        self.host = host
        self.port = port
        self.blocks = blocks
        self.block_size = block_size
        self.sha256 = sha256

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        # envia metadata assim que a conexão é aceita
        writer.write(build_metadata(len(self.blocks), self.block_size, self.sha256))
        await writer.drain()
        # envia bitfield indicando índices que este servidor possui
        try:
            from .protocol import build_bitfield
            have = [i for i,b in enumerate(self.blocks) if b is not None]
            # debug: imprimir indices que este servidor possui
            print('SERVER: announcing blocks ->', have)
            writer.write(build_bitfield(have))
            await writer.drain()
        except Exception:
            pass
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                cmd = line.decode().strip()
                if cmd.startswith('GET '):
                    idx = int(cmd.split()[1])
                    if 0 <= idx < len(self.blocks):
                        blk = self.blocks[idx]
                        # responde com header OK <len> e em seguida os bytes do bloco
                        writer.write(build_ok(len(blk)))
                        writer.write(blk)
                    else:
                        writer.write(b'ERR\n')
                else:
                    writer.write(b'ERR\n')
                await writer.drain()
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
