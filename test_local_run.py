"""Auxiliar de teste: inicia servidor e cliente no mesmo loop para validar fluxo básico."""
import asyncio
import os
from peer.server import PeerServer
from peer.client import PeerClient
from peer.storage import fragment_file, reassemble_file, sha256_file

async def main():
    # cria arquivo de exemplo
    sample = 'sample.bin'
    out = 'out.bin'
    data = os.urandom(10 * 1024)  # 10 KB
    with open(sample, 'wb') as f:
        f.write(data)

    blocks = fragment_file(sample, 1024)
    sha = sha256_file(sample)

    server = PeerServer('127.0.0.1', 9000, blocks, 1024, sha)

    async def run_server():
        await server.start()

    async def run_client():
        client = PeerClient([('127.0.0.1', 9000)], out, len(blocks))
        await asyncio.sleep(0.2)  # deixa o servidor iniciar
        await client.run()

    # executa servidor e cliente concorrentemente; cliente encerra, servidor é cancelado
    srv = asyncio.create_task(run_server())
    cli = asyncio.create_task(run_client())
    await cli
    srv.cancel()
    try:
        await srv
    except asyncio.CancelledError:
        pass

    # verifica integridade
    if os.path.exists(out):
        s1 = sha256_file(sample)
        s2 = sha256_file(out)
        print('sha sample:', s1)
        print('sha out:   ', s2)
        print('match:', s1 == s2)
    else:
        print('Cliente não gerou saída')

if __name__ == '__main__':
    asyncio.run(main())
