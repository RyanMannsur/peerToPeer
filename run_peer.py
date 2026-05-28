"""Ponto de entrada CLI para executar um peer nos modos server, client ou peer (híbrido).

Opções:
- --mode: 'server' (apenas servidor), 'client' (apenas cliente) ou 'peer' (servidor+cliente hóspede).
"""
import argparse
import asyncio
from peer.server import PeerServer
from peer.client import PeerClient
from peer.storage import fragment_file, sha256_file, DEFAULT_BLOCK_SIZE

def parse_neighbors(s: str):
    if not s:
        return []
    parts = s.split(',')
    res = []
    for p in parts:
        host, port = p.split(':')
        res.append((host, int(port)))
    return res

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--mode', choices=('server','client','peer'), required=True)
    ap.add_argument('--host', default='127.0.0.1')
    ap.add_argument('--port', type=int, required=True)
    ap.add_argument('--file', help='file to serve (server mode)')
    ap.add_argument('--neighbors', help='comma-separated host:port list (client mode)')
    ap.add_argument('--out', help='output file (client mode)')
    ap.add_argument('--block-size', type=int, default=DEFAULT_BLOCK_SIZE)
    ap.add_argument('--concurrency', type=int, default=8, help='concurrency para downloads')
    ap.add_argument('--total-blocks', type=int, default=None, help='total de blocos (evita prompt interativo)')
    ap.add_argument('--no-seed-after-complete', action='store_true',
                    help='Encerra o servidor após o download (por padrão, mantém servindo)')
    args = ap.parse_args()

    if args.mode == 'server':
        assert args.file, 'server mode requires --file'
        blocks = fragment_file(args.file, args.block_size)
        sha = sha256_file(args.file)
        server = PeerServer(args.host, args.port, blocks, args.block_size, sha)
        asyncio.run(server.start())
    elif args.mode == 'client':
        assert args.out, 'client mode requires --out'
        neighbors = parse_neighbors(args.neighbors)
        total_blocks = int(input('Total blocks count: '))
        client = PeerClient(neighbors, args.out, total_blocks)
        asyncio.run(client.run())
    else:  # peer (hybrid)
        from peer.node import PeerNode
        neighbors = parse_neighbors(args.neighbors)
        seed_after = not args.no_seed_after_complete
        if args.file:
            node = PeerNode(
                args.host, args.port, neighbors,
                file=args.file, out=args.out, block_size=args.block_size,
                seed_after_complete=seed_after, concurrency=args.concurrency,
            )
        else:
            total_blocks = args.total_blocks if args.total_blocks is not None else int(input('Total blocks count: '))
            node = PeerNode(
                args.host, args.port, neighbors,
                file=None, out=args.out, block_size=args.block_size,
                total_blocks=total_blocks,
                seed_after_complete=seed_after, concurrency=args.concurrency,
            )
        import logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
        asyncio.run(node.start())

if __name__ == '__main__':
    main()
