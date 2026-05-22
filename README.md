# Trabalho Prático 2 — Peer-to-Peer (P2P)

Este repositório contém a implementação em Python do Trabalho Prático 2 — Transferência de Arquivos P2P.

Visão geral
- Código principal: `peer/` (protocol, storage, server, client, node)
- Entrada CLI: `run_peer.py` (modos `server`, `peer`, `client`)
- Runner de testes automatizados: `scripts/run_real_tests.py` (use `--label` para pastas curtas em `results/`)

**Decisões de projeto**
- **Linguagem:** Python 3.8+ com `asyncio` para concorrência e simplicidade.
- **Protocolo:** texto simples com mensagens `METADATA`, `BITFIELD`, `GET`, `OK`, `ERR` — facilita depuração e logs.
- **Fragmentação:** blocos fixos (padrão 1024 bytes), configurável via `--block-size`.
- **Bitfield:** cada Peer anuncia os índices de blocos que possui na conexão inicial.
- **Scheduler:** cliente prioriza blocos mais raros entre vizinhos (estratégia de raridade) e baixa em paralelo (limite `--concurrency`).
- **Verificação:** SHA-256 do arquivo completo; estendível para hashes por bloco.
- **Test runner:** `scripts/run_real_tests.py` gera `results/{label}_{timestamp}/` com `sample.bin`, logs, `out_N.bin` e `result.json`.

Pré-requisitos
- Python 3.8+ instalado.

Como executar (resumido)
- Rodar servidor (seeder):

```
python run_peer.py --mode server --host 127.0.0.1 --port 9000 --file arquivo.bin --block-size 1024
```

- Rodar peer híbrido (seeder com arquivo):

```
python run_peer.py --mode peer --host 127.0.0.1 --port 9000 --file arquivo.bin --block-size 1024
```

- Rodar peer híbrido (leecher):

```
python run_peer.py --mode peer --host 127.0.0.1 --port 9001 --neighbors 127.0.0.1:9000 --out recebido.bin --block-size 1024 --total-blocks 10
```

- Runner automatizado (gera arquivo de teste, inicia um seeder + N-1 leechers):

```
python scripts/run_real_tests.py --peers 2 --block-size 1024 --file-size 10240 --label small
```

Parâmetros de teste (seguindo o enunciado)
- Quantidade de peers: 2, 4
- Tamanho do bloco: 1024, 4096 bytes
- Arquivos: File A (10 KB), File B (20 KB) — (File C grande pode ser testado localmente)

Como o runner organiza os resultados
- Cada execução cria `results/{label}_{timestamp}/` com:
  - `sample.bin` — arquivo gerado
  - `seeder.out.log`, `seeder.err.log` — logs do seeder
  - `peer-<port>.out.log`, `peer-<port>.err.log` — logs de cada peer
  - `out_<i>.bin` — arquivo remontado por cada leecher
  - `result.json` — resumo com parâmetros, comandos usados, checksums e `success` boolean

Test runs gerados (executados aqui)
- [results/small_20260522T182908Z](results/small_20260522T182908Z) — 2 peers, block=1024, file=10240 bytes, success=true
- [results/peers2_b1024_s10240_20260522T182928Z](results/peers2_b1024_s10240_20260522T182928Z) — 2 peers, block=1024, file=10240 bytes, success=true
- [results/peers2_b1024_s10240_20260522T183643Z](results/peers2_b1024_s10240_20260522T183643Z) — 2 peers, block=1024, file=10240 bytes, success=true
- [results/peers2_b1024_s10240_20260522T183647Z](results/peers2_b1024_s10240_20260522T183647Z) — 2 peers, block=1024, file=10240 bytes, success=true

Executando a matriz completa de testes
Use os comandos abaixo para gerar cada execução (o `--label` produz um nome curto para a pasta em `results/`):

```
# 2 peers, block 1KB, file 10KB
python scripts/run_real_tests.py --peers 2 --block-size 1024 --file-size 10240 --label p2_b1k_s10k

# 2 peers, block 1KB, file 20KB
python scripts/run_real_tests.py --peers 2 --block-size 1024 --file-size 20480 --label p2_b1k_s20k

# 2 peers, block 4KB, file 10KB
python scripts/run_real_tests.py --peers 2 --block-size 4096 --file-size 10240 --label p2_b4k_s10k

# 2 peers, block 4KB, file 20KB
python scripts/run_real_tests.py --peers 2 --block-size 4096 --file-size 20480 --label p2_b4k_s20k

# 4 peers, block 1KB, file 10KB
python scripts/run_real_tests.py --peers 4 --block-size 1024 --file-size 10240 --label p4_b1k_s10k

# 4 peers, block 1KB, file 20KB
python scripts/run_real_tests.py --peers 4 --block-size 1024 --file-size 20480 --label p4_b1k_s20k

# 4 peers, block 4KB, file 10KB
python scripts/run_real_tests.py --peers 4 --block-size 4096 --file-size 10240 --label p4_b4k_s10k

# 4 peers, block 4KB, file 20KB
python scripts/run_real_tests.py --peers 4 --block-size 4096 --file-size 20480 --label p4_b4k_s20k
```

**Test Results**

Below are the test runs that were executed and their key parameters (folders are under `results/`):

- **results/small_20260522T182908Z/**
	- Peers: 2
	- Block size: 1024 bytes
	- File size: 10240 bytes (10 KB)
	- Total blocks: 10
	- Seeder port: 11001
	- Leecher port: 11002 (neighbors: 127.0.0.1:11001)
	- Output: `out_1.bin` recreated with correct size and SHA-256 `c69abcab821408b480537fd793c6a29dfd6eb11152cc8eaa5cdaa14feac0bc18`

- **results/peers2_b1024_s10240_20260522T182928Z/**
	- Peers: 2
	- Block size: 1024 bytes
	- File size: 10240 bytes (10 KB)
	- Total blocks: 10
	- Seeder port: 11002
	- Leecher port: 11003 (neighbors: 127.0.0.1:11002)
	- Output: `out_1.bin` recreated with correct size and SHA-256 `da0741df88267ada4a4a7b85adaae130b953d400b3f66c9a79e34ad4f6aef3b9`

- **results/peers2_b1024_s10240_20260522T183643Z/**
	- Peers: 2
	- Block size: 1024 bytes
	- File size: 10240 bytes (10 KB)
	- Total blocks: 10
	- Seeder port: 11001
	- Leecher port: 11002 (neighbors: 127.0.0.1:11001)
	- Output: `out_1.bin` recreated with correct size and SHA-256 `c7d38815f158f3d4baf6a2b36c57e5e2f82411c572cf8deea798fa4d586be7b9`

- **results/peers2_b1024_s10240_20260522T183647Z/**
	- Peers: 2
	- Block size: 1024 bytes
	- File size: 10240 bytes (10 KB)
	- Total blocks: 10
	- Seeder port: 11002
	- Leecher port: 11003 (neighbors: 127.0.0.1:11002)
	- Output: `out_1.bin` recreated with correct size and SHA-256 `d25544f49d2a4288c1edcf1c6fa6b719bc2ff59aeb8bc4da4f794092c13a1677`