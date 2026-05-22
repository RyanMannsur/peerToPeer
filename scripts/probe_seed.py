"""Conecta ao seeder em 127.0.0.1:11000 e imprime METADATA e BITFIELD."""
import socket

HOST = '127.0.0.1'
PORT = 11000

try:
    s = socket.create_connection((HOST, PORT), timeout=3)
    f = s.makefile('rb')
    meta = f.readline().decode().rstrip('\n')
    bitf = f.readline().decode().rstrip('\n')
    print('META:', meta)
    print('BITF:', bitf)
    s.close()
except Exception as e:
    print('ERROR connecting to seeder:', e)
