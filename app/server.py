import argparse
import socket
import threading
import secrets

from .resp import BARR, encode

from .handler import handle_connection
from app import resp


class Context:
    def __init__(self):
        self.host = "localhost"
        self.port = 6379
        self.store = {}
        self.waiters = {}
        self.lock = threading.Lock()
        self.role = "master"
        self.masterHOST = ""
        self.masterPORT = 0
        self.master_replid = ""
        self.master_repl_offset = 0


def run():
    """Start the TCP server and accept client connections."""
    ctx = Context()
    parser = argparse.ArgumentParser(description="Redis Server (python)")
    parser.add_argument(
        "--port", type=int, default=6379, help="Port number (default: 6379)"
    )
    parser.add_argument("--replicaof", help="Start replica server")
    args = parser.parse_args()
    ctx.port = args.port

    if args.replicaof:
        ctx.role = "slave"
        mhost, mport = args.replicaof.split(" ")
        ctx.masterHOST = mhost
        ctx.masterPORT = int(mport)

    if ctx.role == "master":
        ctx.master_replid = secrets.token_hex(20)

    if ctx.role == "slave":
        slavethread = threading.Thread(
            target=initalize_slave,
            args=(ctx,),
        )
        slavethread.daemon = True
        slavethread.start()

    with socket.create_server((ctx.host, ctx.port), reuse_port=True) as server:
        print(f"Server {ctx.role} listening on {ctx.host}:{ctx.port}")
        while True:
            connection, address = server.accept()
            print(f"New connection from {address}")
            thread = threading.Thread(
                target=handle_connection,
                args=(
                    connection,
                    ctx,
                ),
            )
            thread.daemon = True
            thread.start()


def initalize_slave(ctx):
    master_sock = socket.create_connection((ctx.masterHOST, ctx.masterPORT))
    # master_sock.sendall(b"*1\r\n$4\r\nPING\r\n")
    master_sock.sendall(encode(["PING"], BARR))
    response = master_sock.recv(1024)
    # print(response)
    master_sock.sendall(encode(["REPLCONF", "listening-port", str(ctx.port)], BARR))
    response = master_sock.recv(1024)
    master_sock.sendall(encode(["REPLCONF", "capa", "psync2"], BARR))
    response = master_sock.recv(1024)
