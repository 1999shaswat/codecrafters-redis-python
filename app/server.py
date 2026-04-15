import argparse
import socket
import threading
import secrets

from .handler import handle_connection

HOST = "localhost"
PORT = 6379


class Context:
    def __init__(self):
        self.store = {}
        self.waiters = {}
        self.lock = threading.Lock()
        self.role = "master"
        self.masterHOST = None
        self.masterPORT = None
        self.master_replid = None
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
    PORT = args.port
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

    with socket.create_server((HOST, PORT), reuse_port=True) as server:
        print(f"Server listening on {HOST}:{PORT}")
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
    master_sock.sendall(b"*1\r\n$4\r\nPING\r\n")
    response = master_sock.recv(1024)
