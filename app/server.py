import socket
import threading

from .handler import handle_connection

HOST = "localhost"
PORT = 6379


class Context:
    def __init__(self):
        self.store = {}
        self.waiters = {}
        self.lock = threading.Lock()


def run():
    """Start the TCP server and accept client connections."""
    ctx = Context()
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
