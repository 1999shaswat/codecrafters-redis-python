from .resp import parse
from .commands import COMMAND_HANDLERS


def handle_connection(connection, ctx):
    """Read commands from a single client connection until it closes."""

    while data := connection.recv(1024):
        parsed = parse(data)
        if not parsed:
            continue

        command = parsed[0].upper()
        handler = COMMAND_HANDLERS.get(command)

        if handler:
            handler(connection, parsed, ctx)
        else:
            connection.sendall(b"-ERR unknown command\r\n")

    connection.close()

