from .resp import BARR, encode, parse
from .commands import COMMAND_HANDLERS


class ConnState:
    def __init__(self):
        self.multi = False
        self.cmd_q = []


def handle_connection(connection, ctx):
    """Read commands from a single client connection until it closes."""
    conn_state = ConnState()
    while data := connection.recv(1024):
        parsed = parse(data)
        if not parsed:
            continue

        command = parsed[0].upper()

        if command in ("MULTI", "EXEC", "DISCARD"):
            if command == "MULTI":
                conn_state.multi = True
                connection.sendall(b"+OK\r\n")
            elif command == "EXEC":
                if conn_state.multi:
                    # run exec
                    response = cmd_exec(conn_state.cmd_q)
                    conn_state.multi = False
                    conn_state.cmd_q.clear()
                    connection.sendall(response)
                else:
                    connection.sendall(b"-ERR EXEC without MULTI\r\n")
            elif command == "DISCARD":
                # handle discard
                pass
        elif conn_state.multi:  # commands other than MULTI EXEC DISCARD
            conn_state.cmd_q.append((parsed))
            connection.sendall(b"+QUEUED\r\n")
            continue
        else:
            handler = COMMAND_HANDLERS.get(command)

            if handler:
                handler(connection, parsed, ctx)
            else:
                connection.sendall(b"-ERR unknown command\r\n")

    connection.close()


def cmd_exec(queue):
    res = []
    for cmd in queue:
        pass
    return encode(res, BARR)
