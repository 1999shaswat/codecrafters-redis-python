from .resp import ESTR, SSTR, encode, parse
from .commands import COMMAND_HANDLERS


class ConnState:
    def __init__(self):
        self.multi = False
        self.cmd_q = []
        self.watcher = {}


class MockConnection:
    def __init__(self):
        self.res = []  # could be array or directly string...

    def sendall(self, response):
        self.res.append(response)

    def getresponse(self):
        header = f"*{len(self.res)}\r\n".encode()
        return header + b"".join(self.res)


def handle_connection(connection, ctx):
    """Read commands from a single client connection until it closes."""
    conn_state = ConnState()
    while data := connection.recv(1024):
        parsed = parse(data)
        if not parsed:
            continue

        command = parsed[0].upper()

        if command in ("MULTI", "EXEC", "DISCARD", "WATCH"):
            if command == "MULTI":
                conn_state.multi = True
                connection.sendall(encode("OK", SSTR))
            elif command == "EXEC":
                if conn_state.multi:
                    # run exec
                    response = cmd_exec(conn_state.cmd_q, ctx)
                    conn_state.multi = False
                    conn_state.cmd_q.clear()
                    connection.sendall(response)
                else:
                    connection.sendall(encode("EXEC without MULTI", ESTR))
            elif command == "DISCARD":
                # handle discard
                if conn_state.multi:
                    conn_state.multi = False
                    conn_state.cmd_q.clear()
                    connection.sendall(encode("OK", SSTR))
                else:
                    connection.sendall(encode("DISCARD without MULTI", ESTR))
            elif command == "WATCH":
                cmd_watch(parsed, conn_state, ctx)
                connection.sendall(encode("OK", SSTR))
        elif conn_state.multi:  # commands other than MULTI EXEC DISCARD
            conn_state.cmd_q.append(parsed)
            connection.sendall(b"+QUEUED\r\n")
            continue
        else:
            handler = COMMAND_HANDLERS.get(command)

            if handler:
                handler(connection, parsed, ctx)
            else:
                connection.sendall(encode("unknown command", ESTR))

    connection.close()


def cmd_watch(args, conn_state, ctx):
    key = args[1]
    conn_state.watcher[key] = ctx.store.get(key)


def cmd_exec(queue, ctx):
    respCollector = MockConnection()
    for parsed in queue:
        command = parsed[0].upper()
        handler = COMMAND_HANDLERS.get(command)
        if handler:
            handler(respCollector, parsed, ctx)
        else:
            respCollector.sendall(encode("unknown command", ESTR))
        pass
    # need to change encode to just to wrap array and not modify response
    return respCollector.getresponse()
