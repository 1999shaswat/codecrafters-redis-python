from .resp import BARR, ESTR, SSTR, encode, parse
from .commands import COMMAND_HANDLERS

WRITE_CMDS = {
    "SET",
    "RPUSH",
    "LPUSH",
    "LPOP",
    "BLPOP",
    "XADD",
    "INCR",
}


class ConnState:
    def __init__(self):
        self.multi = False
        self.cmd_q = []
        self.watching = {}


class MockConnection:
    def __init__(self):
        self.res = []  # could be array or directly string...

    def sendall(self, response):
        self.res.append(response)

    def getresponse(self):
        header = f"*{len(self.res)}\r\n".encode()
        return header + b"".join(self.res)


# """Used by slave to not send response to master (for write cmds)"""
class MockSlaveConnection:
    def sendall(self, _response):
        pass


def handle_connection(connection, ctx):
    """1. Read commands from a single client connection until it closes."""
    """2. Read commands from a master as slave."""
    conn_state = ConnState()
    mockSlaveConnection = MockSlaveConnection()
    clientConnection = connection

    while data := clientConnection.recv(1024):
        parsed_list = parse(data)
        if not parsed_list:
            continue

        for parsed in parsed_list:
            # print(parsed, ctx.role, ctx.master_repl_offset, ctx.store)
            if not parsed:
                continue
            command = parsed[0].upper()
            is_getack = (
                command == "REPLCONF"
                and len(parsed) > 1
                and parsed[1].upper() == "GETACK"
            )
            # Dont send response (to master) on write commands
            if (
                ctx.role == "slave"
                and clientConnection is ctx.master_sock
                and not is_getack
            ):
                connection = mockSlaveConnection
            else:
                connection = clientConnection

            if command in {"MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH"}:
                # master never sends above to slave
                if command == "MULTI":
                    conn_state.multi = True
                    connection.sendall(encode("OK", SSTR))
                elif command == "EXEC":
                    if conn_state.multi:
                        # run exec
                        response = cmd_exec(conn_state, ctx)
                        conn_state.multi = False
                        conn_state.cmd_q.clear()
                        conn_state.watching.clear()
                        connection.sendall(response)
                    else:
                        connection.sendall(encode("EXEC without MULTI", ESTR))
                elif command == "DISCARD":
                    # handle discard
                    if conn_state.multi:
                        conn_state.multi = False
                        conn_state.cmd_q.clear()
                        conn_state.watching.clear()
                        connection.sendall(encode("OK", SSTR))
                    else:
                        connection.sendall(encode("DISCARD without MULTI", ESTR))
                elif command == "WATCH":
                    if conn_state.multi:
                        connection.sendall(
                            encode("WATCH inside MULTI is not allowed", ESTR)
                        )
                    else:
                        cmd_watch(parsed, conn_state, ctx)
                        connection.sendall(encode("OK", SSTR))
                elif command == "UNWATCH":
                    conn_state.watching.clear()
                    connection.sendall(encode("OK", SSTR))

            elif conn_state.multi:
                # commands other than MULTI EXEC DISCARD, never used in Slaves
                conn_state.cmd_q.append(parsed)
                connection.sendall(b"+QUEUED\r\n")
                continue
            else:
                # Reject write commands from client in slaves
                if (
                    ctx.role == "slave"
                    and clientConnection is not ctx.master_sock
                    and command in WRITE_CMDS
                ):
                    connection.sendall(
                        encode(
                            "READONLY You can't write against a read only slave", ESTR
                        )
                    )
                    continue

                handler = COMMAND_HANDLERS.get(command)
                if handler:
                    handler(connection, parsed, ctx)
                else:
                    connection.sendall(encode("unknown command", ESTR))

            if ctx.role == "slave" and clientConnection is ctx.master_sock:
                ctx.master_repl_offset += len(encode(parsed, BARR))

            if ctx.role == "master" and command in WRITE_CMDS:
                # print("sent to all slaves")
                ctx.master_repl_offset += len(data)
                for slave in ctx.slaves:
                    slave.sendall(data)

            # print(ctx.role, ctx.store)

    clientConnection.close()


def cmd_watch(args, conn_state, ctx):
    keys = args[1:]
    for key in keys:
        conn_state.watching[key] = ctx.store.get(key)


def cmd_exec(conn_state, ctx):
    respCollector = MockConnection()
    for key in conn_state.watching:
        if conn_state.watching[key] != ctx.store.get(key):
            return encode(None, BARR)

    for parsed in conn_state.cmd_q:
        command = parsed[0].upper()
        handler = COMMAND_HANDLERS.get(command)
        if handler:
            handler(respCollector, parsed, ctx)
        else:
            respCollector.sendall(encode("unknown command", ESTR))
    # need to change encode to just to wrap array and not modify response
    return respCollector.getresponse()
