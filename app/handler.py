from .commands import COMMAND_HANDLERS
from .resp import BARR, ESTR, SSTR, encode, parse

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
        self.channels = []


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
    clientConnection = connection

    while data := clientConnection.recv(1024):
        parsed_list = parse(data)
        if not parsed_list:
            continue

        for parsed, _ in parsed_list:
            # print(parsed, ctx.role, ctx.master_repl_offset, ctx.store)
            if not parsed:
                continue
            command = parsed[0].upper()

            if command in {
                "SUBSCRIBE",
                "UNSUBSCRIBE",
                "PSUBSCRIBE",
                "PUNSUBSCRIBE",
                "QUIT",
            }:
                handle_pubsub_cmds(connection, ctx, conn_state, parsed, command)
            elif conn_state.channels:  # in subscriber mode, block other commands
                if command == "PING":
                    connection.sendall(encode(["pong", ""], BARR))
                    continue
                connection.sendall(
                    encode(
                        f"ERR Can't execute '{command}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
                        ESTR,
                    )
                )
            elif command in {"MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH"}:
                handle_transaction_cmds(connection, ctx, conn_state, parsed, command)
            elif conn_state.multi:
                # commands other than MULTI EXEC DISCARD, never used in Slaves
                conn_state.cmd_q.append(parsed)
                connection.sendall(b"+QUEUED\r\n")
                continue
            else:
                # Reject write commands from client in slaves
                if ctx.role == "slave" and command in WRITE_CMDS:
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

            if ctx.role == "master" and command in WRITE_CMDS:
                ctx.master_repl_offset += len(data)
                # with ctx.lock:
                for slave in ctx.slaves:
                    slave.sendall(data)

            # print(ctx.role, ctx.store)

    clientConnection.close()


def cmd_watch(args, conn_state, ctx):
    keys = args[1:]
    for key in keys:
        conn_state.watching[key] = ctx.store.get(key)


# SUSCRIPTIONS CODE HERE
def cmd_subscribe(connection, args, ctx, conn_state):
    for channel in args[1:]:
        subs = ctx.channels.setdefault(channel, [])
        subs.append(connection)
        conn_state.channels.append(channel)
        connection.sendall(
            encode(["subscribe", channel, len(conn_state.channels)], BARR)
        )


def cmd_unsubscribe(connection, args, ctx, conn_state):
    channel = args[1]
    if channel in conn_state.channels:
        conn_state.channels.remove(channel)
        ctx.channels[channel].remove(channel)
    connection.sendall(encode(["unsubscribe", channel, len(conn_state.channels)], BARR))


def handle_pubsub_cmds(connection, ctx, conn_state, parsed, command):
    if command == "SUBSCRIBE":
        cmd_subscribe(connection, parsed, ctx, conn_state)
    if command == "UNSUBSCRIBE":
        cmd_unsubscribe(connection, parsed, ctx, conn_state)


# TRANSACTION CODE HERE
def handle_transaction_cmds(connection, ctx, conn_state, parsed, command):
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
            connection.sendall(encode("WATCH inside MULTI is not allowed", ESTR))
        else:
            cmd_watch(parsed, conn_state, ctx)
            connection.sendall(encode("OK", SSTR))
    elif command == "UNWATCH":
        conn_state.watching.clear()
        connection.sendall(encode("OK", SSTR))


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


# SLAVE CODE HERE
# """Used by slave to not send response to master (for write cmds)"""
class MockSlaveConnection:
    def sendall(self, _response):
        pass


def handle_master_connection(ctx, initial_data=b""):
    """Read commands from a master as slave."""
    mockSlaveConnection = MockSlaveConnection()
    pending = initial_data

    while True:
        if pending:
            data = pending
            pending = b""
        else:
            data = ctx.master_sock.recv(1024)
            if not data:
                break

        parsed_list = parse(data)
        if not parsed_list:
            continue

        for parsed, consumed_bytes in parsed_list:
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
            if not is_getack:
                connection = mockSlaveConnection
            else:
                connection = ctx.master_sock

            handler = COMMAND_HANDLERS.get(command)
            if handler:
                handler(connection, parsed, ctx)
            else:
                connection.sendall(encode("unknown command", ESTR))

            ctx.master_repl_offset += consumed_bytes

    ctx.master_sock.close()
