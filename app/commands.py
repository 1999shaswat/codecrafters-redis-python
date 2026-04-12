import threading
from collections import deque

from .resp import encode, SSTR, BSTR, BARR, INTR
from .utils import delete_key, slice_deque


def cmd_ping(connection, _args, _ctx):
    connection.sendall(encode("PONG", SSTR))


def cmd_echo(connection, args, _ctx):
    connection.sendall(encode(args[1], BSTR))


def cmd_set(connection, args, ctx):
    store = ctx.store
    store[args[1]] = args[2]
    if len(args) == 5:
        time = int(args[4])
        if args[3].upper() == "PX":
            time /= 1000
        threading.Timer(time, delete_key, args=(store, args[1])).start()
    connection.sendall(encode("OK", SSTR))


def cmd_get(connection, args, ctx):
    store = ctx.store
    value = store.get(args[1])
    connection.sendall(encode(value, BSTR))


def cmd_llen(connection, args, ctx):
    store = ctx.store
    dq = store.get(args[1], deque())
    connection.sendall(encode(len(dq), INTR))


def cmd_rpush(connection, args, ctx):
    store = ctx.store
    lock = ctx.lock
    waiter = {"q": deque([]), "e": threading.Event()}
    with lock:
        waiter = ctx.waiters.setdefault(args[1], waiter)
        dq = store.get(args[1], deque())
        dq.extend(args[2:])
        store[args[1]] = dq
        waiter["e"].set()
    connection.sendall(encode(len(dq), INTR))


def cmd_lpush(connection, args, ctx):
    store = ctx.store
    lock = ctx.lock
    waiter = {"q": deque([]), "e": threading.Event()}
    with lock:
        waiter = ctx.waiters.setdefault(args[1], waiter)
        dq = store.get(args[1], deque())
        dq.extendleft(args[2:])
        store[args[1]] = dq
        waiter["e"].set()
    connection.sendall(encode(len(dq), INTR))


def cmd_lrange(connection, args, ctx):
    store = ctx.store
    start, end = int(args[2]), int(args[3])
    dq = store.get(args[1], deque())
    if start < 0:
        start = max(len(dq) + start, 0)
    if end < 0:
        end = max(len(dq) + end, 0)
    connection.sendall(encode(slice_deque(dq, start, end + 1), BARR))


def cmd_lpop(connection, args, ctx):
    store = ctx.store
    dq = store.get(args[1], deque())
    ret_arr = len(args) == 3
    popn = int(args[2]) if ret_arr else 1

    popitems = []
    if dq:
        if popn >= len(dq):
            popitems = list(dq)
            dq.clear()
        else:
            popitems = [dq.popleft() for _ in range(popn)]

    if ret_arr:
        connection.sendall(encode(popitems, BARR))
    else:
        value = popitems[0] if popitems else None
        connection.sendall(encode(value, BSTR))


def cmd_blpop(connection, args, ctx):
    store = ctx.store
    lock = ctx.lock
    result = [args[1]]
    timeout = float(args[-1])
    timeout = None if timeout == 0 else timeout
    thread_id = threading.get_ident()
    waiter = {"q": deque([]), "e": threading.Event()}
    with lock:
        dq = store.get(args[1], deque())
        if dq:
            result.append(dq.popleft())
            connection.sendall(encode(result, BARR))
            return
        waiter = ctx.waiters.setdefault(args[1], waiter)
        waiter["q"].append(thread_id)
        waiter["e"].clear()
    alive = True
    while True:
        alive = waiter["e"].wait(timeout)
        if not alive:
            with lock:
                waiter["q"].remove(thread_id)
            connection.sendall(encode(None, BARR))
            return

        if waiter["q"][0] == thread_id:
            with lock:
                waiter["e"].clear()
                dq = store.get(args[1], deque())
                result.append(dq.popleft())
                waiter["q"].popleft()
                if len(waiter["q"]) > 0 and len(dq) > 0:
                    waiter["e"].set()
                else:
                    waiter["e"].clear()
            connection.sendall(encode(result, BARR))
            return
        else:
            with lock:
                waiter["e"].clear()


def cmd_type(connection, args, ctx):
    val = ctx.store.get(args[1])
    connection.sendall(encode(TYPES[type(val).__name__], SSTR))


TYPES = {"str": "string", "NoneType": "none"}

# Dispatch table: command name → handler function
COMMAND_HANDLERS = {
    "PING": cmd_ping,
    "ECHO": cmd_echo,
    "SET": cmd_set,
    "GET": cmd_get,
    "LLEN": cmd_llen,
    "RPUSH": cmd_rpush,
    "LPUSH": cmd_lpush,
    "LRANGE": cmd_lrange,
    "LPOP": cmd_lpop,
    "BLPOP": cmd_blpop,
    "TYPE": cmd_type,
}
