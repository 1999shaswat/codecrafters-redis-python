import threading
from collections import deque

from .resp import BARR, BSTR, ESTR, INTR, SSTR, encode
from .utils import (
    autogenerate,
    bsearch_lower,
    bsearch_upper,
    delete_key,
    flatten_entry,
    safe_convert,
    slice_deque,
    is_valid,
)


def cmd_ping(connection, _args, _ctx):
    connection.sendall(encode("PONG", SSTR))


def cmd_echo(connection, args, _ctx):
    connection.sendall(encode(args[1], BSTR))


def cmd_set(connection, args, ctx):
    store = ctx.store
    store[args[1]] = safe_convert(args[2])
    if len(args) == 5:
        time = int(args[4])
        if args[3].upper() == "PX":
            time /= 1000
        threading.Timer(time, delete_key, args=(store, args[1])).start()
    connection.sendall(encode("OK", SSTR))


def cmd_get(connection, args, ctx):
    store = ctx.store
    value = store.get(args[1])
    if value:
        value = str(value)
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
            return connection.sendall(encode(result, BARR))
        waiter = ctx.waiters.setdefault(args[1], waiter)
        waiter["q"].append(thread_id)
        waiter["e"].clear()
    alive = True
    while True:
        alive = waiter["e"].wait(timeout)
        if not alive:
            with lock:
                waiter["q"].remove(thread_id)
            return connection.sendall(encode(None, BARR))

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


def cmd_xadd(connection, args, ctx):
    eid = args[2]
    lock = ctx.lock
    with lock:
        waiter_event = ctx.waiters.setdefault(args[1], threading.Event())
        stream = ctx.store.setdefault(args[1], [])
        if eid != "*" and not is_valid(connection, stream, eid):
            return
        eid = autogenerate(stream, eid)
        e_dict = {args[i]: args[i + 1] for i in range(3, len(args), 2)}
        stream.append((eid, e_dict))
        waiter_event.set()
    connection.sendall(encode(eid, BSTR))


def cmd_xrange(connection, args, ctx):
    start, end = args[2], args[3]
    stream = ctx.store.setdefault(args[1], [])
    s_ind = 0 if start == "-" else bsearch_lower(stream, start)
    e_ind = len(stream) - 1 if end == "+" else bsearch_upper(stream, end)
    result = [flatten_entry(e) for e in stream[s_ind : e_ind + 1]]
    connection.sendall(encode(result, BARR))


def cmd_xread(connection, args, ctx):
    if args[1].upper() == "BLOCK":
        return cmd_xread_block(connection, args, ctx)
    streams_args = args[2:]
    mid = len(streams_args) // 2
    keys = streams_args[:mid]
    eids = streams_args[mid:]
    result = []
    for key, eid in zip(keys, eids):
        stream = ctx.store.setdefault(key, [])
        start = bsearch_lower(stream, eid)
        if start < len(stream) and stream[start][0] == eid:
            start += 1
        tmp = [flatten_entry(e) for e in stream[start:]]
        result.append([key, tmp])
    connection.sendall(encode(result, BARR))


def cmd_xread_block(connection, args, ctx):
    timeout = float(args[2]) / 1000
    timeout = None if timeout == 0 else timeout
    key, eid = args[4], args[5]
    stream = ctx.store.setdefault(key, [])
    if eid == "$":
        eid = stream[-1][0] if stream else "0-0"
    lock = ctx.lock
    result = []
    waiter_event = ctx.waiters.setdefault(key, threading.Event())
    while True:
        alive = waiter_event.wait(timeout)
        if not alive:
            return connection.sendall(encode(None, BARR))
        with lock:
            start = bsearch_lower(stream, eid)
            if start < len(stream):
                if stream[start][0] == eid:
                    start += 1
            if start < len(stream):
                tmp = [flatten_entry(e) for e in stream[start:]]
                result.append([key, tmp])
                return connection.sendall(encode(result, BARR))
            waiter_event.clear()


def cmd_incr(connection, args, ctx):
    val = ctx.store.setdefault(args[1], 0)
    if isinstance(val, str):
        return connection.sendall(
            encode("value is not an integer or out of range", ESTR)
        )
    val += 1
    ctx.store[args[1]] = val
    connection.sendall(encode(val, INTR))


def cmd_info(connection, args, ctx):
    info = {
        "role": ctx.role,
        "master_replid": ctx.master_replid,
        "master_repl_offset": ctx.master_repl_offset,
    }
    res = "# Replication\r\n"
    res += "\r\n".join([f"{k}:{v}" for k, v in zip(info.keys(), info.values())])
    connection.sendall(encode(res, BSTR))


def cmd_replconf(connection, args, ctx):
    connection.sendall(encode("OK", SSTR))


TYPES = {"str": "string", "NoneType": "none", "list": "stream"}

# Dispatch table: command name: handler function
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
    "XADD": cmd_xadd,
    "XRANGE": cmd_xrange,
    "XREAD": cmd_xread,
    "INCR": cmd_incr,
    "INFO": cmd_info,
    "REPLCONF": cmd_replconf,
}
