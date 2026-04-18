# RESP Data Types
ESTR = 0  # Error Simple String  "-ERR message\r\n"
SSTR = 1  # Simple String  "+OK\r\n"
BSTR = 2  # Bulk String    "$6\r\nfoobar\r\n"
BARR = 3  # Bulk Array     "*2\r\n..."
INTR = 4  # Integer        ":42\r\n"


def encode(item, type):
    if type == ESTR:
        return f"-ERR {item}\r\n".encode()
    elif type == SSTR:
        return f"+{item}\r\n".encode()
    elif type == BSTR:
        if item is None:
            return b"$-1\r\n"
        return f"${len(item)}\r\n{item}\r\n".encode()
    elif type == BARR:
        if item is None:
            return b"*-1\r\n"
        if len(item) == 0:
            return b"*0\r\n"
        res = f"*{len(item)}\r\n".encode()
        for each in item:
            if isinstance(each, list):
                res += encode(each, BARR)
            elif isinstance(each, str):
                res += encode(each, BSTR)
        return res
    elif type == INTR:
        return f":{item}\r\n".encode()
    return b"$-1\r\n"


def rdb_encode(store):
    rdb = bytes.fromhex(
        "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
    )
    header = f"${len(rdb)}\r\n".encode()
    return header + rdb


def parse(raw_bytes):
    parts = raw_bytes.split(b"\r\n")
    # print(parts)
    res = []
    bookmark = 0
    while bookmark < len(parts):
        if parts[bookmark] == b"":
            bookmark += 1
            continue
        val, incr = _parse_parts(parts, bookmark)
        res.append(val)
        bookmark += incr
    return res


def _parse_parts(tlist, st):
    # print(tlist[st])
    if not tlist or not tlist[st]:
        return []
    first = tlist[st][0]
    if first == ord("+"):
        return (tlist[st][1:].decode(), 1)
    elif first == ord("$"):
        return (tlist[st + 1].decode(), 2)
    elif first == ord("*"):
        count = int(tlist[st][1:].decode())
        result = []
        cursor = 1
        for _ in range(count):
            val, incr = _parse_parts(tlist, st + cursor)
            result.append(val)
            cursor += incr
        return result, cursor
    return []
