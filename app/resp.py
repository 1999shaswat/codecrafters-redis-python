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
    rdb = bytes.fromhex("524544495330303131...")
    header = f"${len(rdb)}\r\n".encode()
    return header + rdb


def parse(raw_bytes):
    parts = raw_bytes.split(b"\r\n")
    return _parse_parts(parts)


def _parse_parts(tlist):
    if not tlist or not tlist[0]:
        return []
    first = tlist[0][0]
    if first == ord("+"):
        return tlist[0][1:].decode()
    elif first == ord("$"):
        return tlist[1].decode()
    elif first == ord("*"):
        count = int(tlist[0][1:].decode())
        result = []
        cursor = 1
        for _ in range(count):
            result.append(_parse_parts(tlist[cursor : cursor + 2]))
            cursor += 2
        return result
    return []
