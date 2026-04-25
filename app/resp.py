import time

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
            elif isinstance(each, int):
                res += encode(each, INTR)
        return res
    elif type == INTR:
        return f":{item}\r\n".encode()
    return b"$-1\r\n"


def rdb_decode(filename):
    store = {}
    try:
        with open(filename, "rb") as f:
            # header
            header = f.read(9)
            print(f"header: {header.decode()}")
            while True:
                opcode = f.read(1)
                if not opcode or opcode == b"\xff":
                    break
                if opcode == b"\xfa":
                    key = rdb_read_string(f)
                    val = rdb_read_string(f)
                    print(f"Metadata: {key} = {val}")
                elif opcode == b"\xfe":
                    db_index = rdb_read_length(f)
                    print(f"Switching to DB: {db_index}")
                elif opcode == b"\xfb":
                    table_size, _ = rdb_read_length(f)
                    exp_table_size, _ = rdb_read_length(f)
                    print(
                        f"Table size: {table_size}, Expire Table size: {exp_table_size}"
                    )
                else:
                    expiry = None
                    value_type = opcode
                    if opcode == b"\xfc":
                        expiry = int.from_bytes(f.read(8), "little") / 1000
                        value_type = f.read(1)
                    if opcode == b"\xfd":
                        expiry = int.from_bytes(f.read(4), "little")
                        value_type = f.read(1)

                    key = rdb_read_string(f)
                    value = rdb_read_string(f)
                    if expiry is None or expiry > time.time():
                        store[key] = value
    except FileNotFoundError:
        pass
    return store


def rdb_read_length(f):
    first_byte = int.from_bytes(f.read(1), "big")
    encoding_type = (first_byte & 0xC0) >> 6
    if encoding_type == 0:
        return first_byte & 0x3F, False
    elif encoding_type == 1:
        second_byte = int.from_bytes(f.read(1), "big")
        return ((first_byte & 0x3F) << 8) | second_byte, False
    elif encoding_type == 2:
        return int.from_bytes(f.read(4), "big"), False
    else:  # encoding_type == 3
        return first_byte & 0x3F, True


def rdb_read_string(f):
    length, is_special = rdb_read_length(f)
    if is_special:
        if length == 0:
            return str(int.from_bytes(f.read(1), "little"))
        if length == 1:
            return str(int.from_bytes(f.read(2), "little"))
        if length == 2:
            return str(int.from_bytes(f.read(4), "little"))
    else:
        return f.read(length).decode("latin-1")


def rdb_encode(store):
    rdb = bytes.fromhex(
        "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
    )
    header = f"${len(rdb)}\r\n".encode()
    return header + rdb


# def parse(raw_bytes):
#     parts = raw_bytes.split(b"\r\n")
#     # print(parts)
#     res = []
#     bookmark = 0
#     while bookmark < len(parts):
#         if parts[bookmark] == b"":
#             bookmark += 1
#             continue
#         val, incr = _parse_parts(parts, bookmark)
#         res.append(val)
#         bookmark += incr
#     return res
#
#
# def _parse_parts(tlist, st):
#     # print(tlist[st])
#     if not tlist or not tlist[st]:
#         return []
#     first = tlist[st][0]
#     if first == ord("+"):
#         return (tlist[st][1:].decode(), 1)
#     elif first == ord("$"):
#         return (tlist[st + 1].decode(), 2)
#     elif first == ord("*"):
#         count = int(tlist[st][1:].decode())
#         result = []
#         cursor = 1
#         for _ in range(count):
#             val, incr = _parse_parts(tlist, st + cursor)
#             result.append(val)
#             cursor += incr
#         return result, cursor
#     return []


def parse(raw_bytes):
    """Returns a list of (parsed_command_list, bytes_consumed)"""
    res = []
    cursor = 0
    while cursor < len(raw_bytes):
        start = cursor
        parsed_val, new_cursor = _parse_recursive(raw_bytes, cursor)

        # This is the exact byte count for the replication offset
        consumed = new_cursor - start
        res.append((parsed_val, consumed))

        cursor = new_cursor
    return res


def _parse_recursive(data, pos):
    prefix = data[pos : pos + 1]
    # Find the end of the current RESP header line
    line_end = data.find(b"\r\n", pos)
    if line_end == -1:
        return None, pos

    header = data[pos + 1 : line_end].decode()

    if prefix == b"*":  # Array
        count = int(header)
        current_pos = line_end + 2
        items = []
        for _ in range(count):
            item, next_pos = _parse_recursive(data, current_pos)
            items.append(item)
            current_pos = next_pos
        return items, current_pos

    elif prefix == b"$":  # Bulk String
        length = int(header)
        if length == -1:
            return None, line_end + 2

        # Read exactly 'length' bytes, skipping the prefix/header
        str_start = line_end + 2
        str_end = str_start + length
        # Bulk strings end with \r\n (2 bytes)
        return data[str_start:str_end].decode(), str_end + 2

    elif prefix == b"+":  # Simple String
        return header, line_end + 2

    return None, len(data)
