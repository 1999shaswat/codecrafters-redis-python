from itertools import islice
from math import floor, ceil
import socket
import time
import sys

from .resp import BARR, ESTR, encode, parse


def get_slave_status(slave, offset):
    slave.sendall(encode(["REPLCONF", "GETACK", "*"], BARR))
    # data = slave.recv(1024)
    # parsed_list = parse(data)
    # if not parsed_list:
    #     return 0
    # for parsed, _ in parsed_list:
    #     if parsed[0].upper() == "REPLCONF" and parsed[1].upper() == "ACK":
    #         return 1 if int(parsed[2]) >= offset else 0


def recv_until_crlf(sock, buf):
    """Read from sock until buf contains \r\n, return (line_with_crlf, remainder)."""
    while b"\r\n" not in buf:
        chunk = sock.recv(1024)
        if not chunk:
            break
        buf += chunk
    idx = buf.index(b"\r\n")
    return buf[: idx + 2], buf[idx + 2 :]


def flatten_entry(entry):
    """Flattens each stream entry"""
    eid, d = entry
    fields = [x for item in d.items() for x in item]
    return [eid, fields]


def bsearch_lower(stream, val):
    """stream: only for start: lower bound"""
    if "-" not in val:
        val += "-0"
    s, e = 0, len(stream) - 1
    tgt = parse_id(val)
    while s < e:
        m = floor((s + e) / 2)
        cur = parse_id(stream[m][0])
        if cur < tgt:
            s = m + 1
        else:
            e = m
    return s


def bsearch_upper(stream, val):
    """stream: only for end: upper bound"""
    if "-" not in val:
        val += f"-{sys.maxsize}"
    s, e = 0, len(stream) - 1
    tgt = parse_id(val)
    while s < e:
        m = ceil((s + e) / 2)
        cur = parse_id(stream[m][0])
        if cur > tgt:
            e = m - 1
        else:
            s = m
    return e


def autogenerate(stream, eid):
    """Autogenerate the stream entry ID"""
    if eid == "*":
        eid = "*-*"
    ts, seq = eid.split("-")
    gts, gseq = ts == "*", seq == "*"
    if gts:
        ts = time.time_ns() // 1_000_000

    if not stream:
        if gseq:
            seq = 1 if ts == "0" else 0
    else:
        last_eid = stream[-1][0]
        lts, lseq = last_eid.split("-")
        if gseq:
            seq = int(lseq) + 1 if ts == lts else 0

    return f"{ts}-{seq}"


def is_valid(connection, stream, eid):
    """Validate the stream entry ID"""
    auto_gen = "*" in eid
    if not auto_gen and not (parse_id(eid) > parse_id("0-0")):
        connection.sendall(
            encode("The ID specified in XADD must be greater than 0-0", ESTR)
        )
        return False

    valid = True
    if stream:
        last_eid = stream[-1][0]
        if not auto_gen and not (parse_id(eid) > parse_id(last_eid)):
            valid = False
        eid_ts = eid.split("-")[0]
        if auto_gen and eid_ts != "*" and (parse_id(last_eid)[0] > int(eid_ts)):
            valid = False

    if not valid:
        connection.sendall(
            encode(
                "The ID specified in XADD is equal or smaller than the target stream top item",
                ESTR,
            )
        )
    return valid


def parse_id(id):
    """Parse the stream entry ID into a tuple"""
    ms, seq = id.split("-")
    return (int(ms), int(seq))


def delete_key(keystore, key):
    """Called by expiry timers to remove a key from the datastore."""
    keystore.pop(key, None)


def safe_convert(value):
    """Safely convert value to number (int/float) if possible"""
    try:
        if "." not in value:
            return int(value)
        return float(value)
    except ValueError:
        return value


def slice_deque(d, start, stop):
    """Return a list slice from a deque without permanently mutating it."""
    d.rotate(-start)
    result = list(islice(d, 0, stop - start))
    d.rotate(start)
    return result
