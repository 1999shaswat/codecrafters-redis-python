from itertools import islice
import time

from .resp import ESTR, encode


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
            encode("ERR The ID specified in XADD must be greater than 0-0", ESTR)
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
                "ERR The ID specified in XADD is equal or smaller than the target stream top item",
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


def slice_deque(d, start, stop):
    """Return a list slice from a deque without permanently mutating it."""
    d.rotate(-start)
    result = list(islice(d, 0, stop - start))
    d.rotate(start)
    return result
