from itertools import islice


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

