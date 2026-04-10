import socket  # noqa: F401
import threading
from collections import deque
from itertools import islice

# Deque Data Types
SSTR = 1
BSTR = 2
BARR = 3
INTR = 4


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage
    #
    with socket.create_server(("localhost", 6379), reuse_port=True) as server:
        while True:  # wait for client
            connection, _ = server.accept()
            thread = threading.Thread(target=task, args=(connection,))
            thread.start()


def task(connection):  # listen for connections
    datastore = {}
    # liststore = []
    while data := connection.recv(1024):
        array = respParse(data)
        command = array[0].upper()
        if command == "PING":
            connection.sendall(respEncoder("PONG", SSTR))
        elif command == "ECHO":
            connection.sendall(respEncoder(array[1], BSTR))
        elif command == "SET":
            datastore[array[1]] = array[2]
            if len(array) == 5:
                time = int(array[4])
                if array[3] == "PX":
                    time /= 1000
                threading.Timer(time, deletekey, args=(datastore, array[1])).start()
            connection.sendall(respEncoder("OK", SSTR))
        elif command == "GET":
            val = datastore.get(array[1])
            connection.sendall(respEncoder(val, BSTR))
        elif command == "LLEN":
            list_key = array[1]
            dq = datastore.get(list_key, deque([]))
            connection.sendall(respEncoder(len(dq), INTR))
        elif command == "RPUSH":
            # what to do with list_key (RPUSH list_key "foo")
            list_key = array[1]
            dq = datastore.get(list_key, deque([]))
            dq.extend(array[2:])
            datastore[list_key] = dq
            connection.sendall(respEncoder(len(dq), INTR))
        elif command == "LPUSH":
            # what to do with list_key (RPUSH list_key "foo")
            list_key = array[1]
            dq = datastore.get(list_key, deque([]))
            dq.extendleft(array[2:])
            datastore[list_key] = dq
            connection.sendall(respEncoder(len(dq), INTR))
        elif command == "LRANGE":
            start, end = int(array[2]), int(array[3])
            list_key = array[1]
            dq = datastore.get(list_key, deque([]))
            if start < 0:
                start = max(len(dq) + start, 0)
            if end < 0:
                end = max(len(dq) + end, 0)
            # print(start, end)
            connection.sendall(respEncoder(slice_dq(dq, start, end + 1), BARR))
        elif command == "LPOP":
            list_key = array[1]
            dq = datastore.get(list_key, deque([]))
            val = ""
            if dq:
                val = dq.popleft()
            connection.sendall(respEncoder(val, 2), BSTR)

    connection.close()


def deletekey(keystore, key):
    del keystore[key]


def respEncoder(item, type):
    if type == 1:  # simple strings
        return f"+{item}\r\n".encode()
    elif type == 2:  # bulk strings
        if item is None:
            return b"$-1\r\n"
        return f"${len(item)}\r\n{item}\r\n".encode()
    elif type == 3:  # bulk array
        if item is None or len(item) == 0:
            return b"*0\r\n"
        res = f"*{len(item)}\r\n".encode()
        for each in item:
            res += respEncoder(each, 2)
        return res
    elif type == 4:  # Integer
        # :[<+|->]<value>\r\n
        return f":{item}\r\n".encode()
    return "$-1\r\n".encode()


def respParse(bytes):
    return parser(bytes.split(b"\r\n"))


def parser(tlist):
    if tlist[0][0] == ord("+"):
        return tlist[0][1:].decode()
    elif tlist[0][0] == ord("$"):
        return tlist[1].decode()
    elif tlist[0][0] == ord("*"):
        i = int(tlist[0][1:].decode())
        res = []
        c = 1
        for _ in range(i):
            res.append(parser(tlist[c : c + 2]))
            c += 2
        return res
    return []


def slice_dq(d, start, stop):
    d.rotate(-start)
    slice = list(islice(d, 0, stop - start))
    d.rotate(start)
    return slice


if __name__ == "__main__":
    main()
