import socket  # noqa: F401
import threading


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
    keystore = {}
    while data := connection.recv(1024):
        array = respParse(data)
        command = array[0].upper()
        if command == "PING":
            connection.sendall(respEncoder("PONG", 1))
        elif command == "ECHO":
            connection.sendall(respEncoder(array[1], 2))
        elif command == "SET":
            keystore[array[1]] = array[2]
            if len(array) == 5:
                time = int(array[4])
                if array[3] == "EX":
                    threading.Timer(time, deleteKey, args=(keystore, array[1])).start()
                elif array[3] == "PX":
                    threading.Timer(time, deleteKey, args=(keystore, array[1])).start()
            connection.sendall(respEncoder("OK", 1))
        elif command == "GET":
            val = keystore.get(array[1])
            connection.sendall(respEncoder(val, 2))
    connection.close()


def deleteKey(keystore, key):
    del keystore[key]


def respEncoder(item, type):
    if type == 1:  # simple strings
        return f"+{item}\r\n".encode()
    elif type == 2:  # bulk strings
        if item is None:
            return b"$-1\r\n"
        return f"${len(item)}\r\n{item}\r\n".encode()
    elif type == 3:  # bulk array
        res = f"*{len(item)}\r\n".encode()
        for each in item:
            res += respEncoder(each, 2)
        return res
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


if __name__ == "__main__":
    main()
