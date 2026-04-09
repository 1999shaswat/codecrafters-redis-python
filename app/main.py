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
    while data := connection.recv(1024):
        array = respParse(data)
        if array[0] == "PING":
            connection.sendall(respEncoder("PONG", 1))
        elif array[0] == "ECHO":
            connection.sendall(respEncoder(array[1], 2))
    connection.close()


def respEncoder(item, type):
    if type == 1:  # simple strings
        return f"+{item}\r\n".encode()
    elif type == 2:  # bulk strings
        return f"${len(item)}\r\n{item}\r\n".encode()
    elif type == 3:  # bulk array
        res = f"*{len(item)}\r\n".encode()
        for each in item:
            res += respEncoder(each, 2)
        return res
    return b""


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
