import socket  # noqa: F401
import threading


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment the code below to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while v := server_socket.accept():  # wait for client
        thread = threading.Thread(target=task, args=(v[0],))
        thread.start()


def task(connection):  # listen for connections
    while data := connection.recv(1024):
        connection.sendall(b"+PONG\r\n")
    connection.close()


if __name__ == "__main__":
    main()
