import socket


def sendRequest(message):
    host = "127.0.0.1"
    port = 8294

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.connect((host, port))

        print(f"Sending: {message}")
        client.sendall((message + "\n").encode("utf-8"))

        data = client.recv(1024)
        print(f"Received: {data.decode('utf-8')}")
        print("-" * 20)


if __name__ == "__main__":
    sendRequest("add 420")
    sendRequest("get")
