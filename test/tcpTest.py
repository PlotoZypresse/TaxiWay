import socket
import time


def sendRequest(message):
    host = "127.0.0.1"
    port = 8294

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.connect((host, port))

        print(f"Sending: {message}")
        client.sendall((message + "\n").encode("utf-8"))

        data = client.recv(1024)
        decoded = data.decode("utf-8").strip()
        print(f"Received: {decoded}")
        print("-" * 20)

        return decoded


if __name__ == "__main__":
    jobs = [10, 12, 15, 42, 67, 69, 420, 6969]

    for job in jobs:
        sendRequest(f"add {job}")

    time.sleep(10)

    returnedJobs = []

    for _ in jobs:
        result = sendRequest("get")
        returnedJobs.append(result)

    print(jobs)
    print("-" * 20)
    print(returnedJobs)
