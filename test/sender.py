import socket
import struct
import time
import threading
import itertools
import sys

HOST = "127.0.0.1"
PORT = 8294
SENDER_THREADS = 8
SEND_DELAY = 0.1


def send(payload, recv_size=1024):
    with socket.create_connection((HOST, PORT)) as s:
        s.sendall(payload)
        return s.recv(recv_size)


def ping():
    return send(b"\x06") == b"\x45"  # 69


def submit_job(data: bytes) -> int | None:
    payload = b"\x01" + struct.pack(">I", len(data)) + data
    resp = send(payload)

    if not resp or resp[0] != 0:
        return None

    return struct.unpack(">I", resp[1:5])[0]


def sender_thread(thread_id: int):
    counter = itertools.count()

    for i in counter:
        payload = f"sender-{thread_id}-job-{i}".encode()
        job_id = submit_job(payload)

        if job_id is not None:
            print(f"[sender {thread_id}] submitted job {job_id}")

        time.sleep(SEND_DELAY)


def main():
    print("Sender starting")

    if not ping():
        print("Server not responding")
        sys.exit(1)

    print("Ping OK, starting sender threads")

    threads = []
    for i in range(SENDER_THREADS):
        t = threading.Thread(target=sender_thread, args=(i,), daemon=True)
        t.start()
        threads.append(t)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nSender stopped")


if __name__ == "__main__":
    main()
