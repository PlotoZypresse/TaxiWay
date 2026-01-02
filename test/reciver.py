import socket
import struct
import time
import random
import threading
import sys

HOST = "127.0.0.1"
PORT = 8294

WORKER_THREADS = 8
WORK_DELAY = 0.2
DROP_ACK_PROBABILITY = 0.01  # forces requeue


def send(payload, recv_size=4096):
    with socket.create_connection((HOST, PORT)) as s:
        s.sendall(payload)
        return s.recv(recv_size)


def ping():
    return send(b"\x06") == b"\x45"  # 69


def get_job():
    resp = send(b"\x02")

    if not resp or resp[0] != 0:
        return None

    job_id = struct.unpack(">Q", resp[1:9])[0]
    data_len = struct.unpack(">I", resp[9:13])[0]
    data = resp[13 : 13 + data_len]

    return job_id, data


def ack_job(job_id: int) -> bool:
    payload = b"\x03" + struct.pack(">Q", job_id)
    resp = send(payload)
    return bool(resp and resp[0] == 0)


def worker_thread(worker_id: int):
    while True:
        job = get_job()

        if job is None:
            time.sleep(0.1)
            continue

        job_id, data = job
        print(f"[worker {worker_id}] got job {job_id}: {data!r}")

        # simulate processing
        time.sleep(WORK_DELAY)

        # intentionally drop ACKs sometimes
        if random.random() < DROP_ACK_PROBABILITY:
            print(f"[worker {worker_id}] DROPPED ack for job {job_id}")
            continue

        if ack_job(job_id):
            print(f"[worker {worker_id}] acked job {job_id}")


def main():
    print("Receiver starting")

    if not ping():
        print("Server not responding")
        sys.exit(1)

    print("Ping OK, starting worker threads")

    threads = []
    for i in range(WORKER_THREADS):
        t = threading.Thread(target=worker_thread, args=(i,), daemon=True)
        t.start()
        threads.append(t)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nReceiver stopped")


if __name__ == "__main__":
    main()
