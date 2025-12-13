import socket
import struct
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

HOST = "127.0.0.1"
PORT = 8294


def send_submit(job_data_str):
    # 1. Prepare Payload
    # Convert string (e.g., "10") to bytes
    payload_bytes = str(job_data_str).encode("utf-8")
    payload_len = len(payload_bytes)

    # 2. Pack the Header
    # Format '>B I':
    #   > = Big Endian (Standard Network Byte Order)
    #   B = Unsigned Char (1 byte) -> Opcode
    #   I = Unsigned Int (4 bytes) -> Length
    header = struct.pack(">B I", 1, payload_len)

    message = header + payload_bytes

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.connect((HOST, PORT))
        # print(f"[DEBUG] Sending Submit: {message}")
        client.sendall(message)

        # 3. Receive Status (1 Byte)
        status_byte = client.recv(1)
        if not status_byte:
            print("Error: Server closed connection")
            return None

        status = status_byte[0]  # Convert byte to int

        if status == 0:
            # Success! Receive Job ID (4 bytes)
            # Your handle_connection sends ID as u32 (4 bytes)
            id_bytes = client.recv(4)
            job_id = struct.unpack(">I", id_bytes)[0]
            # print(f"Sent: '{job_data_str}' -> Received ID: {job_id}")
            return job_id
        else:
            print(f"Failed to submit. Error Code: {status}")
            return None


def send_get():
    # 1. Prepare Request (Opcode 2 only)
    message = struct.pack(">B", 2)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.connect((HOST, PORT))
        client.sendall(message)

        # 2. Receive Status (1 Byte)
        status_byte = client.recv(1)
        if not status_byte:
            return None

        status = status_byte[0]

        if status == 0:
            # Success!
            # A. Receive Job ID (8 bytes - Your JobQueue uses u64 for return_job)
            id_bytes = client.recv(8)
            job_id = struct.unpack(">Q", id_bytes)[
                0
            ]  # Q = unsigned long long (8 bytes)

            # B. Receive Data Length (4 bytes)
            len_bytes = client.recv(4)
            data_len = struct.unpack(">I", len_bytes)[0]

            # C. Receive Data
            data_bytes = client.recv(data_len)
            decoded_data = data_bytes.decode("utf-8")

            # print(f"Got Job: ID {job_id}")
            # | Data: {decoded_data}")
            return decoded_data

        elif status == 1:
            print("Server Queue is Empty.")
            return None
        else:
            print(f"Error getting job. Code: {status}")
            return None


def send_queue_length():
    # 1. Prepare Request (Opcode 5 only)
    message = struct.pack(">B", 5)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.connect((HOST, PORT))
        client.sendall(message)

        # 2. Receive 4 bytes length
        len_bytes = client.recv(8)
        if not len_bytes or len(len_bytes) != 8:
            print("Failed to get queue length")
            return None

        queue_length = struct.unpack(">Q", len_bytes)[0]
        print(f"Queue Length: {queue_length}")
        return queue_length


if __name__ == "__main__":
    jobs = [10, 12, 15, 42, 67, 69, 420, 6969]

    print("--- 1. SUBMITTING JOBS ---")
    start_submit = time.time()
    for job in jobs:
        send_submit(job)
    end_submit = time.time()
    print(f"Time to submit {len(jobs)} jobs: {end_submit - start_submit:.2f} seconds")

    print("\n--- 2. CHECK QUEUE LENGTH AFTER SUBMIT ---")
    current_length = send_queue_length()
    expected_length = len(jobs)
    if current_length == expected_length:
        print(f"Queue length is correct: {current_length}")
    else:
        print(
            f"Queue length mismatch! Expected {expected_length}, got {current_length}"
        )

    print("\n--- 3. WAITING ---")
    # time.sleep(20)

    print("\n--- 4. RETRIEVING JOBS ---")
    returnedJobs = []
    start_retrieve = time.time()
    for _ in range(len(jobs) + 1):
        result = send_get()
        if result:
            returnedJobs.append(result)
    end_retrieve = time.time()
    print(
        f"Time to retrieve {len(returnedJobs)} jobs: {end_retrieve - start_retrieve:.2f} seconds"
    )

    print("-" * 20)
    print("Original Inputs:", [str(j) for j in jobs])

    print("\n--- 5. CHECK QUEUE LENGTH AFTER RETRIEVE ---")
    current_length = send_queue_length()
    expected_length = 0
    if current_length == expected_length:
        print(f"Queue is now empty as expected: {current_length}")
    else:
        print(
            f"Queue length mismatch after retrieval! Expected {expected_length}, got {current_length}"
        )

    # --- STRESS TEST ---
    NUM_JOBS = 100000
    MAX_WORKERS = 100
    PAYLOAD_SIZE = 1024
    stress_jobs = [str(i) * PAYLOAD_SIZE for i in range(NUM_JOBS)]

    print(f"\n--- SUBMITTING {NUM_JOBS} JOBS CONCURRENTLY ---")
    start_stress_submit = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(send_submit, job) for job in stress_jobs]
        submitted_ids = []
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                submitted_ids.append(result)
    end_stress_submit = time.time()
    print(
        f"Time to submit {len(submitted_ids)} jobs: {end_stress_submit - start_stress_submit:.2f} seconds"
    )

    current_length = send_queue_length()
    print(f"Queue Length After Stress Submit: {current_length}")

    print("\n--- RETRIEVING ALL JOBS ---")
    retrieved_jobs = []
    start_stress_retrieve = time.time()
    while True:
        job = send_get()
        if job is None:
            break
        retrieved_jobs.append(job)
    end_stress_retrieve = time.time()
    print(
        f"Time to retrieve {len(retrieved_jobs)} jobs: {end_stress_retrieve - start_stress_retrieve:.2f} seconds"
    )

    current_length = send_queue_length()
    print(f"Queue Length After Stress Retrieval: {current_length}")
