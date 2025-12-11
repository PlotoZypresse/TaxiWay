import socket
import struct
import time

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
            print(f"Sent: '{job_data_str}' -> Received ID: {job_id}")
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

            print(f"Got Job: ID {job_id} | Data: {decoded_data}")
            return decoded_data

        elif status == 1:
            print("Server Queue is Empty.")
            return None
        else:
            print(f"Error getting job. Code: {status}")
            return None


if __name__ == "__main__":
    jobs = [10, 12, 15, 42, 67, 69, 420, 6969]

    print("--- 1. SUBMITTING JOBS ---")
    for job in jobs:
        send_submit(job)

    print("\n--- 2. WAITING ---")
    time.sleep(120)

    print("\n--- 3. RETRIEVING JOBS ---")
    returnedJobs = []

    # We loop one extra time to test the "Empty Queue" message
    for _ in range(len(jobs) + 1):
        result = send_get()
        if result:
            returnedJobs.append(result)

    print("-" * 20)
    print("Original Inputs:", [str(j) for j in jobs])
    print("Returned Data:  ", returnedJobs)
