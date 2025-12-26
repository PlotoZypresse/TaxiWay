import socket
import struct
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

HOST = "127.0.0.1"
PORT = 8294

# --- API WRAPPERS ---


def send_submit(data_str):
    """Opcode 1: Submit a job"""
    payload = str(data_str).encode("utf-8")
    header = struct.pack(">B I", 1, len(payload))
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            s.sendall(header + payload)
            status = s.recv(1)
            if status and status[0] == 0:
                return struct.unpack(">I", s.recv(4))[0]
    except Exception as e:
        print(f"Submit Error: {e}")
    return None


def send_get():
    """Opcode 2: Retrieve a job (Returns (ID, Data))"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            s.sendall(struct.pack(">B", 2))
            status = s.recv(1)
            if not status or status[0] != 0:
                return None, None

            jid = struct.unpack(">Q", s.recv(8))[0]
            dlen = struct.unpack(">I", s.recv(4))[0]
            data = s.recv(dlen).decode("utf-8")
            return jid, data
    except Exception:
        return None, None


def send_ack(job_id):
    """Opcode 3: Acknowledge/Delete a job"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            s.sendall(struct.pack(">B Q", 3, job_id))
            status = s.recv(1)
            return status and status[0] == 0
    except Exception:
        return False


def get_queue_length():
    """Opcode 5: Get count of ready jobs"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            s.sendall(struct.pack(">B", 5))
            res = s.recv(8)
            return struct.unpack(">Q", res)[0] if res else 0
    except Exception:
        return 0


# --- TEST 1: STRESS TEST ---


def run_stress_test(num_jobs=5000):
    print("\n" + "=" * 40)
    print(f"TEST 1: STRESS TEST ({num_jobs} Jobs)")
    print("=" * 40)

    start_submit = time.time()
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(send_submit, f"stress_{i}") for i in range(num_jobs)]
        for f in as_completed(futures):
            f.result()

    print(f"Submitted {num_jobs} jobs in {time.time() - start_submit:.2f}s")
    print(f"Initial Queue Length: {get_queue_length()}")

    print("Retrieving and ACKing all jobs...")
    start_retrieve = time.time()
    count = 0
    while True:
        jid, _ = send_get()
        if jid is None:
            break
        send_ack(jid)
        count += 1
        if count % 1000 == 0:
            print(f"Progress: {count}/{num_jobs}")

    print(f"Retrieved/ACKed {count} jobs in {time.time() - start_retrieve:.2f}s")
    final_len = get_queue_length()
    print(f"Final Queue Length: {final_len}")
    return final_len == 0


# --- TEST 2: TIMEOUT TEST ---


def run_timeout_test():
    print("\n" + "=" * 40)
    print("TEST 2: TIMEOUT VALIDATION (Wait 35s)")
    print("=" * 40)

    test_data = "TIMEOUT_DATA_X"
    print(f"Submitting unique job: {test_data}")
    send_submit(test_data)

    # Get the job but DO NOT ACK IT
    jid, data = send_get()
    print(f"Leased Job ID {jid}. Queue length is now: {get_queue_length()}")

    print("Simulating worker crash (No ACK)... waiting 60 seconds.")
    for i in range(60, 0, -5):
        print(f"Remaining: {i}s...")
        time.sleep(5)

    # Check if it returned
    new_len = get_queue_length()
    if new_len > 0:
        print(f"SUCCESS: Job reappeared. Queue length: {new_len}")
        r_id, r_data = send_get()
        if r_id == jid:
            print(f"Verified: Re-retrieved same Job ID {r_id}. ACKing now.")
            send_ack(r_id)
        else:
            print("Warning: Retrieved a different job than expected.")
    else:
        print("FAILED: Job did not return to queue after 30s timeout.")


# --- MAIN ---

if __name__ == "__main__":
    # Ensure server is empty
    if get_queue_length() > 0:
        print("Cleaning up existing jobs in queue first...")
        while True:
            jid, _ = send_get()
            if jid is None:
                break
            send_ack(jid)

    # Run Stress Test
    stress_passed = run_stress_test(100000)

    # Run Timeout Test
    if stress_passed:
        run_timeout_test()
    else:
        print("Skipping Timeout test because stress test failed to clear queue.")
