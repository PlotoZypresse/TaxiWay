use TaxiWay::ThreadPool;
use std::{
    collections::{BinaryHeap, HashMap, VecDeque},
    io::{BufRead, BufReader, prelude::*},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

pub enum SubmitError {
    InvalidFormat,
    NotANumber,
    EmptyInput,
    NoOpcode,
}

pub enum HashMapError {
    NoJob,
}

/// Struct that holds the data of a job.
/// Currently not representative of the final desing.
#[derive(Clone, PartialEq, Eq)]
struct JobData {
    data: Vec<u8>,
}

/// Struct that represents a Job.
/// A job has an id and its associated data.
#[derive(Clone, PartialEq, Eq)]
struct Job {
    id: usize,
    data: JobData,
    sent: Option<Instant>,
}

/// Struct that represnts the job queue.
/// The queue holds objects of the Job Struct.
/// Has a mutex lock to ensure that there are no race conditions.
struct JobQueue {
    jobs: Mutex<VecDeque<Job>>,
    next_job_id: AtomicUsize,
    pending_jobs: Mutex<HashMap<usize, Job>>,
    unacked_jobs: Mutex<BinaryHeap<Job>>,
}

impl PartialOrd for Job {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Job {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let time1 = self.sent;
        let time2 = other.sent;

        time2.cmp(&time1).then_with(|| self.id.cmp(&other.id))
    }
}

impl JobQueue {
    /// Function to add a job to the back of the queue. Checks the opcode for correctness.
    /// Checks the following 4 bytes for the length of the data.
    /// Saves the data and a jobid in the queue.
    pub fn submit_job(&self, tcp_in: Vec<u8>) -> Result<usize, SubmitError> {
        // read the opcode
        let _opcode: u8 = match tcp_in.get(0) {
            Some(byte) => *byte,
            None => return Err(SubmitError::InvalidFormat),
        };

        // get the length of the data
        let data_len: &[u8] = &tcp_in[1..5];
        let int_data_len = u32::from_be_bytes(data_len.try_into().unwrap());
        if int_data_len == 0 {
            return Err(SubmitError::EmptyInput);
        }

        //read data and put it into the queue
        let data_end = 5 + (int_data_len as usize);
        if tcp_in.len() < data_end {
            return Err(SubmitError::InvalidFormat);
        }
        let data = &tcp_in[5..data_end];
        // make data owned and a Vec<u8> to move it into the struct
        let data = data.to_vec();
        let job_data = JobData { data: data };

        let id = self.next_job_id.fetch_add(1, Ordering::Relaxed);
        let job = Job {
            id: id,
            data: job_data,
            sent: None,
        };

        self.add_job(job);

        Ok(id)
    }

    /// Returns the first job in the queue and its associated data
    pub fn return_job(&self) -> Vec<u8> {
        let mut job = match self.get_job() {
            Some(job) => job,
            None => return Vec::new(),
        };

        //set the send time of the job before adding it to the HashMap
        job.sent = Some(Instant::now());

        // Adding the job to the pending_jobs HashMap
        self.pending_jobs
            .lock()
            .unwrap()
            .insert(job.id, job.clone());

        // add job to the BinaryHeap
        self.unacked_jobs.lock().unwrap().push(job.clone());

        let id = (job.id as u64).to_be_bytes();
        let data_len = job.data.data.len() as u32;
        let data_len_bytes = data_len.to_be_bytes();

        let vec_cap = 8 + 4 + data_len as usize;
        let mut result = Vec::with_capacity(vec_cap);

        //add the 8 id bytes to the start of the result vec
        result.extend_from_slice(&id);
        // add the 4 length bytes
        result.extend_from_slice(&data_len_bytes);
        // add the data
        result.extend_from_slice(&job.data.data);

        // retun it
        result
    }

    ///Function to remove an acked job from the pending queue.
    pub fn removed_acked_job(&self, tcp_in: Vec<u8>) -> Result<(), HashMapError> {
        let job_id: &[u8] = &tcp_in[1..9];
        let id = usize::from_be_bytes(job_id.try_into().unwrap());
        match self.pending_jobs.lock().unwrap().remove(&id) {
            Some(_) => Ok(()),
            None => Err(HashMapError::NoJob),
        }
    }

    pub fn get_queue_length(&self) -> usize {
        self.jobs.lock().unwrap().len()
    }

    /// Function to add job to the job queue
    /// always adds to back of the queue.
    fn add_job(&self, job: Job) {
        self.jobs.lock().unwrap().push_back(job);
    }

    /// Function to get job from the job queue
    /// always gets the job at the start of the queue
    fn get_job(&self) -> Option<Job> {
        self.jobs.lock().unwrap().pop_front()
    }
}

fn main() {
    let queue = Arc::new(JobQueue {
        jobs: Mutex::new(VecDeque::new()),
        next_job_id: AtomicUsize::new(0),
        pending_jobs: Mutex::new(HashMap::new()),
        unacked_jobs: Mutex::new(BinaryHeap::new()),
    });

    let listener = TcpListener::bind("127.0.0.1:8294").unwrap();
    let pool = ThreadPool::new(32);

    let requeue_queue = Arc::clone(&queue);
    thread::spawn(move || {
        loop {
            requeue(&requeue_queue);
            thread::sleep(Duration::from_secs(30));
        }
    });

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let queue_clone = Arc::clone(&queue);
        pool.execute(|| {
            handle_connection(stream, queue_clone);
        });
    }
}

/// Function that checks the root of the HashMap to see if the item has been in the
/// Unacked hashmap more than the desired time. If that is the case its moved back into the queue.
fn requeue(job_queue: &JobQueue) {
    let mut tree = job_queue.unacked_jobs.lock().unwrap();

    while let Some(root) = tree.peek() {
        if root.sent.unwrap().elapsed() < Duration::from_secs(30) {
            break;
        }

        let root = match tree.pop() {
            Some(root) => root,
            None => return,
        };
        let mut pending_jobs = job_queue.pending_jobs.lock().unwrap();

        if pending_jobs.remove(&root.id).is_some() {
            //let mut jobs = job_queue.jobs.lock().unwrap();
            let mut requeue_job = root;
            requeue_job.sent = None;

            println!("Job {}, Was requeud due to Ack time out", requeue_job.id);
            job_queue.add_job(requeue_job);
        }
    }
}

/// Gets called on an incoming TCP stream. Checks the first byte for the opcode.
/// Depending on the opcode runs the appropriate code.
/// Opcodes:
///     [1] => Adds the send data to the queue and sends back the job id.
///     [2] => Retirves the Job in the front of the queue returns it and the id.
///     [3] => ACKnowledgement by the sender that the job with the provided id is recived and
///     can be deleted from the queue.
///     [4] => TODO NACK changes the state of a job from in progess(meaning it was sent by get[2]
///     but not yet ACK'ed) back to ready.
///     [5] => Returns only the number of items in the queue no return code.
///     [6] => Pings the server to check if its running, returns Return Code [69]
/// Return Codes:
///     [0] => Indicates success. If data is send back its appended after the return code.
///     [1] => Indicates an error
///     [2] => indicates an invalid opcode
///     [3] => indicates an empty request
///     [69] => indicates that the server is running and ready
fn handle_connection(mut stream: TcpStream, queue: Arc<JobQueue>) {
    let mut buf_reader = BufReader::new(&stream);
    let data = match buf_reader.fill_buf() {
        Ok(bytes) => bytes,
        Err(e) => {
            eprintln!("Error reading from stream: {}", e);
            return;
        }
    };
    let opcode = data.get(0).copied();

    let response = match opcode {
        Some(1) => {
            let mut data_vec = Vec::new();
            data_vec.extend_from_slice(data);

            match queue.submit_job(data_vec) {
                Ok(id) => {
                    let mut result = vec![0];
                    result.extend_from_slice(&(id as u32).to_be_bytes());
                    result
                }
                Err(_) => vec![1], // error
            }
        }
        Some(2) => {
            let job_bytes = queue.return_job();

            if job_bytes.is_empty() {
                vec![1] // error
            } else {
                let mut result = vec![0];
                result.extend_from_slice(&job_bytes);
                result
            }
        }
        Some(3) => {
            let mut data_vec = Vec::new();
            data_vec.extend_from_slice(data);

            match queue.removed_acked_job(data_vec) {
                Ok(()) => vec![0],
                Err(_) => vec![1],
            }
        }
        //Some(4) => {}
        Some(5) => {
            let queue_length = queue.get_queue_length().to_be_bytes();
            let mut result = Vec::new();
            result.extend_from_slice(&queue_length);
            result
        }
        Some(6) => vec![69],
        Some(_) => vec![2], // invalid opcode
        None => vec![3],    // emty request
    };

    if let Err(e) = stream.write_all(&response) {
        eprintln!("Failed to send response: {}", e);
    }
}
