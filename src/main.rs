use TaxiWay::ThreadPool;
use std::{
    collections::VecDeque,
    fs,
    io::{BufRead, BufReader, prelude::*},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

pub enum SubmitError {
    InvalidFormat,
    NotANumber,
    EmptyInput,
}

/// Struct that holds the data of a job.
/// Currently not representative of the final desing.
struct JobData {
    data: usize,
}

impl JobData {
    pub fn create_job_data(&self, data: usize) -> JobData {
        JobData { data: data }
    }
}

/// Struct that represents a Job.
/// A job has an id and its associated data.
struct Job {
    id: usize,
    data: JobData,
}

impl Job {
    pub fn create_job(&self, id: usize, data: JobData) -> Job {
        Job { id: id, data: data }
    }
}

/// Struct that represnts the job queue.
/// The queue holds objects of the Job Struct.
/// Has a mutex lock to ensure that there are no race conditions.
struct JobQueue {
    jobs: Mutex<VecDeque<Job>>,
    next_job_id: AtomicUsize,
}

impl JobQueue {
    pub fn submit_job(&self, tcp_in: &str) -> Result<usize, SubmitError> {
        let input: Vec<&str> = tcp_in.split_whitespace().collect();
        if input.len() > 2 {
            return Err(SubmitError::InvalidFormat);
        }
        if !tcp_in.starts_with("add") {
            return Err(SubmitError::InvalidFormat);
        }

        let data: usize = input[1].parse().map_err(|_| SubmitError::NotANumber)?;
        let job_data = JobData { data: data };

        let id = self.next_job_id.fetch_add(1, Ordering::Relaxed);

        let job = Job {
            id: id,
            data: job_data,
        };

        self.add_job(job);

        Ok(id)
    }

    /// Returns the first job in the queue and its associated data
    pub fn return_job(&self) -> Option<String> {
        self.get_job()
            .map(|job| format!("{} {}", job.id, job.data.data))
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
    });

    let listener = TcpListener::bind("127.0.0.1:8294").unwrap();
    let pool = ThreadPool::new(4);

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let queue_clone = Arc::clone(&queue);
        pool.execute(|| {
            handle_connection(stream, queue_clone);
        });
    }
}

fn handle_connection(mut stream: TcpStream, queue: Arc<JobQueue>) {
    let buf_reader = BufReader::new(&stream);
    let request_line = buf_reader.lines().next().unwrap().unwrap();

    let response = match request_line.as_str() {
        line if line.starts_with("add") => match queue.submit_job(line) {
            Ok(id) => format!("Job submitted with ID: {id}"),
            Err(_) => "Failed to submit job".to_string(),
        },
        line if line.starts_with("get") => match queue.return_job() {
            Some(job_str) => job_str,
            None => "No Job availiable".to_string(),
        },
        _ => "Generic error".to_string(),
    };

    stream.write_all(response.as_bytes()).unwrap();
}
