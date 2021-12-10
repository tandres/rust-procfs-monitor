use log::{error, info};
use std::{collections::HashMap, sync::{Arc, Mutex, mpsc::{self, Receiver, Sender}}, thread};
use tokio::time;
use procfs::process::Process;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    pretty_env_logger::init();

    let data = Arc::new(Mutex::new(Data::new()));
    let data_clone = data.clone();
    let (tx, rx) = mpsc::channel();
    let tx_clone = tx.clone();
    thread::spawn(move || {
        monitor(rx, data_clone);
    }); 

    let printer = tokio::spawn(async move {
        let mut interval = time::interval(time::Duration::from_millis(1000));
        for _ in 0..10 {
            interval.tick().await;
            print_data(&data);
            tx_clone.send(MonitorOp::Measure).unwrap();
        }
    });
    
    let children = (0..5).map(|_| {
        let tx_clone = tx.clone();
        tokio::spawn( async move {
            let mut child = tokio::process::Command::new("sleep")
                .arg("10")
                .spawn()
                .unwrap();
            if let Some(pid) = child.id() {
                tx_clone.send(MonitorOp::AddPid(pid)).unwrap();
                let _ = child.wait().await;
                tx_clone.send(MonitorOp::RemovePid(pid)).unwrap();
            };

        })
    });
    let _ = tokio::join!(printer, futures::future::join_all(children));
}

fn print_data(data: &Arc<Mutex<Data>>) {
    let data_clone: Data = data.lock().unwrap().clone();
    info!("{:#?}", data_clone);
}

#[derive(Debug, Clone)]
struct PerPidData {
    rss: u64,
}

impl PerPidData {
    fn new() -> Self {
        PerPidData { rss: 0 }
    }
}

#[derive(Debug, Clone)]
struct Data {
    shared_base: usize,
    pids: HashMap<u32, PerPidData>,
}

impl Data {
    fn new() -> Self {
        Data { 
            shared_base: 0,
            pids: HashMap::new(),
        }
    }

    fn add_pid(&mut self, pid: u32) {
        self.pids.insert(pid, PerPidData::new());
        if self.pids.len() == 1 {
            self.populate_baseline();
        }
    }

    fn remove_pid(&mut self, pid: u32) {
        self.pids.remove(&pid);
        if self.pids.is_empty() {
            self.shared_base = 0;
        }
    }

    fn populate_baseline(&mut self) {
        if self.pids.is_empty() {
            return;
        }
        let example_pid = *self.pids.iter().nth(0).unwrap().0;
        if let Ok(proc) = Process::new(example_pid as i32) {
            if let Ok(smaps) = proc.smaps() {
                let baseline: u32 = smaps
                    .into_iter()
                    .map(|x|  {
                        info!("{:#?}", x); 
                         x.1
                    }) //extract MemoryMapData
                    .filter(|m| m.vm_flags.is_some())
                    .map(|f| {
                        0
                    })
                    .sum();

            } else {
                error!("Failed to read smaps for pid {}", example_pid);
            }
        } else {
            error!("Failed to read process data for pid {}", example_pid);
            self.remove_pid(example_pid);
        }
    }
}


enum MonitorOp {
    AddPid(u32),
    RemovePid(u32),
    Measure,
    Quit,
}

fn monitor(rx: Receiver<MonitorOp>, data: Arc<Mutex<Data>>) {
    loop {
        let msg = rx.recv().unwrap_or(MonitorOp::Quit);
        use MonitorOp::*;
        match msg {
            AddPid(pid) => {
                info!("Adding new pid {} to monitor", pid);
                let mut lock = data.lock().unwrap();
                lock.add_pid(pid);
            },
            RemovePid(pid) => {
                info!("Removing pid {} from monitor", pid);
                let mut lock = data.lock().unwrap();
                lock.remove_pid(pid);
            }
            Measure => {
                info!("Running measurement");
            }
            Quit => {
                info!("Monitor loop quiting");
                break;
            }
        }
    }
}

