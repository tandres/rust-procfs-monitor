use log::{error, info, warn};
use std::{collections::HashMap, sync::{Arc, Mutex, mpsc::{self, Receiver, Sender}}, thread};
use tokio::time;
use procfs::{process::Process, ProcError};

type Result<T> = std::result::Result<T, XError>;

#[derive(Debug)]
enum XError {
    ProcfsError(String),
    Missing(String),
    Dead,
}

impl From<ProcError> for XError {
    fn from(perr: ProcError) -> XError {
        XError::ProcfsError(format!("ProcfsError: {}", perr))
    }
}

impl XError {
    pub fn missing<S: AsRef<str>>(msg: S) -> XError {
        XError::Missing(msg.as_ref().to_string())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    pretty_env_logger::init();

    let proc_set = Arc::new(Mutex::new(ProcessSet::new()));
    let proc_set_clone = proc_set.clone();
    let (tx, rx) = mpsc::channel();
    let tx_clone = tx.clone();
    thread::spawn(move || {
        monitor(rx, proc_set_clone);
    }); 

    let printer = tokio::spawn(async move {
        let mut interval = time::interval(time::Duration::from_millis(1000));
        for _ in 0..10 {
            interval.tick().await;
            //print_data(&proc_set);
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

fn print_data(data: &Arc<Mutex<ProcessSet>>) {
    let ps_clone: ProcessSet = data.lock().unwrap().clone();
    info!("{:#?}", ps_clone);
}

#[derive(Debug, Clone)]
struct PidData {
    rss: u64,
    proc: Process,
}

impl PidData {
    fn new(pid: u32) -> Result<Self> {
        Ok(PidData { 
            rss: 0,
            proc: Process::new(pid as i32)?,
        })
    }

    fn get_shared_page_total(&mut self) -> Result<usize> {
        if !self.proc.is_alive() {
            Err(XError::Dead)?
        }

        let smaps = self.proc.smaps()?;
        let mut total = 0;
        let mut total_cnt = 0; 
        
        let mut shared_dirty = 0;
        let mut shared_dirty_cnt = 0;
        let mut shared_clean = 0;
        let mut shared_clean_cnt = 0;
        let mut private_dirty = 0;
        let mut private_dirty_cnt = 0;
        let mut private_clean = 0;
        let mut private_clean_cnt = 0;
        let mut total_rss = 0;
        for (map, data) in smaps {
            let mut page_type = String::new();
            let size = map.address.1 - map.address.0;
            total_cnt += 1;
            total += size;
            if let Some(rss) = data.map.get("Rss") {
                total_rss += rss;
            }

            if let Some(pd) = data.map.get("Private_Dirty") {
                private_dirty_cnt += 1;
                private_dirty += pd;
                if *pd > 0 {
                    page_type.push_str("|PD|");
                }
            }

            if let Some(pc) = data.map.get("Private_Clean") {
                private_clean_cnt += 1;
                private_clean += pc;
                if *pc > 0 {
                    page_type.push_str("|PC|");
                }
            }

            if let Some(sc) = data.map.get("Shared_Clean") {
                shared_clean_cnt += 1;
                shared_clean += sc;
                if *sc > 0 {
                    page_type.push_str("|SC|");
                }
            }

            if let Some(sd) = data.map.get("Shared_Dirty") {
                shared_dirty_cnt += 1;
                shared_dirty += sd;
                if *sd > 0 {
                    page_type.push_str("|SD|");
                }
            }
            info!("Page: {:?}\t\t\tsize: {} type: {} flags: {:?}", map.pathname, size, page_type, data.vm_flags);
        
            //info!("{:#?}", data);
        }

        let stat = self.proc.stat()?;
        info!("Stat Rss: {} smaps rss: {}", stat.rss, total_rss);
        info!("PC: {} ({})", private_clean, private_clean_cnt);
        info!("PD: {} ({})", private_dirty, private_dirty_cnt);
        info!("SC: {} ({})", shared_clean, shared_clean_cnt);
        info!("SD: {} ({})", shared_dirty, shared_dirty_cnt);
        /*let baseline: u32 = smaps
                            .into_iter()
                            .map(|x|  {
                                info!("{:#?}", x); 
                                 x.1
                            }) //extract MemoryMapData
                            .filter(|m| m.vm_flags.is_some())
                            .map(|f| {
                                0
                            })
                            .sum(); */
        Ok(0)
    }
}

#[derive(Debug, Clone)]
struct ProcessSet {
    shared_base: Option<usize>,
    pids: HashMap<u32, PidData>,
}

impl ProcessSet {
    fn new() -> Self {
        ProcessSet { 
            shared_base: None,
            pids: HashMap::new(),
        }
    }

    fn add_pid(&mut self, pid: u32) {
        match PidData::new(pid) {
            Ok(new) => {
                self.pids.insert(pid, new); 
                if self.shared_base.is_none() {
                    match self.get_baseline() {
                        Ok(b) => {
                            self.shared_base = b;
                        }
                        Err(e) => {
                            error!("Failed to populate baseline! {:?}", e);
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to add new pid! {:?}", e);
            }
        }
    }

    fn remove_pid(&mut self, pid: u32) {
        self.pids.remove(&pid);
        if self.pids.is_empty() {
            self.shared_base = None;
        }
    }

    fn get_baseline(&mut self) -> Result<Option<usize>> {
        let mut to_remove = Vec::new();
        for (pid, data) in self.pids.iter_mut() {
            match data.get_shared_page_total() {
                Ok(total) => {
                    return Ok(Some(total));
                }
                Err(XError::Dead) => {
                    warn!("Pid {} is not alive and will be removed", pid);
                    to_remove.push(*pid);
                }
                Err(e) => {
                    error!("Error reading page totals: {:?}", e);
                }
            }    
        }
        for pid in to_remove {
            self.remove_pid(pid);
        }
        Ok(None)
    }
}


enum MonitorOp {
    AddPid(u32),
    RemovePid(u32),
    Measure,
    Quit,
}

fn monitor(rx: Receiver<MonitorOp>, data: Arc<Mutex<ProcessSet>>) {
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

