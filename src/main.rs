use log::{error, info, trace, warn};
use std::{collections::HashMap, sync::{Arc, Mutex, mpsc::{self, Receiver, Sender}}, thread, time::{Duration, Instant}};
use tokio::time;
use procfs::{ProcError, process::{MemoryMap, MemoryMapData, Process, Stat}};

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

    
    let mut children = Vec::new();
    let mut interval = time::interval(time::Duration::from_millis(2000));
    for _ in (0..5) {
        let tx_clone = tx.clone();
        let new_child = tokio::spawn( async move {
            let mut child = tokio::process::Command::new("target/debug/sleeper")
                .arg("100")
                .arg("1")
                .arg("10")
                .spawn()
                .unwrap();
            if let Some(pid) = child.id() {
                tx_clone.send(MonitorOp::AddPid(pid)).unwrap();
                let _ = child.wait().await;
                tx_clone.send(MonitorOp::RemovePid(pid)).unwrap();
            };

        });
        children.push(new_child);
        interval.tick().await;
    }

    let printer = tokio::spawn(async move {
        let mut interval = time::interval(time::Duration::from_millis(1000));
        loop {
            interval.tick().await;
            //print_data(&proc_set);
            tx_clone.send(MonitorOp::Measure).unwrap();
        }
    });
    let _ = tokio::select! {
        _ = printer => {
            info!("Printer exited!"); 
        } 
        _ = futures::future::join_all(children) => {
            info!("All children exited!"); 
            
        }
    };
}

#[derive(Debug)]
struct PidData {
    rss: u64,
    shared_clean: Option<u64>,
    shared_dirty: Option<u64>,
    private_clean: Option<u64>,
    private_dirty: Option<u64>,
    proc: Process,
    smaps_stale_time: Duration,
    smaps_last_refresh: Instant,
    smaps: Option<Vec<(MemoryMap, MemoryMapData)>>,

    stat: Option<Stat>,
    stat_stale_time: Duration,
    stat_last_refresh: Instant,
}

impl PidData {
    fn new(pid: u32) -> Result<Self> {
        Ok(PidData { 
            rss: 0,
            shared_clean: None,
            shared_dirty: None,
            private_clean: None,
            private_dirty: None,
            proc: Process::new(pid as i32)?,
            smaps_stale_time: Duration::from_millis(1000),
            smaps_last_refresh: Instant::now(),
            smaps: None,

            stat_stale_time: Duration::from_millis(500),
            stat_last_refresh: Instant::now(),
            stat: None,
        })
    }

    fn get_meminfo(&mut self) -> Result<MemoryUsage> {
        if self.stat_stale() {
            self.refresh_stat()?;
        }

        Ok(MemoryUsage { total_rss: 0 })        
    }

    fn stat_stale(&self) -> bool {
        self.stat.is_none() || Instant::now().duration_since(self.stat_last_refresh) > self.stat_stale_time
    }
    
    fn refresh_stat(&mut self) -> Result<()> {
        if !self.proc.is_alive() {
            Err(XError::Dead)?
        }

        let stat = self.proc.stat()?;
        self.stat_last_refresh = Instant::now();
        self.rss = stat.rss as u64;
        self.stat = Some(stat);
        Ok(())
    }

    fn refresh_smaps(&mut self) -> Result<()> {
        if !self.proc.is_alive() {
            Err(XError::Dead)?
        }

        let smaps = self.proc.smaps()?;
        self.smaps_last_refresh = Instant::now();
        self.smaps = Some(smaps);
        self.shared_clean = Some(self.get_smaps_optional_data_point_sum("Shared_Clean")?);
        self.shared_dirty = Some(self.get_smaps_optional_data_point_sum("Shared_Dirty")?);
        self.private_clean = Some(self.get_smaps_optional_data_point_sum("Private_Clean")?);
        self.private_dirty = Some(self.get_smaps_optional_data_point_sum("Private_Dirty")?);
        Ok(())
    }

    fn smaps_stale(&self) -> bool {
        self.smaps.is_none() || Instant::now().duration_since(self.smaps_last_refresh) > self.smaps_stale_time
    }

    fn get_smaps_optional_data_point_sum(&mut self, key: &str) -> Result<u64> { 
        if self.smaps_stale() {
            self.refresh_smaps()?;
        }

        let sm = self.smaps.as_ref().ok_or(XError::missing("Smaps missing"))?;
        let psses: Vec<&u64> = sm.iter().filter_map(|(_, md)| md.map.get(key)).collect();
        Ok(psses.iter().map(|x| *x).sum())
    }

    fn report_raw(&mut self) -> Result<usize> {
        if self.stat_stale() {
            trace!("Refreshing stat");
            self.refresh_stat()?;
        }
        if self.smaps_stale() {
            trace!("Refreshing smaps");
            self.refresh_smaps()?;
        }
        let rss = self.get_smaps_optional_data_point_sum("Rss")?;
        let pss = self.get_smaps_optional_data_point_sum("Pss")?;
        info!("{}, Stat Rss, {}, smaps rss, {}, smaps pss, {}, privClean, {}, privDirty, {}, sharedClean, {}, sharedDirty, {}", 
              self.proc.pid,
              self.rss * 4096, 
              rss,
              pss, 
              self.private_clean.ok_or(XError::missing("Missing data point"))?, 
              self.private_dirty.ok_or(XError::missing("Missing data point"))?,
              self.shared_clean.ok_or(XError::missing("Missing data point"))?, 
              self.shared_dirty.ok_or(XError::missing("Missing data point"))?);
        Ok(0)
    }
}

#[derive(Debug)]
struct ProcessSet {
    pids: HashMap<u32, PidData>,
}

impl ProcessSet {
    fn new() -> Self {
        ProcessSet { 
            pids: HashMap::new(),
        }
    }

    fn add_pid(&mut self, pid: u32) {
        match PidData::new(pid) {
            Ok(new) => {
                self.pids.insert(pid, new); 
                self.get_memory_usage();
                /*if self.shared_base.is_none() {
                    match self.get_baseline() {
                        Ok(_) => {
                        }
                        Err(e) => {
                            error!("Failed to populate baseline! {:?}", e);
                        }
                    }
                }
                */
            }
            Err(e) => {
                error!("Failed to add new pid! {:?}", e);
            }
        }
    }

    fn remove_pid(&mut self, pid: u32) {
        self.pids.remove(&pid); 
    }

    fn get_baseline(&mut self) -> Result<Option<usize>> {
        let mut to_remove = Vec::new();
        for (pid, data) in self.pids.iter_mut() {
            match data.report_raw() {
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

    fn get_memory_usage(&mut self) -> Result<Vec<MemoryUsage>> {
        let mut to_remove = Vec::new();
        let mut results = Vec::new();
        for (pid, data) in self.pids.iter_mut() {
            data.report_raw()?;
            /*
            match data.get_meminfo() {
                Ok(total) => {
                    results.push(total);
                }
                Err(XError::Dead) => {
                    warn!("Pid {} is not alive and will be removed", pid);
                    to_remove.push(*pid);
                }
                Err(e) => {
                    error!("Error reading page totals: {:?}", e);
                }
            }    
            */
        }
        for pid in to_remove {
            self.remove_pid(pid);
        }
        Ok(results)
    }
}

#[derive(Debug)]
struct MemoryUsage {
    total_rss: u64,
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
                //info!("Running measurement");
                let mut lock = data.lock().unwrap();
                let measurements = lock.get_memory_usage();
                //info!("Measurement: {:?}", measurements);
            }
            Quit => {
                info!("Monitor loop quiting");
                break;
            }
        }
    }
}

