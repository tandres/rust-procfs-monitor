use log::{debug, error, info, trace, warn};
use std::{collections::HashMap, path::PathBuf, sync::{Arc, Mutex, mpsc::{self, Receiver}}, thread, time::{Duration, Instant}};
use tokio::time;
use procfs::{ProcError, process::{MemoryMap, MemoryMapData, Process}};

type Result<T> = std::result::Result<T, XError>;

#[derive(Debug)]
enum XError {
    ProcfsError(String),
    Missing(String),
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

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Arguments required! <executable> [args..]");
        return;
    }
    let target = PathBuf::from(args.get(1).unwrap());
    if !target.exists() || !target.is_file() {
        println!("Argument is not a file!");
        return;
    }

    let target_args = args[2..].to_vec();

    let proc_set = Arc::new(Mutex::new(ProcessSet::new(60_000, 1_000)));
    let proc_set_clone = proc_set.clone();
    let (tx, rx) = mpsc::channel();
    let tx_clone = tx.clone();
    thread::spawn(move || {
        monitor(rx, proc_set_clone);
    }); 

    
    let mut children = Vec::new();
    for _ in 0..5 {
        let tx_clone = tx.clone();
        let target_clone = target.clone();
        let args_clone = target_args.clone();
        let new_child = tokio::spawn( async move {
            let mut child = tokio::process::Command::new(target_clone)
                .args(args_clone)
                //.arg("1000")
                //.arg("1")
                //.arg("10")
                .spawn()
                .unwrap();
            if let Some(pid) = child.id() {
                tx_clone.send(MonitorOp::AddPid(pid)).unwrap();
                let _ = child.wait().await;
                tx_clone.send(MonitorOp::RemovePid(pid)).unwrap();
            };

        });
        children.push(new_child);
    }

    let printer = tokio::spawn(async move {
        let mut interval = time::interval(time::Duration::from_millis(5000));
        loop {
            interval.tick().await;
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
struct ProcessSet {
    page_size: u64,
    baseline_source: Option<u32>,
    baseline_rss: Option<u64>,
    baseline_shared_clean: u64,
    baseline_last_update: Option<Instant>,
    baseline_ttl: Duration,
    stat_update_ttl: Duration,
    stat_last_update: Option<Instant>,
    total_rss_est: u64,
    per_pid_rss: Vec<(u32, u64)>,
    pids: HashMap<u32, Process>,
}

impl ProcessSet {
    fn new(baseline_ttl_ms: u64, stat_ttl_ms: u64) -> Self {
        let page_size = procfs::page_size().unwrap_or(4096) as u64;
        ProcessSet { 
            page_size,
            baseline_source: None,
            baseline_rss: None,
            baseline_shared_clean: 0,
            baseline_last_update: None,
            baseline_ttl: Duration::from_millis(baseline_ttl_ms),
            stat_update_ttl: Duration::from_millis(stat_ttl_ms),
            stat_last_update: None,
            total_rss_est: 0,
            per_pid_rss: Vec::new(),
            pids: HashMap::new(),
        }
    }

    fn reset(&mut self) {
        self.baseline_source = None;
        self.baseline_rss = None;
        self.baseline_shared_clean = 0;
        self.baseline_last_update = None;
        self.stat_last_update = None;
        self.total_rss_est = 0;
        self.per_pid_rss = Vec::new();
        self.pids = HashMap::new();
    }

    fn refresh(&mut self) {
        self.remove_dead_pids();
        let mut now = Instant::now();
        // Check if our data needs to be updated
        let refresh_baseline = match self.baseline_last_update {
            Some(time) if now.duration_since(time) < self.baseline_ttl => false,
            _ => true,
        };
        let refresh_stat = match self.stat_last_update {
            Some(time) if now.duration_since(time) < self.stat_update_ttl => false,
            _ => true,
        };

        if refresh_baseline {
            self.update_baseline();
            let new_now = Instant::now();
            trace!("Baseline refresh took: {} ms", new_now.duration_since(now).as_millis());
            self.baseline_last_update = Some(new_now);
            // Reset now for any stat update
            now = new_now;
        }

        if refresh_stat {
            self.update_stat();
            let new_now = Instant::now();
            trace!("Stat refresh took: {} ms", new_now.duration_since(now).as_millis());
            self.stat_last_update = Some(new_now);
        }
    }

    fn update_baseline(&mut self) {
        let mut keys: Vec<u32> = self.pids.keys().map(|x| *x).collect();
        // We prefer to get our baseline from teh previous baseline source but
        // have to search through every process until we get a baseline
        if let Some(source_key) = self.baseline_source {
            if let Some(location) = keys.iter().position(|x| *x == source_key) {
                keys.swap(0, location);
            }
        }
        for key in keys {
            // At any time a process could die, which means that any calls to our
            // remove_dead_pids will have a race condition. The only way to be sure
            // that we've gotten a baseline is to try to read smaps
            let proc = if let Some(proc) = self.pids.get(&key) {
                proc
            } else {
                // We pulled this key from the hashmap and it should still be in there?
                warn!("Missing proc that should be there");
                continue;
            };
            let regions = match proc.smaps() {
                Ok(smaps) => smaps,
                Err(e) => {
                    error!("Failed to read proc smaps: {}", e);
                    continue;
                }
            };
            match Self::smaps_sum_region_data(&regions, "Shared_Clean") {
                Ok(shared_clean) => {
                    self.baseline_shared_clean = shared_clean;
                    self.baseline_source = Some(key);
                }
                Err(e) => {
                    error!("Failed to read shared_clean: {:?}", e);
                }
            }

            match Self::smaps_sum_region_data(&regions, "Rss") {
                Ok(rss) => {
                    self.baseline_rss = Some(rss);
                }
                Err(e) => {
                    error!("Failed to read Rss for baseline: {:?}", e);
                }
            }

            if self.baseline_source.is_some() {
                break;
            }
        }
        if self.baseline_source.is_none() {
            warn!("Failed to find baseline source");
        }
    }


    fn smaps_sum_region_data(smaps: &Vec<(MemoryMap, MemoryMapData)>, key: &str) -> Result<u64> {
        let points: Vec<&u64> = smaps.iter().filter_map(|(_, md)| md.map.get(key)).collect();
        if points.is_empty() {
            Err(XError::missing(format!("No data points for {} in smaps", key)))
        } else {
            Ok(points.iter().map(|x| *x).sum())
        }
    }

    fn update_stat(&mut self) {
        let mut new_rss = Vec::new();
        for (key, proc) in self.pids.iter() {
            match proc.stat() {
                Ok(stat) => {
                    // Stat rss is delivered in number of pages, everything else is in bytes
                    let stat_rss = (stat.rss as u64).saturating_mul(self.page_size);
                    new_rss.push((*key, stat_rss));
                    if Some(key) == self.baseline_source.as_ref() {
                        if let Some(baseline_rss) = self.baseline_rss {
                            let diff = stat_rss as i128 - baseline_rss as i128;
                            debug!("Difference between baseline and stat rss: {} ({:3.2}%)", diff.abs(), 100.0 * diff.abs() as f32 / baseline_rss as f32);
                        }
                    }
                },
                Err(e) => {
                    warn!("Failed to read proc from stat list: {:?}", e);
                }
            }
        }
        self.per_pid_rss = new_rss;
    }

    fn remove_dead_pids(&mut self) {
        let to_remove: Vec<u32> = self.pids.iter().filter_map(|x| {
            if !x.1.is_alive() {
                trace!("Removing dead pid {}", x.0);
                Some(*x.0)
            } else {
                None
            }
        }).collect();
        for pid in to_remove {
            self.remove_pid(pid)
        }
    }

    fn add_pid(&mut self, pid: u32) -> Result<u32> {
        let proc = Process::new(pid as i32)?;
        self.pids.insert(pid, proc);
        // We don't want to set up a baseline quite yet. Observations indicate that the majority of
        // processes settle a little bit and if we establish a baseline too early it can throw off
        // our readings. We will perform a refresh on the first request of readings with data
        // eventually reaching something matching reality.
        Ok(pid) 
    }

    fn remove_pid(&mut self, pid: u32) {
        self.pids.remove(&pid); 
        if self.pids.is_empty() {
            self.reset();
        }
    }

    fn get_memory_usage(&mut self) -> Result<MemoryUsage> {
        // This will refresh our data if it is needed. All data from the refresh is stored within
        // us once that call completes.
        self.refresh(); 
        let (total_rss, rss_data) = match (self.baseline_source, self.baseline_shared_clean, &self.per_pid_rss) {
            (Some(_), shared_clean, rss_list) if !rss_list.is_empty() => {
                let adjusted_list = rss_list.iter().map(|(pid, rss)| {
                    let adjusted = rss.saturating_sub(shared_clean);
                    if adjusted == 0 {
                        debug!("Rss value ({}) is below shared page ({}), pid {} will have 0 size", rss, shared_clean, pid);
                    }
                    adjusted
                }).collect::<Vec<u64>>();
                (adjusted_list.into_iter().sum::<u64>() + shared_clean, rss_list.iter().map(|(_, data)| *data).collect())
            }
            (Some(_), _, _) => {
                warn!("Baseline populated but rss is empty!");
                (0, Vec::new())
            }
            (None, _, rss_list) if rss_list.is_empty() => {
                (0, Vec::new())
            }
            (None, _, rss_list) => {
                warn!("Baseline empty, total rss value will not be accurate!");
                (rss_list.iter().map(|(_, rss)| rss).sum(), rss_list.iter().map(|(_, data)| *data).collect())
            }
        };
        let result = MemoryUsage { 
            total_rss,
            rss_data,
        };
        Ok(result)
    }

    fn get_actual_usage(&mut self) -> Result<MemoryUsage> {
        let start = Instant::now();
        let proc_data: Vec<(u64, u64)> = self.pids.iter().filter_map(|(_key, proc)| {
            proc.smaps().ok()
        })
        .map(|smaps| {
            let rss: u64 = smaps.iter().filter_map(|(_, mapdata)| mapdata.map.get("Rss")).sum();
            let shared_clean: u64 = smaps.iter().filter_map(|(_, mapdata)| mapdata.map.get("Shared_Clean")).sum(); 
            (rss, shared_clean)
        }).collect();
        let len = proc_data.len() as u64;
        let (total_rss, rss_data) = if len != 0 {
            let adjusted : u64 = proc_data.iter().map(|(rss, shared)| rss.saturating_sub(*shared)).sum();
            let shared_clean : u64 = proc_data.iter().map(|(_, shared)| shared).sum(); 
            let average_shared_clean = shared_clean / proc_data.len() as u64;
            (adjusted + average_shared_clean, proc_data.iter().map(|(rss, _)| *rss).collect())
        } else {
            (0, Vec::new())
        };
        trace!("Reading actual usage took {} ms", Instant::now().duration_since(start).as_millis());
        Ok(MemoryUsage {
            total_rss,
            rss_data,
        })
    }
}

#[derive(Debug)]
struct MemoryUsage {
    total_rss: u64,
    rss_data: Vec<u64>,
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
                if let Err(e) = lock.add_pid(pid) {
                    error!("Add pid failed: {:?}", e);
                }
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
                let actual = lock.get_actual_usage();
                info!("Measurement: {:?}", measurements);
                info!("Actual: {:?}", actual);
            }
            Quit => {
                info!("Monitor loop quiting");
                break;
            }
        }
    }
}

