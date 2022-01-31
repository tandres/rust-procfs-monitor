use log::{debug, error, info, trace, warn};
use procfs::{
    process::{MemoryMap, MemoryMapData, Process},
    ProcError,
};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        mpsc::{self, Receiver},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};
use tokio::time;

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

    let proc_set = Arc::new(Mutex::new(ProcessSet::new(10_000, 1_000)));
    let proc_set_clone = proc_set.clone();
    let (tx, rx) = mpsc::channel();
    let tx_clone = tx.clone();
    thread::spawn(move || {
        monitor(rx, proc_set_clone);
    });

    let mut children = Vec::new();
    for _ in 0..4 {
        let tx_clone = tx.clone();
        let target_clone = target.clone();
        let args_clone = target_args.clone();
        let new_child = tokio::spawn(async move {
            let mut child = tokio::process::Command::new(target_clone)
                .args(args_clone)
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

// Memory accounting on linux is a little complicated. Any invdividual process has its own
// working private set of writable (private dirty) memory as well as memory regions that
// are shared (shared clean) with other external processes. There are several apis available
// that are of interest for tracking a process's memory consumption. We use smaps and stat
// in this implementation. Smaps (/proc/<pid>/smaps) is accurate but expensive (in terms of
// computation as well as time) as the kernel has to do a significant amount of work to
// satisfy the request. The stat (/proc/<pid>/stat) interface is inexpensive but not very
// accurate.
//
// Our objective here is to provide an interface that gives a pretty good estimate of the
// total memory consumption of a set of processes. The processes themselves are assumed
// to all be running the same binary with each sharing a common set of shared memory
// regions. We perform infrequent smaps reads of a single one of the processes and use
// the shared regions as a baseline to inform how the stat rss values (which include all
// memory without including any information about which of those memory regions are shared
// with other processes) of the whole set of processes correspond to reality. The final
// result is a single number which represents the sum total of the used memory of the
// process set.
#[derive(Debug)]
struct ProcessSet {
    page_size: u64,
    baseline_source: Option<u32>,
    baseline_rss: Option<u64>,
    baseline_our_shared: u64,
    baseline_total_shared: u64,
    baseline_last_update: Option<Instant>,
    baseline_ttl: Duration,
    stat_update_ttl: Duration,
    stat_last_update: Option<Instant>,
    total_rss_est: u64,
    per_pid_rss: Vec<(u32, u64)>,
    pids: HashMap<u32, Process>,
}

impl ProcessSet {
    const RSS_DIFF_REPORT_THRESHOLD_PRCNT: f32 = 5.0;

    fn new(baseline_ttl_ms: u64, stat_ttl_ms: u64) -> Self {
        let page_size = procfs::page_size().unwrap_or(4096) as u64;
        ProcessSet {
            page_size,
            baseline_source: None,
            baseline_rss: None,
            baseline_our_shared: 0,
            baseline_total_shared: 0,
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
        self.baseline_our_shared = 0;
        self.baseline_total_shared = 0;
        self.baseline_last_update = None;
        self.stat_last_update = None;
        self.total_rss_est = 0;
        self.per_pid_rss = Vec::new();
        self.pids = HashMap::new();
    }

    fn refresh(&mut self) {
        //self.remove_dead_pids();
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
            trace!(
                "Baseline refresh took: {} ms",
                new_now.duration_since(now).as_millis()
            );
            self.baseline_last_update = Some(new_now);
            // Reset now for any stat update
            now = new_now;
        }

        if refresh_stat {
            self.update_stat();
            let new_now = Instant::now();
            trace!(
                "Stat refresh took: {} ms",
                new_now.duration_since(now).as_millis()
            );
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
            // At any time a process could die, and we can be preempted during which
            // a process could die.
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
            // Todo: What about highly shared regions like glibc? We are getting dinged
            // for them right now.
            match self.smaps_get_shared_clean_baseline(&regions) {
                Ok((our_share, total)) => {
                    self.baseline_our_shared = our_share;
                    self.baseline_total_shared = total;
                    self.baseline_source = Some(key);
                }
                Err(e) => {
                    error!("Failed to read shared_clean: {:?}", e);
                }
            }

            match Self::smaps_region_sum_data(&regions, "Rss") {
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

    fn smaps_get_shared_clean_baseline(
        &self,
        smaps: &Vec<(MemoryMap, MemoryMapData)>,
    ) -> Result<(u64, u64)> {
        // This is where we calculate the baseline shared memory for the process
        //
        // There are a few different page situations but we are really only interested in
        // ones with non-zero "Shared_Clean" values in the map. From there we have maybe two
        // classes of regions we need to differentiate. Heavily shared libraries and lightly
        // shared binary regions. If we are running a binary called sleeper it will include, among
        // many different regions, it's own binary say /bin/sleeper as well as libc. Libc is very
        // heavily shared and very little (if any) gets accounted to our process set. The binary
        // that we are running however is completely accounted to our set. The objective with
        // this algorithm is to try and filter out some of the heavily shared regions since we
        // really shouldn't get "charged" for that. The PSS value is informative in this regard
        // as it is the kernel's value for how much of a shared region that process is accountable
        // for.

        // There is a bit of a race condition here. We can't snapshot the state of all the
        // processes at once, nor can we guarantee that we won't get preempted and return
        // when the process state has changed. Care should be taken with these values.
        let count = self.pids.len();
        if count < 1 {
            // Wait what? How?
            return Err(XError::missing("No pids in pid pool?"));
        }
        let mut our_share = 0;
        let mut total_sc = 0;
        for (region, data) in smaps {
            let map = &data.map;
            let (sc, pss) = match (map.get("Shared_Clean"), map.get("Pss")) {
                (Some(&sc), Some(&pss)) if sc != 0 => {
                    trace!("Counting {:?} -> {} / {}", region.pathname, pss, sc);
                    (sc, pss)
                }
                (Some(&sc), Some(&pss)) => {
                    // This Region has no shared clean values
                    trace!("Not Counting {:?} -> {} / {}", region.pathname, pss, sc);
                    continue;
                }
                (_, _) => {
                    continue;
                }
            };
            our_share += pss;
            total_sc += sc;
        }
        trace!(
            "Final accounting: Our share: {} total shared clean: {}",
            our_share, total_sc
        );
        Ok((our_share, total_sc))
    }

    fn smaps_region_sum_data(smaps: &Vec<(MemoryMap, MemoryMapData)>, key: &str) -> Result<u64> {
        let points: Vec<&u64> = smaps.iter().filter_map(|(_, md)| md.map.get(key)).collect();
        if points.is_empty() {
            Err(XError::missing(format!(
                "No data points for {} in smaps",
                key
            )))
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
                    // If we are looking at the baseline pid we want to get an idea if the stat
                    // and smaps rss values are super far off.
                    if Some(*key) == self.baseline_source {
                        if let Some(baseline_rss) = self.baseline_rss {
                            let diff = (stat_rss as f32 - baseline_rss as f32).abs();
                            let diff_percent = 100.0 * diff / baseline_rss as f32;
                            if diff_percent > Self::RSS_DIFF_REPORT_THRESHOLD_PRCNT {
                                debug!(
                                    "Difference between baseline and stat rss: {} ({:3.2}%)",
                                    diff, diff_percent
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to read proc from stat list: {:?}", e);
                }
            }
        }
        self.per_pid_rss = new_rss;
    }
    
    #[allow(unused)]
    fn remove_dead_pids(&mut self) {
        let to_remove: Vec<u32> = self
            .pids
            .iter()
            .filter_map(|x| {
                if !x.1.is_alive() {
                    trace!("Removing dead pid {}", x.0);
                    Some(*x.0)
                } else {
                    None
                }
            })
            .collect();
        for pid in to_remove {
            self.remove_pid(pid)
        }
    }

    fn add_pid(&mut self, pid: u32) -> Result<u32> {
        let proc = Process::new(pid as i32)?;
        self.pids.insert(pid, proc);
        // Set a delay on baseline update so we don't get bad baselines from measuring too early.
        // Observations indicate that the majority of processes settle a little bit and if we 
        // establish a baseline too early it can throw off our readings. We will perform a refresh
        // on the first request of readings with data eventually reaching something matching reality.
        self.baseline_source = Some(pid);
        self.baseline_last_update = Some(Instant::now());
        Ok(pid)
    }

    fn remove_pid(&mut self, pid: u32) {
        self.pids.remove(&pid);
        if let Some(index) = self.per_pid_rss.iter().position(|(lpid, _)| pid == *lpid) {
            self.per_pid_rss.swap_remove(index);
        }
        if self.pids.is_empty() {
            self.reset();
        }
    }

    fn get_memory_usage(&mut self) -> Result<MemoryUsage> {
        // This will refresh our data if it is needed. All data from the refresh is stored within
        // us once that call completes.
        self.refresh();
        let (rough_estimate, fine_estimate, rss_data) = match (self.baseline_source, &self.per_pid_rss) {
            (Some(_), rss_list) if !rss_list.is_empty() => {
                let adjusted_list = rss_list.iter().map(|(pid, rss)| {
                    // Remove the other's common share for each pid's rss value
                    let adjusted = rss.saturating_sub(self.baseline_total_shared);
                    if adjusted == 0 {
                        debug!("Rss value ({}) is below other's share ({}), pid {} will have 0 size", rss, self.baseline_total_shared, pid);
                    }
                    adjusted
                }).collect::<Vec<u64>>();
                let rough_adjusted_sum = adjusted_list.into_iter().sum::<u64>();
                let fine_adjusted_sum = rough_adjusted_sum
                    .saturating_add(rss_list.len() as u64 * self.baseline_our_shared);
                (
                    rough_adjusted_sum,
                    fine_adjusted_sum,
                    rss_list.iter().map(|(_, data)| *data).collect(),
                )
            }
            (Some(_), _) => {
                warn!("Baseline populated but rss is empty!");
                (0, 0, Vec::new())
            }
            (None, rss_list) if rss_list.is_empty() => (0, 0, Vec::new()),
            (None, rss_list) => {
                warn!("Baseline empty, total rss value will not be accurate!");
                let rss_sum = rss_list.iter().map(|(_, rss)| rss).sum();
                (
                    rss_sum,
                    rss_sum,
                    rss_list.iter().map(|(_, data)| *data).collect(),
                )
            }
        };
        let result = MemoryUsage {
            rough_estimate,
            fine_estimate,
            rss_data,
        };
        Ok(result)
    }

    // This is a helper function to aid in analyzing whether or not
    // the reported values of the algorithm are accurate.
    //
    // WARNING: Smaps read take a really long time
    fn get_actual_usage(&mut self) -> Result<MemoryUsage> {
        let start = Instant::now();
        let proc_data: Vec<(u64, u64, u64)> = self
            .pids
            .iter()
            .filter_map(|(_key, proc)| proc.smaps().ok())
            .map(|smaps| {
                let rss: u64 = smaps
                    .iter()
                    .filter_map(|(_, mapdata)| mapdata.map.get("Rss"))
                    .sum();
                let pss: u64 = smaps
                    .iter()
                    .filter_map(|(_, mapdata)| mapdata.map.get("Pss"))
                    .sum();
                let shared_clean: u64 = smaps
                    .iter()
                    .filter_map(|(_, mapdata)| mapdata.map.get("Shared_Clean"))
                    .sum();
                (rss, pss, shared_clean)
            })
            .collect();
        let len = proc_data.len() as u64;
        let (rough_estimate, fine_estimate, rss_data) = if len != 0 {
            let adjusted: u64 = proc_data
                .iter()
                .map(|(rss, _, shared)| rss.saturating_sub(*shared))
                .sum();
            let total_pss: u64 = proc_data.iter().map(|(_, pss, _)| *pss).sum();
            let shared_clean: u64 = proc_data.iter().map(|(_, _, shared)| shared).sum();
            let average_shared_clean = shared_clean / proc_data.len() as u64;
            (
                adjusted + average_shared_clean,
                total_pss,
                proc_data.iter().map(|(rss, _, _)| *rss).collect(),
            )
        } else {
            (0, 0, Vec::new())
        };
        trace!(
            "Reading actual usage took {} ms",
            Instant::now().duration_since(start).as_millis()
        );
        Ok(MemoryUsage {
            rough_estimate,
            fine_estimate,
            rss_data,
        })
    }
}

#[derive(Debug)]
struct MemoryUsage {
    rough_estimate: u64,
    fine_estimate: u64,
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
            }
            RemovePid(pid) => {
                info!("Removing pid {} from monitor", pid);
                let mut lock = data.lock().unwrap();
                lock.remove_pid(pid);
            }
            Measure => {
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
