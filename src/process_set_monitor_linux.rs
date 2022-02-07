use log::{debug, error, info, trace, warn};
use procfs::{
    process::{MemoryMap, MemoryMapData, Process, Stat},
    ProcError,
};
use std::{
    collections::HashMap,
    sync::{
        mpsc::{self, Sender},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::oneshot;

use thiserror::Error;

type Result<T> = std::result::Result<T, ProcessSetMonitorError>;

#[derive(Debug, Error)]
pub enum ProcessSetMonitorError {
    #[error("Procfs Error")]
    ProcfsError(#[from] ProcError),
    #[error("StdIO Error {0}")]
    StdIo(#[from] std::io::Error),
    #[error("Missing {0}")]
    Missing(String),
    #[error("Not Started!")]
    NotStarted,
    #[error("Send/Receive Failed, worker thread might be dead")]
    ThreadDead,
}

impl ProcessSetMonitorError {
    fn missing<S: AsRef<str>>(msg: S) -> ProcessSetMonitorError {
        let msg = msg.as_ref().to_string();
        ProcessSetMonitorError::Missing(msg)
    }
}

enum MonitorOp {
    AddPid(u32, oneshot::Sender<Result<u32>>),
    RemovePid(u32, oneshot::Sender<Result<u32>>),
    GetReport(oneshot::Sender<Result<ProcessSetMonitorReport>>),
    Quit,
}

#[derive(Clone)]
pub struct ProcessSetMonitor {
    smaps_cooldown_ms: u64,
    stat_cooldown_ms: u64,
    inner_sender: Option<Arc<Mutex<Sender<MonitorOp>>>>,
}

impl ProcessSetMonitor {
    pub fn new(smaps_cooldown_ms: u64, stat_cooldown_ms: u64) -> ProcessSetMonitor {
        ProcessSetMonitor {
            smaps_cooldown_ms,
            stat_cooldown_ms,
            inner_sender: None,
        }
    }

    pub fn start(&mut self) -> impl Fn() {
        let (tx, rx) = mpsc::channel();
        self.inner_sender = Some(Arc::new(Mutex::new(tx)));
        let smaps_cooldown = self.smaps_cooldown_ms;
        let stat_cooldown = self.stat_cooldown_ms;
        move || {
            //   let tx_clone = tx.clone();
            let mut inner = ProcessSetMonitorInner::new(smaps_cooldown, stat_cooldown);
            loop {
                let msg = rx.recv().unwrap_or(MonitorOp::Quit);
                use MonitorOp::*;
                match msg {
                    AddPid(pid, tx) => {
                        info!("Adding new pid {} to monitor", pid);
                        let _ = tx.send(inner.add_pid(pid));
                    }
                    RemovePid(pid, tx) => {
                        info!("Removing pid {} from monitor", pid);
                        let _ = tx.send(inner.remove_pid(pid));
                    }
                    GetReport(tx) => {
                        info!("Generating Report");
                        let _ = tx.send(inner.get_report());
                    }
                    Quit => {
                        info!("Monitor loop quiting");
                        break;
                    }
                }
            }
        }
    }

    pub async fn add_pid(&self, pid: u32) -> Result<u32> {
        let inner_sender = self
            .inner_sender
            .as_ref()
            .ok_or(ProcessSetMonitorError::NotStarted)?;
        let (tx, rx) = oneshot::channel();
        let op = MonitorOp::AddPid(pid, tx);
        {
            let _ = inner_sender.lock().unwrap().send(op);
        }
        rx.await
            .map_err(|e: tokio::sync::oneshot::error::RecvError| {
                error!("Receiver dropped unexpectedly: {:?}", e);
                ProcessSetMonitorError::ThreadDead
            })?
    }

    pub async fn remove_pid(&self, pid: u32) -> Result<u32> {
        let inner_sender = self
            .inner_sender
            .as_ref()
            .ok_or(ProcessSetMonitorError::NotStarted)?;
        let (tx, rx) = oneshot::channel();
        let op = MonitorOp::RemovePid(pid, tx);
        {
            let _ = inner_sender.lock().unwrap().send(op);
        }
        rx.await.map_err(|e| {
            error!("Receiver dropped unexpectedly: {:?}", e);
            ProcessSetMonitorError::ThreadDead
        })?
    }

    pub async fn get_report(&self) -> Result<ProcessSetMonitorReport> {
        let inner_sender = self
            .inner_sender
            .as_ref()
            .ok_or(ProcessSetMonitorError::NotStarted)?;
        let (tx, rx) = oneshot::channel();
        let op = MonitorOp::GetReport(tx);
        {
            let _ = inner_sender.lock().unwrap().send(op);
        }
        rx.await.map_err(|e| {
            error!("Receiver dropped unexpectedly: {:?}", e);
            ProcessSetMonitorError::ThreadDead
        })?
    }
}

#[derive(Debug)]
pub struct ProcessSetMonitorPerPidReport {
    pid: u32,
    rss_measured: u32,
    cpu_percent: f32,
    pss_est: u32,
}

#[derive(Debug)]
pub struct ProcessSetMonitorReport {
    total_rss_estimate: u64,
    total_pss_estimate: u64,
    pid_data: Vec<ProcessSetMonitorPerPidReport>,
}

#[derive(Debug)]
struct InnerPidData {
    process: Process,
    stat: Stat,
    last_stat_read: Instant,
    cpu_perc: f32,
    baseline_last_update: Option<Instant>,
    baseline: BaselineMeasurement,
}

impl InnerPidData {
    fn new(pid: u32) -> Result<InnerPidData> {
        let process = Process::new(pid as i32)?;
        let stat = process.stat.clone();
        Ok(InnerPidData {
            stat,
            process,
            last_stat_read: Instant::now(),
            cpu_perc: 0.,
            baseline_last_update: None,
            baseline: BaselineMeasurement::new(),
        })
    }

    fn is_baseline_valid(&self, now: &Instant, ttl: &Duration) -> bool {
        if let Some(time) = self.baseline_last_update {
            now.duration_since(time) < *ttl
        } else {
            false
        }
    }
}

#[derive(Clone, Debug)]
struct BaselineMeasurement {
    // These fields are are delivered to us as u64s, however we don't anticipate
    // that such a large value is strictly necessary.
    shared_clean_bytes: u32,
    shared_dirty_bytes: u32,
    private_clean_bytes: u32,
    private_dirty_bytes: u32,
    pss_bytes: u32,
    rss_bytes: u32,
}

impl BaselineMeasurement {
    fn new() -> BaselineMeasurement {
        BaselineMeasurement {
            shared_clean_bytes: 0,
            shared_dirty_bytes: 0,
            private_clean_bytes: 0,
            private_dirty_bytes: 0,
            pss_bytes: 0,
            rss_bytes: 0,
        }
    }
}

impl From<&Vec<(MemoryMap, MemoryMapData)>> for BaselineMeasurement {
    fn from(data: &Vec<(MemoryMap, MemoryMapData)>) -> BaselineMeasurement {
        let mut shared_clean_bytes = 0;
        let mut shared_dirty_bytes = 0;
        let mut private_clean_bytes = 0;
        let mut private_dirty_bytes = 0;
        let mut pss_bytes = 0;
        let mut rss_bytes = 0;
        for (_, region) in data {
            let rmap = &region.map;

            shared_clean_bytes += rmap.get("Shared_Clean").copied().unwrap_or(0) as u32;
            shared_dirty_bytes += rmap.get("Shared_Dirty").copied().unwrap_or(0) as u32;
            private_clean_bytes += rmap.get("Private_Clean").copied().unwrap_or(0) as u32;
            private_dirty_bytes += rmap.get("Private_Dirty").copied().unwrap_or(0) as u32;

            pss_bytes += rmap.get("Pss").copied().unwrap_or(0) as u32;
            rss_bytes += rmap.get("Rss").copied().unwrap_or(0) as u32;
        }
        BaselineMeasurement {
            shared_clean_bytes,
            shared_dirty_bytes,
            private_clean_bytes,
            private_dirty_bytes,
            pss_bytes,
            rss_bytes,
        }
    }
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
struct ProcessSetMonitorInner {
    page_size: u32,
    baseline_ttl: Duration,
    stat_update_ttl: Duration,
    stat_last_update: Option<Instant>,
    pids: HashMap<u32, InnerPidData>,
}

impl ProcessSetMonitorInner {
    const RSS_DIFF_REPORT_THRESHOLD_PRCNT: f32 = 5.0;

    fn new(baseline_ttl_ms: u64, stat_ttl_ms: u64) -> Self {
        ProcessSetMonitorInner {
            page_size: procfs::page_size().unwrap() as u32,
            baseline_ttl: Duration::from_millis(baseline_ttl_ms),
            stat_update_ttl: Duration::from_millis(stat_ttl_ms),
            stat_last_update: None,
            pids: HashMap::new(),
        }
    }

    fn reset(&mut self) {
        self.stat_last_update = None;
        self.pids = HashMap::new();
    }

    fn refresh(&mut self) -> Result<()> {
        let mut now = Instant::now();
        // Check if our data needs to be updated
        let refresh_baseline = !self.pids.iter().any(|(_, p)| {
            p.baseline_last_update
                .map(|time| now.duration_since(time) < self.baseline_ttl)
                .unwrap_or(false)
        });
        let refresh_stat = match self.stat_last_update {
            Some(time) if now.duration_since(time) < self.stat_update_ttl => false,
            _ => true,
        };

        if refresh_baseline {
            self.update_baseline()?;
            let new_now = Instant::now();
            trace!(
                "Baseline refresh took: {} ms",
                new_now.duration_since(now).as_millis()
            );
            // Reset now for any stat update
            now = new_now;
        }

        if refresh_stat {
            self.update_stat()?;
            let new_now = Instant::now();
            trace!(
                "Stat refresh took: {} ms",
                new_now.duration_since(now).as_millis()
            );
            self.stat_last_update = Some(new_now);
        }
        Ok(())
    }

    fn update_baseline(&mut self) -> Result<()> {
        for (pid, data) in self.pids.iter_mut() {
            match data.process.smaps() {
                Ok(smaps) => {
                    data.baseline_last_update = Some(Instant::now());
                    data.baseline = BaselineMeasurement::from(&smaps);
                    return Ok(());
                }
                Err(e) => {
                    error!("Failed to read pid {}! {}", pid, e);
                }
            }
        }
        Err(ProcessSetMonitorError::missing(
            "Unable to find living pid for baseline",
        ))
    }

    fn update_stat(&mut self) -> Result<()> {
        let now = Instant::now();
        let ticks_per_second = procfs::ticks_per_second()?;
        for (_key, proc) in self.pids.iter_mut() {
            match proc.process.stat() {
                Ok(stat) => {
                    // This is some debug code to watch to see if we get a really
                    // bad rss reading from stat. There's not a whole lot we can do
                    // if we do besides report it. We can only really tell if the
                    // stat entry we are about to update has a corresponding smaps
                    // baseline valid.
                    //
                    // It's not clear the exact mechanism why these two readings can
                    // be off. The answer is deep in the kernel memory accounting code
                    // but it appears that stat is updated more in response to process
                    // activity rather than consistently or at read time. From limited
                    // observation and measurement it seems like smaller single core
                    // systems might be more prompt than my desktop.
                    if proc.is_baseline_valid(&now, &self.baseline_ttl) {
                        // Unwrapping rss_bytes() shouldn't be a problem because the page_size
                        // error should be caught during initialization;
                        let stat_rss = stat.rss_bytes().unwrap();
                        let diff = (stat_rss as f32 / proc.baseline.rss_bytes as f32).abs();
                        let percent = 100.0 * diff / proc.baseline.rss_bytes as f32;
                        if percent > Self::RSS_DIFF_REPORT_THRESHOLD_PRCNT {
                            debug!(
                                "Difference between baseline and stat rss: {} ({:3.2}%)",
                                diff, percent
                            );
                        }
                    }
                    // Get the old and up to date user+system time for this process
                    let cur_proc_usage = stat.utime + stat.stime;
                    let old_proc_usage = proc.stat.utime + proc.stat.stime;

                    // Determine how much that value has changed since the last update
                    // and turn it into a total cpu time
                    let proc_delta = cur_proc_usage - old_proc_usage;
                    let proc_duration =
                        Duration::from_millis((1000 * proc_delta) / ticks_per_second as u64);
                    let elapsed_time = now.duration_since(proc.last_stat_read);
                    let cpu_perc =
                        100.0 * proc_duration.as_millis() as f32 / elapsed_time.as_millis() as f32;

                    proc.stat = stat;
                    proc.cpu_perc = cpu_perc;
                    proc.last_stat_read = now;
                }
                Err(e) => {
                    warn!("Failed to read proc from stat list: {:?}", e);
                }
            }
        }
        Ok(())
    }

    fn add_pid(&mut self, pid: u32) -> Result<u32> {
        self.pids.insert(pid, InnerPidData::new(pid)?);
        // Set a delay on baseline update so we don't get bad baselines from measuring too early.
        // Observations indicate that the majority of processes settle a little bit and if we
        // establish a baseline too early it can throw off our readings. We will perform a refresh
        // on the first request of readings with data eventually reaching something matching reality.
        Ok(pid)
    }

    fn remove_pid(&mut self, pid: u32) -> Result<u32> {
        self.pids
            .remove(&pid)
            .ok_or(ProcessSetMonitorError::missing(format!(
                "Remove requested for untracked pid {}",
                pid
            )))?;
        if self.pids.is_empty() {
            self.reset();
        }
        Ok(pid)
    }

    fn get_report(&mut self) -> Result<ProcessSetMonitorReport> {
        // This will refresh our data if it is needed. All data from the refresh is stored within
        // us once that call completes.
        self.refresh()?;
        let mut pid_reports = Vec::new();
        let mut total_rss_estimate = 0;
        let mut total_pss_estimate = 0;
        let now = Instant::now();
        let baseline = self
            .pids
            .iter()
            .find_map(|(_, data)| {
                data.is_baseline_valid(&now, &self.baseline_ttl)
                    .then(|| data.baseline.clone())
            })
            .ok_or(ProcessSetMonitorError::missing("Missing baseline"))?;
        let shared_clean_discount = baseline.shared_clean_bytes
            - baseline
                .shared_clean_bytes
                .checked_div(self.pids.len() as u32)
                .unwrap_or(baseline.shared_clean_bytes);

        for (_, data) in self.pids.iter() {
            // rss_bytes() can only fail if procfs doesn't know the page size somehow
            let rss_measured = data.stat.rss_bytes().unwrap() as u32;
            let pss_est = rss_measured.saturating_sub(shared_clean_discount);
            let pid_report = ProcessSetMonitorPerPidReport {
                pid: data.process.pid as u32,
                rss_measured,
                cpu_percent: data.cpu_perc,
                pss_est,
            };
            total_pss_estimate += pid_report.pss_est as u64;
            total_rss_estimate += pid_report.rss_measured as u64;
            pid_reports.push(pid_report);
        }

        Ok(ProcessSetMonitorReport {
            total_rss_estimate,
            total_pss_estimate,
            pid_data: pid_reports,
        })
    }

    // This is a helper function to aid in analyzing whether or not
    // the reported values of the algorithm are accurate.
    //
    // WARNING: Smaps read take a really long time
    #[allow(unused)]
    fn get_actual_usage(&mut self) {
        let start = Instant::now();
        let pss: Vec<u64> = self
            .pids
            .iter()
            .filter_map(|(_key, proc)| proc.process.smaps().ok())
            .map(|smaps| {
                let pss: u64 = smaps
                    .iter()
                    .filter_map(|(_, mapdata)| mapdata.map.get("Pss"))
                    .sum();
                pss
            })
            .collect();
        trace!(
            "Reading actual usage took {} ms",
            Instant::now().duration_since(start).as_millis()
        );
        info!(
            "Actual pss: {} [{:?}]",
            pss.iter().map(|x| *x).sum::<u64>(),
            pss
        );
    }
}
