use log::{debug, error, info, trace, warn};
use std::{
    path::PathBuf,
    thread,
};
use tokio::time;

mod process_set_monitor_linux;
use process_set_monitor_linux as process_set_monitor;


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

    let mut monitor = process_set_monitor::ProcessSetMonitor::new(10_000, 1_000);
    let monitor_fn = monitor.start();
    thread::spawn(monitor_fn);

    let mut children = Vec::new();
    for _ in 0..4 {
        let monitor_clone = monitor.clone();
        let target_clone = target.clone();
        let args_clone = target_args.clone();
        let new_child = tokio::spawn(async move {
            let mut child = tokio::process::Command::new(target_clone)
                .args(args_clone)
                .spawn()
                .unwrap();
            if let Some(pid) = child.id() {
                monitor_clone.add_pid(pid).await.unwrap();
                let _ = child.wait().await;
                monitor_clone.remove_pid(pid).await.unwrap();
            };
        });
        children.push(new_child);
    }

    let printer = tokio::spawn(async move {
        let mut interval = time::interval(time::Duration::from_millis(5000));
        loop {
            interval.tick().await;
            monitor.report_set().await.unwrap();
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


