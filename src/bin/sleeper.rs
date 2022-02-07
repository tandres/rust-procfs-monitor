use std::time::{Duration, Instant};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < (3 + 1) {
        panic!("Missing args!\n sleeper <loops> <seconds_between_allocs> <pages>");
    }
    let mut heap_eater = Vec::new();
    let busy = true;
    let loops = args[1].parse::<usize>().unwrap();
    let seconds_between_loops = args[2].parse::<u64>().unwrap();
    let pages = args[3].parse::<usize>().unwrap();
    for i in 0..loops {
        if i < loops / 2 && heap_eater.len() < pages {
            let array: Box<Vec<u8>> = Box::new(vec![1; 4096]);
            heap_eater.push(array);
        } else if i > loops / 2 && !heap_eater.is_empty() {
            let _ = heap_eater.pop();
        }
        if busy {
            let start = Instant::now();

            while start.elapsed() < Duration::from_secs(1) {
                for block in heap_eater.iter_mut() {
                    block[0] = block[0].wrapping_add(1);
                }
            }
        } else {
            std::thread::sleep(std::time::Duration::from_secs(seconds_between_loops));
        }
    }
}
