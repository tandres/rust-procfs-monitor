fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < (3 + 1) {
        panic!("Missing args!\n sleeper <loops> <seconds_between_allocs> <pages>");
    }
    let mut heap_eater = Vec::new();

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
        std::thread::sleep(std::time::Duration::from_secs(seconds_between_loops));
    }
}
