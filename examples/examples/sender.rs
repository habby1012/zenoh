use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use clap::Parser;
use zenoh::{bytes::ZBytes, qos::CongestionControl, qos::Priority, Config, Wait};
use zenoh_examples::CommonArgs;

fn main() {
    zenoh::init_log_from_env_or("error");

    let (config, size, n, express, user_priority) = parse_args();
    let session = zenoh::open(config).wait().unwrap();

    let dummy_data: Vec<u8> = (0..size - 16).map(|i| (i % 256) as u8).collect();

    // Publisher 1: user-specified priority
    let pub1 = session
        .declare_publisher("data1")
        .congestion_control(CongestionControl::Drop)
        .priority(user_priority)
        .express(express)
        .wait()
        .unwrap();

    // Publisher 2: priority 3
    let pub2 = session
        .declare_publisher("data2")
        .congestion_control(CongestionControl::Drop)
        .priority(Priority::InteractiveLow)
        .express(express)
        .wait()
        .unwrap();

    // Publisher 3: priority 4
    let pub3 = session
        .declare_publisher("data3")
        .congestion_control(CongestionControl::Drop)
        .priority(Priority::DataHigh)
        .express(express)
        .wait()
        .unwrap();

    // Spawn three threads
    let d1 = dummy_data.clone();
    let t1 = thread::spawn(move || {
        println!("Start sending to data1 with priority {:?}", user_priority);
        for i in 0..n {
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros()
                .to_be_bytes();
            let mut data = ts.to_vec();
            data.extend_from_slice(&d1);
            pub1.put(ZBytes::from(data)).wait().unwrap();
            println!("[data1] Sent message {} with priority {:?}", i + 1, user_priority);
        }
    });

    let d2 = dummy_data.clone();
    let t2 = thread::spawn(move || {
        thread::sleep(Duration::from_secs(5));
        println!("Start sending to data2 with priority 3");
        for i in 0..n {
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros()
                .to_be_bytes();
            let mut data = ts.to_vec();
            data.extend_from_slice(&d2);
            pub2.put(ZBytes::from(data)).wait().unwrap();
            println!("[data2] Sent message {} with priority 3", i + 1);
        }
    });

    let d3 = dummy_data;
    let t3 = thread::spawn(move || {
        thread::sleep(Duration::from_secs(10));
        println!("Start sending to data3 with priority 4");
        for i in 0..n {
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros()
                .to_be_bytes();
            let mut data = ts.to_vec();
            data.extend_from_slice(&d3);
            pub3.put(ZBytes::from(data)).wait().unwrap();
            println!("[data3] Sent message {} with priority 4", i + 1);
        }
    });

    // Wait for all threads to finish
    t1.join().unwrap();
    t2.join().unwrap();
    t3.join().unwrap();
}

#[derive(Parser)]
struct Args {
    #[arg(short = 'n', long, default_value = "100")]
    samples: usize,
    #[arg(short = 's', long, default_value = "128")]
    payload_size: usize,
    #[arg(long, default_value = "false")]
    express: bool,
    #[arg(short = 'p', long, default_value = "5")]
    priority: u8,
    #[command(flatten)]
    common: CommonArgs,
}

fn parse_args() -> (Config, usize, usize, bool, Priority) {
    let args = Args::parse();

    let priority = match args.priority {
        1 => Priority::RealTime,
        2 => Priority::InteractiveHigh,
        3 => Priority::InteractiveLow,
        4 => Priority::DataHigh,
        5 => Priority::Data,
        6 => Priority::DataLow,
        7 => Priority::Background,
        _ => {
            eprintln!("Invalid priority value. Using default (Data = 5).");
            Priority::Data
        }
    };

    (
        args.common.into(),
        args.payload_size,
        args.samples,
        args.express,
        priority,
    )
}

