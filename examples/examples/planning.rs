use clap::Parser;
use zenoh::{
    key_expr::keyexpr,
    qos::{CongestionControl, Priority},
    Config, Wait,
    bytes::ZBytes
};
use zenoh_examples::CommonArgs;
use std::{thread, time::Duration};
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    zenoh::init_log_from_env_or("error");

    let (config, size, priority) = parse_args();
    let session = zenoh::open(config).wait().unwrap();

    let key_expr_planning_trajectory = keyexpr::new("planning/trajectory").unwrap();
    let key_expr_planning_status = keyexpr::new("planning/status").unwrap();

    let _sub_lidar = session.declare_subscriber("sensing/lidar")
        .callback(move |sample| {
            calculate_latency("sensing/lidar", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    let _sub_camera = session.declare_subscriber("sensing/camera")
        .callback(move |sample| {
            calculate_latency("sensing/camera", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    let _sub_control_actuary = session.declare_subscriber("control/actuary")
        .callback(move |sample| {
            calculate_latency("control/actuary", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    let _sub_control_status = session.declare_subscriber("control/status")
        .callback(move |sample| {
            calculate_latency("control/status", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    let publisher_planning_trajectory = session
        .declare_publisher(key_expr_planning_trajectory)
        .congestion_control(CongestionControl::Block)
        .priority(priority)
        .wait()
        .unwrap();

    let publisher_planning_status = session
        .declare_publisher(key_expr_planning_status)
        .congestion_control(CongestionControl::Block)
        .priority(priority)
        .wait()
        .unwrap();

    let dummy_data: Vec<u8> = vec![0; size];

    loop {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros()
            .to_be_bytes();

        let mut planning_trajectory_data = timestamp.to_vec();
        let mut planning_status_data = timestamp.to_vec();
        planning_trajectory_data.extend_from_slice(&dummy_data);
        planning_status_data.extend_from_slice(&dummy_data);

        publisher_planning_trajectory.put(ZBytes::from(planning_trajectory_data)).wait().unwrap();
        publisher_planning_status.put(ZBytes::from(planning_status_data)).wait().unwrap();

        thread::sleep(Duration::from_millis(100));
    }
}

#[derive(Parser, Clone, Debug)]
struct Args {
    #[arg(short = 's', long, default_value = "1024")]
    payload_size: usize,

    #[arg(short = 'p', long, default_value = "3")]
    /// Set the priority level (1=RealTime, 2=InteractiveHigh, 3=InteractiveLow, 4=DataHigh, 5=Data, 6=DataLow, 7=Background)
    priority: u8,

    #[command(flatten)]
    common: CommonArgs,
}

fn parse_args() -> (Config, usize, Priority) {
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
            eprintln!("Invalid priority value. Using default (InteractiveLow = 3).");
            Priority::InteractiveLow
        }
    };

    (args.common.into(), args.payload_size, priority,)
}

fn calculate_latency(topic: &str, payload: &[u8]) {
    if payload.len() < 16 {
        return;
    }

    let original_timestamp_bytes: [u8; 16] = payload[0..16].try_into().unwrap();
    let original_timestamp = u128::from_be_bytes(original_timestamp_bytes);

    let current_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    
    let latency = current_timestamp - original_timestamp;
    println!("Latency for {}: {} µs", topic, latency);
}
