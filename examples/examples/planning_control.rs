use clap::Parser;
use zenoh::{
    bytes::ZBytes, key_expr::keyexpr, pubsub::Publisher, qos::{CongestionControl, Priority}, Config, Wait
};
use zenoh_examples::CommonArgs;
use std::{thread, time::Duration};
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use std::thread::spawn;

fn main() {
    zenoh::init_log_from_env_or("error");
    let (config, _) = parse_args();
    let session = zenoh::open(config).wait().unwrap();

    // === Subscribe ===
    let _sub_sensing_lidar = session.declare_subscriber("sensing/lidar/pointcloud")
        .callback(move |sample| {
            calculate_latency("sensing/lidar/pointcloud", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    let _sub_sensing_camera = session.declare_subscriber("sensing/camera/raw_image")
        .callback(move |sample| {
            calculate_latency("sensing/camera/raw_image", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    let _sub_perception_objects = session.declare_subscriber("perception/objects")
        .callback(move |sample| {
            calculate_latency("perception/objects", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    // === Declare publishers ===
    let publisher_planning_trajectory = Arc::new(session
        .declare_publisher(keyexpr::new("planning/trajectory").unwrap())
        .congestion_control(CongestionControl::Block)
        .priority(Priority::InteractiveHigh) // 2
        .wait()
        .unwrap());

    let publisher_planning_status = Arc::new(session
        .declare_publisher(keyexpr::new("planning/status").unwrap())
        .congestion_control(CongestionControl::Block)
        .priority(Priority::Background) // 7
        .wait()
        .unwrap());

    // === Prepare dummy payloads ===
    let trajectory_data = vec![0; 20000 - 16];
    let status_data= vec![0; 100000 - 16];

    // === Spawn threads for each topic ===
    let trajectory_pub = publisher_planning_trajectory.clone();
    let trajectory_data = trajectory_data.clone();
    spawn(move || {
        loop {
            send_with_timestamp(&trajectory_pub, &trajectory_data);
            thread::sleep(Duration::from_millis(50));
        }
    });

    let status_pub = publisher_planning_status.clone();
    let status_data = status_data.clone();
    spawn(move || {
        loop {
            send_with_timestamp(&status_pub, &status_data);
            thread::sleep(Duration::from_millis(500));
        }
    });

    // Keep main thread alive
    loop {
        thread::sleep(Duration::from_secs(60));
    }
}

#[derive(Parser, Clone, Debug)]
struct Args {
    #[command(flatten)]
    common: CommonArgs,
}

fn parse_args() -> (Config, ()) {
    let args = Args::parse();
    (args.common.into(), ())
}

fn send_with_timestamp(publisher: &Arc<Publisher<'_>>, data: &[u8]) {
    let mut buf = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
        .to_be_bytes()
        .to_vec();
    buf.extend_from_slice(data);
    publisher.put(ZBytes::from(buf)).wait().unwrap();
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

