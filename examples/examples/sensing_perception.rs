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
    let _sub_planning_trajectory = session.declare_subscriber("planning/trajectory")
        .callback(move |sample| {
            calculate_latency("planning/trajectory", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    let _sub_planning_status = session.declare_subscriber("planning/status")
        .callback(move |sample| {
            calculate_latency("planning/status", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    // === Declare publishers ===
    let publisher_lidar = Arc::new(session
        .declare_publisher(keyexpr::new("sensing/lidar/pointcloud").unwrap())
        .congestion_control(CongestionControl::Block)
        .priority(Priority::Data) // 5
        .wait()
        .unwrap());

    let publisher_camera = Arc::new(session
        .declare_publisher(keyexpr::new("sensing/camera/raw_image").unwrap())
        .congestion_control(CongestionControl::Block)
        .priority(Priority::Data) // 5
        .wait()
        .unwrap());

    let publisher_objects = Arc::new(session
        .declare_publisher(keyexpr::new("perception/objects").unwrap())
        .congestion_control(CongestionControl::Block)
        .priority(Priority::InteractiveLow) // 3
        .wait()
        .unwrap());

    // === Prepare dummy payloads ===
    let lidar_data = vec![0; 1500000 - 16];
    let camera_data = vec![0; 500000 - 16];
    let object_data = vec![0; 10000 - 16];

    // === Spawn threads for each topic ===
    let lidar_pub = publisher_lidar.clone();
    let lidar_data = lidar_data.clone();
    spawn(move || {
        loop {
            send_with_timestamp(&lidar_pub, &lidar_data);
            thread::sleep(Duration::from_millis(100));
        }
    });

    let camera_pub = publisher_camera.clone();
    let camera_data = camera_data.clone();
    spawn(move || {
        loop {
            send_with_timestamp(&camera_pub, &camera_data);
            thread::sleep(Duration::from_millis(100));
        }
    });

    let object_pub = publisher_objects.clone();
    let object_data = object_data.clone();
    spawn(move || {
        loop {
            send_with_timestamp(&object_pub, &object_data);
            thread::sleep(Duration::from_millis(100));
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

