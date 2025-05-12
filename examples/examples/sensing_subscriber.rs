use clap::Parser;
use zenoh::{key_expr::keyexpr, Config, Wait};
use zenoh_examples::CommonArgs;
use std::{thread, time::Duration};
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    zenoh::init_log_from_env_or("error");
    let (config, _) = parse_args();
    let session = zenoh::open(config).wait().unwrap();

    // === Subscribe to all sensing topics ===
    let _sub_front_camera = session
        .declare_subscriber(keyexpr::new("camera/front").unwrap())
        .callback(|sample| {
            calculate_latency("camera/front", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    let _sub_left_camera = session
        .declare_subscriber(keyexpr::new("camera/left").unwrap())
        .callback(|sample| {
            calculate_latency("camera/left", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    let _sub_right_camera = session
        .declare_subscriber(keyexpr::new("camera/right").unwrap())
        .callback(|sample| {
            calculate_latency("camera/right", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    let _sub_rear_camera = session
        .declare_subscriber(keyexpr::new("camera/rear").unwrap())
        .callback(|sample| {
            calculate_latency("camera/rear", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    let _sub_lidar_main = session
        .declare_subscriber(keyexpr::new("lidar/main").unwrap())
        .callback(|sample| {
            calculate_latency("lidar/main", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    let _sub_lidar_aux = session
        .declare_subscriber(keyexpr::new("lidar/aux").unwrap())
        .callback(|sample| {
            calculate_latency("lidar/aux", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    let _sub_radar_front = session
        .declare_subscriber(keyexpr::new("radar/front").unwrap())
        .callback(|sample| {
            calculate_latency("radar/front", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    let _sub_radar_rear = session
        .declare_subscriber(keyexpr::new("radar/rear").unwrap())
        .callback(|sample| {
            calculate_latency("radar/rear", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    let _sub_objects = session
        .declare_subscriber(keyexpr::new("perception/objects").unwrap())
        .callback(|sample| {
            calculate_latency("perception/objects", sample.payload().to_bytes().as_ref());
        })
        .wait()
        .unwrap();

    // Keep thread alive
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

fn calculate_latency(topic: &str, payload: &[u8]) {
    if payload.len() < 24 {
        return;
    }

    let ts_bytes: [u8; 16] = payload[0..16].try_into().unwrap();
    let original_timestamp = u128::from_be_bytes(ts_bytes);

    let seq_bytes: [u8; 8] = payload[16..24].try_into().unwrap();
    let seq = u64::from_be_bytes(seq_bytes);

    let current_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();

    let latency = current_timestamp - original_timestamp;
    println!("{} | seq = {} | latency = {} µs", topic, seq, latency);
}
