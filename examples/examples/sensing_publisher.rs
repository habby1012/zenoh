use rand::{Rng};
use clap::Parser;
use zenoh::{
    bytes::ZBytes,
    key_expr::keyexpr,
    pubsub::Publisher,
    qos::{CongestionControl, Priority},
    Config, Wait,
};
use zenoh_examples::CommonArgs;
use std::{thread, time::Duration};
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use std::thread::spawn;
use std::fs;

fn read_jitter_config() -> i64 {
    let contents = fs::read_to_string("/home/newslab/repos/zenoh/examples/examples/jitter.txt").unwrap_or_else(|_| "jitter = 0".to_string());
    for line in contents.lines() {
        if let Some(value) = line.strip_prefix("jitter = ") {
            if let Ok(parsed) = value.trim().parse::<i64>() {
                return parsed;
            }
        }
    }
    0 // default
}

fn main() {
    zenoh::init_log_from_env_or("error");
    let (config, priority) = parse_args();
    let session = zenoh::open(config).wait().unwrap();

    let jitter_range = read_jitter_config();
    // ============================ Publisher definitions ============================

    let pub_front_camera = Arc::new(session
        .declare_publisher(keyexpr::new("camera/front").unwrap())
        .congestion_control(CongestionControl::Drop)
        .priority(Priority::Data)
        .wait().unwrap());

    let pub_left_camera = Arc::new(session
        .declare_publisher(keyexpr::new("camera/left").unwrap())
        .congestion_control(CongestionControl::Drop)
        .priority(Priority::Data)
        .wait().unwrap());

    let pub_right_camera = Arc::new(session
        .declare_publisher(keyexpr::new("camera/right").unwrap())
        .congestion_control(CongestionControl::Drop)
        .priority(Priority::Data)
        .wait().unwrap());

    let pub_rear_camera = Arc::new(session
        .declare_publisher(keyexpr::new("camera/rear").unwrap())
        .congestion_control(CongestionControl::Drop)
        .priority(Priority::Data)
        .wait().unwrap());

    let pub_lidar_main = Arc::new(session
        .declare_publisher(keyexpr::new("lidar/main").unwrap())
        .congestion_control(CongestionControl::Drop)
        .priority(Priority::DataLow)
        .wait().unwrap());

    let pub_lidar_aux = Arc::new(session
        .declare_publisher(keyexpr::new("lidar/aux").unwrap())
        .congestion_control(CongestionControl::Drop)
        .priority(Priority::DataLow)
        .wait().unwrap());

    let pub_radar_front = Arc::new(session
        .declare_publisher(keyexpr::new("radar/front").unwrap())
        .congestion_control(CongestionControl::Drop)
        .priority(Priority::Data)
        .wait().unwrap());

    let pub_radar_rear = Arc::new(session
        .declare_publisher(keyexpr::new("radar/rear").unwrap())
        .congestion_control(CongestionControl::Drop)
        .priority(Priority::Data)
        .wait().unwrap());

    let pub_object = Arc::new(session
        .declare_publisher(keyexpr::new("perception/objects").unwrap())
        .congestion_control(CongestionControl::Drop)
        .priority(priority)
        .wait().unwrap());

    // ============================ Dummy payloads ============================

    let data_front_camera = vec![0; 1000000 - 16];
    let data_left_camera  = vec![0; 1000000 - 16];
    let data_right_camera = vec![0; 1000000 - 16];
    let data_rear_camera  = vec![0; 1000000 - 16];
    let data_lidar_main   = vec![0; 5000000 - 16];
    let data_lidar_aux    = vec![0; 5000000 - 16];
    let data_radar_front  = vec![0; 500000 - 16];
    let data_radar_rear   = vec![0; 500000 - 16];
    let data_object       = vec![0; 100000 - 16];

    // ============================ Spawn threads ============================

    let pub1 = pub_front_camera.clone();
    let data1 = data_front_camera.clone();
    spawn(move || {
        let mut seq = 1u64;
        let mut rng = rand::thread_rng();

        loop {
            send_with_timestamp_with_seq(&pub1, &data1, seq);
            seq += 1;
        
            let jitter = rng.gen_range(-jitter_range as f64..=jitter_range as f64);
            let base = 20.0; 
            let interval_ms = (base + jitter).max(1.0);

            thread::sleep(Duration::from_secs_f64(interval_ms / 1000.0));
        }
    });

    let pub2 = pub_left_camera.clone();
    let data2 = data_left_camera.clone();
    spawn(move || {
        let mut seq = 1u64;
        let mut rng = rand::thread_rng();

        loop {
            send_with_timestamp_with_seq(&pub2, &data2, seq);
            seq += 1;

            let jitter = rng.gen_range(-jitter_range as f64..=jitter_range as f64);
            let base = 20.0;  
            let interval_ms = (base + jitter).max(1.0);

            thread::sleep(Duration::from_secs_f64(interval_ms / 1000.0));
        }
    });

    let pub3 = pub_right_camera.clone();
    let data3 = data_right_camera.clone();
    spawn(move || {
        let mut seq = 1u64;
        let mut rng = rand::thread_rng();

        loop {
            send_with_timestamp_with_seq(&pub3, &data3, seq);
            seq += 1;
        
            let jitter = rng.gen_range(-jitter_range as f64..=jitter_range as f64);
            let base = 20.0;
            let interval_ms = (base + jitter).max(1.0);

            thread::sleep(Duration::from_secs_f64(interval_ms / 1000.0));
        }
    });

    let pub4 = pub_rear_camera.clone();
    let data4 = data_rear_camera.clone();
    spawn(move || {
        let mut seq = 1u64;
        let mut rng = rand::thread_rng();

        loop {
            send_with_timestamp_with_seq(&pub4, &data4, seq);
            seq += 1;
        
            let jitter = rng.gen_range(-jitter_range as f64..=jitter_range as f64);
            let base = 20.0;
            let interval_ms = (base + jitter).max(1.0);

            thread::sleep(Duration::from_secs_f64(interval_ms / 1000.0));
        }
    });

    let pub5 = pub_lidar_main.clone();
    let data5 = data_lidar_main.clone();
    spawn(move || {
        let mut seq = 1u64;
        let mut rng = rand::thread_rng();

        loop {
            send_with_timestamp_with_seq(&pub5, &data5, seq);
            seq += 1;

            let jitter = rng.gen_range(-jitter_range as f64..=jitter_range as f64);
            let base = 33.3;
            let interval_ms = (base + jitter).max(1.0);

            thread::sleep(Duration::from_secs_f64(interval_ms / 1000.0));
        }
    });

    let pub6 = pub_lidar_aux.clone();
    let data6 = data_lidar_aux.clone();
    spawn(move || {
        let mut seq = 1u64;
        let mut rng = rand::thread_rng();

        loop {
            send_with_timestamp_with_seq(&pub6, &data6, seq);
            seq += 1;
        
            let jitter = rng.gen_range(-jitter_range as f64..=jitter_range as f64);
            let base = 33.3;
            let interval_ms = (base + jitter).max(1.0);

            thread::sleep(Duration::from_secs_f64(interval_ms / 1000.0));
        }
    });

    let pub7 = pub_radar_front.clone();
    let data7 = data_radar_front.clone();
    spawn(move || {
        let mut seq = 1u64;
        let mut rng = rand::thread_rng();

        loop {
            send_with_timestamp_with_seq(&pub7, &data7, seq);
            seq += 1;
        
            let jitter = rng.gen_range(-jitter_range as f64..=jitter_range as f64);
            let base = 50.0;
            let interval_ms = (base + jitter).max(1.0);

            thread::sleep(Duration::from_secs_f64(interval_ms / 1000.0));
        }
    });

    let pub8 = pub_radar_rear.clone();
    let data8 = data_radar_rear.clone();
    spawn(move || {
        let mut seq = 1u64;
        let mut rng = rand::thread_rng();

        loop {
            send_with_timestamp_with_seq(&pub8, &data8, seq);
            seq += 1;
        
            let jitter = rng.gen_range(-jitter_range as f64..=jitter_range as f64);
            let base = 50.0;
            let interval_ms = (base + jitter).max(1.0);

            thread::sleep(Duration::from_secs_f64(interval_ms / 1000.0));
        }
    });

    let pub9 = pub_object.clone();
    let data9 = data_object.clone();
    spawn(move || {
        let mut seq = 1u64;
        let mut rng = rand::thread_rng();

        loop {
            send_with_timestamp_with_seq(&pub9, &data9, seq);
            seq += 1;
        
            let jitter = rng.gen_range(-jitter_range as f64..=jitter_range as f64);
            let base = 50.0;
            let interval_ms = (base + jitter).max(1.0);

            thread::sleep(Duration::from_secs_f64(interval_ms / 1000.0));
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

    #[arg(short = 'p', long, default_value = "5")]
    object_priority: u8,
}

fn parse_args() -> (Config, Priority) {
    let args = Args::parse();

    let priority = match args.object_priority {
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

    (args.common.into(), priority)
}

fn send_with_timestamp_with_seq(publisher: &Arc<Publisher<'_>>, data: &[u8], seq: u64) {
    let mut buf = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
        .to_be_bytes()
        .to_vec();

    let seq_bytes = seq.to_be_bytes();
    buf.extend_from_slice(&seq_bytes);
    buf.extend_from_slice(data);
    publisher.put(ZBytes::from(buf)).wait().unwrap();

    println!(
        "{} | seq= {}",
        publisher.key_expr().as_str(),
        seq
    );
}
