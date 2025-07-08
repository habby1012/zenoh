use csv::ReaderBuilder;
use clap::Parser;
use zenoh::{
    bytes::ZBytes,
    key_expr::keyexpr,
    qos::{CongestionControl, Priority},
    Config, Wait,
};
use zenoh_examples::CommonArgs;
use std::{thread, time::Duration};
use std::time::{SystemTime, UNIX_EPOCH};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use rand::Rng;

static TCLA_ROTATE_INDEX: AtomicUsize = AtomicUsize::new(0);
const TCLA_PRIORITIES: [Priority; 4] = [
    Priority::InteractiveLow,
    Priority::DataHigh,
    Priority::Data,
    Priority::DataLow,
];

fn parse_byte_size(s: &str) -> usize {
    let s = s.trim().to_uppercase();
    if let Some(v) = s.strip_suffix("K") {
        (v.trim().parse::<f64>().unwrap_or(0.0) * 1024.0) as usize
    } else if let Some(v) = s.strip_suffix("M") {
        (v.trim().parse::<f64>().unwrap_or(0.0) * 1024.0 * 1024.0) as usize
    } else {
        s.trim().parse::<usize>().unwrap_or(0)
    }
}

#[derive(Parser, Clone, Debug)]
struct Args {
    #[command(flatten)]
    common: CommonArgs,
}

fn parse_args() -> Config {
    let args = Args::parse();
    args.common.into()
}

fn main() {
    zenoh::init_log_from_env_or("error");
    let config = parse_args();
    let session = zenoh::open(config).wait().unwrap();

    // ---------------------- Load and spawn publishers ----------------------
    let mut ex_pub = ReaderBuilder::new().has_headers(false).from_path("config/INTER_carla_one_camera.csv").unwrap();
    for result in ex_pub.records() {
        let rec = result.unwrap();
        let topic_string = rec.get(0).unwrap().trim().to_string();

        if !(topic_string.starts_with("sensing") || topic_string.starts_with("perception") || topic_string.starts_with("localization")) {
            continue;
        }

        let topic_static: &'static str = Box::leak(topic_string.into_boxed_str());
        let criticality = rec.get(2).unwrap().trim();
        let freq: f64 = rec.get(3).unwrap().trim().parse().unwrap_or(10.0);
        if freq == 0.0 {
           continue;
        }
        let size: usize = parse_byte_size(rec.get(4).unwrap());

        let priority = match criticality {
            "Critical" => Priority::RealTime,
            "Sensor" => {
                if topic_static == "sensing/camera/main/image_raw" {
                    Priority::InteractiveHigh
                } else {
                    let index = TCLA_ROTATE_INDEX.fetch_add(1, Ordering::Relaxed) % TCLA_PRIORITIES.len();
                    TCLA_PRIORITIES[index]
                }
            },
            _ => Priority::Background,
        };

        let pub_decl = session
            .declare_publisher(keyexpr::new(topic_static).unwrap())
            .congestion_control(CongestionControl::Drop)
            .priority(priority)
            .wait()
            .unwrap();

        let pub_arc = Arc::new(pub_decl);
        let pub_clone = pub_arc.clone();
        thread::spawn(move || {
            let mut rng = rand::thread_rng();
            let mut pkt_counter: u64 = 1;
            loop {
                // Set data size
                let size_jitter_ratio = 0.1; // ±10%
                let jitter_bytes = (size as f64 * size_jitter_ratio) as isize;
                let delta = rng.gen_range(-jitter_bytes..=jitter_bytes);
                let adjusted_size = (size as isize + delta).max(16) as usize;

                // Get time
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros();

                // First 8 bytes: packet sequence, then 16 bytes timestamp
                let mut payload = pkt_counter.to_be_bytes().to_vec();
                payload.extend_from_slice(&now.to_be_bytes());
                payload.extend_from_slice(&vec![0u8; adjusted_size.saturating_sub(24)]);

                pub_clone.put(ZBytes::from(payload)).wait().unwrap();

                // Add packet counter
                pkt_counter += 1;

                // Set sleep time
                let base_sleep = 1.0 / freq;
                let jitter_ratio = 0.2;
                let jitter_range = base_sleep * jitter_ratio;
                let jitter_s: f64 = rng.gen_range(-jitter_range..=jitter_range);
                let sleep_duration = Duration::from_secs_f64((base_sleep + jitter_s).max(0.0));
                thread::sleep(sleep_duration);
            }
        });
    }

    // ---------------------- Load and spawn subscribers ----------------------
    let sub_file = "config/INTER_carla_one_camera.csv";
    let mut reader = ReaderBuilder::new().has_headers(false).from_path(sub_file).unwrap();
    let mut subscribers = Vec::new();

    for result in reader.records() {
        let rec = result.unwrap();
        let module_type = rec.get(1).unwrap().trim();

        if !module_type.eq_ignore_ascii_case("Sensing/Perception/Localization") {
            continue;
        }

        let topic = rec.get(0).unwrap().trim().to_string();
        let topic_clone = topic.clone();
        let subscriber = session
            .declare_subscriber(keyexpr::new(&topic).unwrap())
            .callback(move |sample| {
                let payload_buf = sample.payload().to_bytes();
                let payload = payload_buf.as_ref();
                if payload.len() < 24 { return; }
                let pkt_id = u64::from_be_bytes(payload[..8].try_into().unwrap());
                let ts = u128::from_be_bytes(payload[8..24].try_into().unwrap());
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros();
                println!("[{}] packet_id: {}  latency: {} µs", topic_clone, pkt_id, now - ts);
            })
            .wait()
            .unwrap();

        subscribers.push(subscriber);
    }

    loop { thread::sleep(Duration::from_secs(60)); }
}
