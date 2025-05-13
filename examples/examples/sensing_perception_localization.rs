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
    let mut ex_pub = ReaderBuilder::new().has_headers(false).from_path("config/EX_sensing_perception_localization.csv").unwrap();
    for result in ex_pub.records() {
        let rec = result.unwrap();
        let topic_string = rec.get(0).unwrap().trim().to_string();
        let topic_static: &'static str = Box::leak(topic_string.into_boxed_str());
        let criticality = rec.get(1).unwrap().trim();
        let freq: f64 = rec.get(3).unwrap().trim().parse().unwrap_or(10.0);
        let size: usize = parse_byte_size(rec.get(4).unwrap());

        let priority = match criticality {
            "Critical" => Priority::RealTime,
            "Time-Critical-Small" => Priority::InteractiveHigh,
            "Time-Critical-Large" => {
                let index = TCLA_ROTATE_INDEX.fetch_add(1, Ordering::Relaxed) % TCLA_PRIORITIES.len();
                TCLA_PRIORITIES[index]
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
            loop {
                let size_jitter_ratio = 0.1; // ±10%
                let jitter_bytes = (size as f64 * size_jitter_ratio) as isize;
                let delta = rng.gen_range(-jitter_bytes..=jitter_bytes);
                let adjusted_size = (size as isize + delta).max(16) as usize;

                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros();
                let mut payload = now.to_be_bytes().to_vec();
                payload.extend_from_slice(&vec![0u8; adjusted_size.saturating_sub(16)]);

                pub_clone.put(ZBytes::from(payload)).wait().unwrap();

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
    let sub_files = vec![
        "config/EX_control_planning.csv",
        "config/EX_vehicle_system.csv"
    ];
    let this_module = "Sensing/Perception/Localization";

    let mut subscribers = Vec::new();

    for file in sub_files {
        let mut reader = ReaderBuilder::new().has_headers(false).from_path(file).unwrap();
        for result in reader.records() {
            let rec = result.unwrap();
            let topic = rec.get(0).unwrap().trim().to_string();
            let target_modules = rec.get(2).unwrap_or(&"").trim();
            
            if target_modules.contains(this_module) {
                let topic_clone = topic.clone();

                let subscriber = session
                    .declare_subscriber(keyexpr::new(&topic).unwrap())
                    .callback(move |sample| {
                        let payload_buf = sample.payload().to_bytes();
                        let payload = payload_buf.as_ref();
                        if payload.len() < 16 { return; }
                        let ts = u128::from_be_bytes(payload[..16].try_into().unwrap());
                        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros();
                        println!("[{}] latency: {} µs", topic_clone, now - ts);
                    })
                    .wait()
                    .unwrap();

                subscribers.push(subscriber);
            }
        }
    }

    loop { thread::sleep(Duration::from_secs(60)); }
}
