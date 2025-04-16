use clap::Parser;
use std::time::{SystemTime, UNIX_EPOCH};
use zenoh::{Config, Wait};
use zenoh_examples::CommonArgs;

fn main() {
    // Initialize logging
    zenoh::init_log_from_env_or("error");

    let config = parse_args();
    let session = zenoh::open(config).wait().unwrap();

    // Subscribe to "data1"
    let _sub_data1 = session
        .declare_subscriber("data1")
        .callback(|sample| {
            handle_sample("data1", sample.payload().to_bytes());
        })
        .background()
        .wait()
        .unwrap();

    // Subscribe to "data2"
    let _sub_data2 = session
        .declare_subscriber("data2")
        .callback(|sample| {
            handle_sample("data2", sample.payload().to_bytes());
        })
        .background()
        .wait()
        .unwrap();

    // Subscribe to "data3"
    let _sub_data3 = session
        .declare_subscriber("data3")
        .callback(|sample| {
            handle_sample("data3", sample.payload().to_bytes());
        })
        .background()
        .wait()
        .unwrap();

    std::thread::park();
}

fn handle_sample(label: &str, payload_bytes: std::borrow::Cow<'_, [u8]>) {
    if payload_bytes.len() < 16 {
        eprintln!("Received data too small on {}", label);
        return;
    }

    let original_timestamp_bytes: [u8; 16] = payload_bytes[0..16]
        .try_into()
        .expect("Invalid timestamp length");

    let original_timestamp = u128::from_be_bytes(original_timestamp_bytes);

    let current_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();

    let time_difference = current_timestamp - original_timestamp;

    println!(
        "[{}] Received: sent at {}µs, now {}µs, Δ = {}µs",
        label, original_timestamp, current_timestamp, time_difference,
    );
}

#[derive(clap::Parser, Clone, PartialEq, Eq, Hash, Debug)]
struct Args {
    #[command(flatten)]
    common: CommonArgs,
}

fn parse_args() -> Config {
    let args = Args::parse();
    args.common.into()
}

