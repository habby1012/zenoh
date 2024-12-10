use core::time;
//
// Copyright (c) 2023 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use zenoh::{bytes::ZBytes, qos::CongestionControl, qos::Priority, Config, Wait};
use zenoh_examples::CommonArgs;

fn main() {
    // initiate logging
    zenoh::init_log_from_env_or("error");

    let (config, size, n, express) = parse_args();
    let session = zenoh::open(config).wait().unwrap();

    let topic = "data_realtime";
    let pong_topic = format!("{}pong", topic);
    let key_expr_ping = topic;
    let key_expr_pong = pong_topic;

    let sub = session.declare_subscriber(&key_expr_pong).wait().unwrap();
    let publisher = session
        .declare_publisher(key_expr_ping)
        .congestion_control(CongestionControl::Block)
        .priority(Priority::RealTime)
        .express(express)
        .wait()
        .unwrap();

    // Create dummy data of user-defined size
    let dummy_data: Vec<u8> = (0..size - 16)
        .map(|i| (i % 256) as u8)
        .collect();

    let mut samples = Vec::with_capacity(n);

    for i in 0..n {
        let sent_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros()
            .to_be_bytes();
        let mut data = sent_timestamp.to_vec();
        data.extend_from_slice(&dummy_data);
        publisher.put(ZBytes::from(data)).wait().unwrap();
        
        if let Ok(sample) = sub.recv() {
            let received_payload = sample.payload();
            let payload_bytes: std::borrow::Cow<'_, [u8]> = received_payload.to_bytes();
                
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
                "Message {}: Received message with original timestamp: {:?}, Time difference: {:?}µs",
                    i + 1,
                    original_timestamp,
                    time_difference,
            );
            
            samples.push(time_difference);
        }
        else{
            println!("Failed to receive response for message");
        }
    }

    for (i, rtt) in samples.iter().enumerate().take(n) {
        println!(
            "{} bytes: seq={} rtt={:?}µs lat={:?}µs",
            size,
            i,
            rtt,
            rtt / 2
        );
    }
} 


#[derive(Parser)]
struct Args {
    #[arg(short = 'n', long, default_value = "100")]
    /// The number of round-trips to measure
    samples: usize,
    #[arg(short = 's', long, default_value = "128")]
    /// Sets the size of the payload to publish
    payload_size: usize,
    #[arg(long, default_value = "false")]
    /// Enable express mode
    express: bool,
    #[command(flatten)]
    common: CommonArgs,
}

fn parse_args() -> (Config, usize, usize, bool) {
    let args = Args::parse();
    (
        args.common.into(),
        args.payload_size,
        args.samples,
        args.express,
    )
}
