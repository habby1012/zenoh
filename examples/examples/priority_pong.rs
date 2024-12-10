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
use clap::Parser;
use zenoh::{
    key_expr::keyexpr,
    qos::{CongestionControl, Priority},
    Config, Wait,
};
use zenoh_examples::CommonArgs;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    // Initialize logging
    zenoh::init_log_from_env_or("error");

    let (config, express) = parse_args();
    let session = zenoh::open(config).wait().unwrap();

    let topic = "data_realtime";
    let pong_topic = format!("{}pong", topic);
    let pong_topic_static: &'static str = Box::leak(pong_topic.into_boxed_str());
    let key_expr_ping = keyexpr::new(topic).unwrap();
    let key_expr_pong = keyexpr::new(pong_topic_static).unwrap();

    // Declare publisher for pong messages
    let publisher = session
        .declare_publisher(key_expr_pong)
        .congestion_control(CongestionControl::Block)
        .priority(Priority::RealTime)
        .express(express)
        .wait()
        .unwrap();

    // Declare subscriber for ping messages and respond with pong
    session
        .declare_subscriber(key_expr_ping)
        .callback(move |sample| {
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
                "Received message with original timestamp: {:?}µs, Time difference: {:?}µs",
                original_timestamp, time_difference,
            );

            publisher.put(received_payload.clone()).wait().unwrap();
        })
        .background()
        .wait()
        .unwrap();

    // Park the thread to keep the process running
    std::thread::park();
}

#[derive(clap::Parser, Clone, PartialEq, Eq, Hash, Debug)]
struct Args {
    /// express for sending data
    #[arg(long, default_value = "false")]
    no_express: bool,
    #[command(flatten)]
    common: CommonArgs,
}

fn parse_args() -> (Config, bool) {
    let args = Args::parse();
    (args.common.into(), !args.no_express)
}
