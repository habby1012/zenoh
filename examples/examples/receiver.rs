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
use std::time::{SystemTime, UNIX_EPOCH};
use zenoh::{Config, Wait};
use zenoh_examples::CommonArgs;

fn main() {
    // Initialize logging
    zenoh::init_log_from_env_or("error");

    let config = parse_args();
    let session = zenoh::open(config).wait().unwrap();

    let topics = ["data1", "data2", "data3"];
    let mut _subs = vec![];

    for topic in topics {
        let key_expr = topic.to_string();
        let label = key_expr.clone();

        let sub = session
            .declare_subscriber(&key_expr)
            .callback(move |sample| {
                let received_payload = sample.payload();
                let payload_bytes: std::borrow::Cow<'_, [u8]> = received_payload.to_bytes();

                if payload_bytes.len() < 16 {
                    eprintln!("Received data too small on {label}");
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
            })
            .background()
            .wait()
            .unwrap();

        _subs.push(sub);
    }

    std::thread::park();
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

