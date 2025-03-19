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
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;
use zenoh::{bytes::ZBytes, qos::CongestionControl, qos::Priority, Config, Wait};
use zenoh_examples::CommonArgs;

fn main() {
    // initiate logging
    zenoh::init_log_from_env_or("error");

    let (config, size, n, express, priority) = parse_args();
    let session = zenoh::open(config).wait().unwrap();

    let topic = "data";
    let key_expr_ping = topic;

    let publisher = session
        .declare_publisher(key_expr_ping)
        .congestion_control(CongestionControl::Block)
        .priority(priority)
        .express(express)
        .wait()
        .unwrap();

    // Create dummy data of user-defined size
    let dummy_data: Vec<u8> = (0..size - 16)
        .map(|i| (i % 256) as u8)
        .collect();

    for i in 0..n {
        let sent_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros()
            .to_be_bytes();

        let mut data = sent_timestamp.to_vec();
        data.extend_from_slice(&dummy_data);

        publisher.put(ZBytes::from(data)).wait().unwrap();

        println!("Sent message {} with priority {:?}", i + 1, priority);
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

    #[arg(short = 'p', long, default_value = "5")]
    /// Set the priority level (1=RealTime, 2=InteractiveHigh, 3=InteractiveLow, 4=DataHigh, 5=Data, 6=DataLow, 7=Background)
    priority: u8,

    #[command(flatten)]
    common: CommonArgs,
}

fn parse_args() -> (Config, usize, usize, bool, Priority) {
    let args = Args::parse();

    let priority = match args.priority {
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

    (
        args.common.into(),
        args.payload_size,
        args.samples,
        args.express,
        priority,
    )
}
