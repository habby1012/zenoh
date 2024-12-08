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

use std::convert::TryInto;
use std::thread;
use std::time::Duration;

use clap::Parser;
use zenoh::{
    bytes::ZBytes,
    qos::{CongestionControl, Priority},
    Wait,
};
use zenoh_examples::CommonArgs;

fn main() {
    // initiate logging
    zenoh::init_log_from_env_or("error");
    let args = Args::parse();

    let mut prio_realtime = Priority::RealTime;
    let mut prio_data = Priority::Data;
    let mut prio_background = Priority::Background;

    let data_realtime: ZBytes = (0..100)
        .map(|i| (i % 10) as u8)
        .collect::<Vec<u8>>()
        .into();

    let data_data: ZBytes = (0..500000)
        .map(|i| (i % 10) as u8)
        .collect::<Vec<u8>>()
        .into();

    let data_background: ZBytes = (0..500)
        .map(|i| (i % 10) as u8)
        .collect::<Vec<u8>>()
        .into();

    let session = zenoh::open(args.common).wait().unwrap();

    let publisher_realtime = session
        .declare_publisher("test/realtime")
        .congestion_control(CongestionControl::Drop)
        .priority(prio_realtime)
        .express(args.express)
        .wait()
        .unwrap();

    let publisher_data = session
        .declare_publisher("test/data")
        .congestion_control(CongestionControl::Drop)
        .priority(prio_data)
        .express(args.express)
        .wait()
        .unwrap();

    let publisher_background = session
        .declare_publisher("test/background")
        .congestion_control(CongestionControl::Drop)
        .priority(prio_background)
        .express(args.express)
        .wait()
        .unwrap();

    println!("Press CTRL-C to quit...");
    let mut count: usize = 0;
    let mut start = std::time::Instant::now();

    let handle_realtime = thread::spawn(move || {
        loop {
            publisher_realtime.put(data_realtime.clone()).wait().unwrap();
            println!("put data_realtime");
            thread::sleep(Duration::from_secs_f64(1.0 / 30.0));
        }
    });

    let handle_data = thread::spawn(move || {
        loop {
            publisher_data.put(data_data.clone()).wait().unwrap();
            println!("put data_data");
            thread::sleep(Duration::from_secs_f64(1.0 / 20.0));
        }
    });

    let handle_background = thread::spawn(move || {
        loop {
            publisher_background.put(data_background.clone()).wait().unwrap();
            println!("put data_background");
            thread::sleep(Duration::from_secs_f64(1.0 / 20.0));
        }
    });

    loop {
        if args.print {
            if count < args.number {
                count += 1;
            } else {
                let thpt = count as f64 / start.elapsed().as_secs_f64();
                println!("{thpt} msg/s");
                count = 0;
                start = std::time::Instant::now();
            }
        }
    }
}

#[derive(Parser, Clone, PartialEq, Eq, Hash, Debug)]
struct Args {
    /// express for sending data
    #[arg(long, default_value = "false")]
    express: bool,
    /// Priority for sending data
    #[arg(short, long)]
    priority: Option<u8>,
    /// Print the statistics
    #[arg(short = 't', long)]
    print: bool,
    /// Number of messages in each throughput measurements
    #[arg(short, long, default_value = "100000")]
    number: usize,
    /// Sets the size of the payload to publish
    #[arg(long)]
    payload_size: Option<usize>,
    #[command(flatten)]
    common: CommonArgs,
}
