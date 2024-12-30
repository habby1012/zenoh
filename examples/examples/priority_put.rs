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
    zenoh::init_log_from_env_or("error");
    let args = Args::parse();

    let congestion_control = match args.congestion_control.as_str() {
        "Drop" => CongestionControl::Drop,
        "Block" => CongestionControl::Block,
        _ => {
            eprintln!("Invalid congestion control mode '{}'. Using default: Drop.", args.congestion_control);
            CongestionControl::Drop
        }
    };

    let data_realtime: ZBytes = (0..args.payload_size_realtime)
        .map(|i| (i % 10) as u8)
        .collect::<Vec<u8>>()
        .into();

    let data_data: ZBytes = (0..args.payload_size_data)
        .map(|i| (i % 10) as u8)
        .collect::<Vec<u8>>()
        .into();

    let data_background: ZBytes = (0..args.payload_size_background)
        .map(|i| (i % 10) as u8)
        .collect::<Vec<u8>>()
        .into();

    let session = zenoh::open(args.common).wait().expect("Failed to open session");

    let publisher_realtime = session
        .declare_publisher("test/realtime")
        .congestion_control(congestion_control)
        .priority(Priority::RealTime)
        .express(args.express)
        .wait()
        .map_err(|e| eprintln!("Failed to declare realtime publisher: {:?}", e))
        .expect("Cannot continue without a publisher");

    let publisher_data = session
        .declare_publisher("test/data")
        .congestion_control(congestion_control)
        .priority(Priority::Data)
        .express(args.express)
        .wait()
        .map_err(|e| eprintln!("Failed to declare data publisher: {:?}", e))
        .expect("Cannot continue without a publisher");

    let publisher_background = session
        .declare_publisher("test/background")
        .congestion_control(congestion_control)
        .priority(Priority::Background)
        .express(args.express)
        .wait()
        .map_err(|e| eprintln!("Failed to declare background publisher: {:?}", e))
        .expect("Cannot continue without a publisher");

    let freq_realtime = args.frequency_realtime as f64;
    thread::spawn(move || {
        let mut count_realtime = 0;
        loop {
            if let Err(e) = publisher_realtime.put(data_realtime.clone()).wait() {
                eprintln!("Error sending RealTime message: {:?}", e);
                break;
            }
            count_realtime += 1;
            println!("Realtime messages sent: {}", count_realtime);
            thread::sleep(Duration::from_secs_f64(1.0 / freq_realtime));
        }
    });

    let freq_data = args.frequency_data as f64;
    thread::spawn(move || {
        let mut count_data = 0;
        loop {
            if let Err(e) = publisher_data.put(data_data.clone()).wait() {
                eprintln!("Error sending Data message: {:?}", e);
                break;
            }
            count_data += 1;
            println!("Data messages sent: {}", count_data);
            thread::sleep(Duration::from_secs_f64(1.0 / freq_data));
        }
    });

    let freq_background = args.frequency_background as f64;
    thread::spawn(move || {
        let mut count_background = 0;
        loop {
            if let Err(e) = publisher_background.put(data_background.clone()).wait() {
                eprintln!("Error sending Background message: {:?}", e);
                break;
            }
            count_background += 1;
            println!("Background messages sent: {}", count_background);
            thread::sleep(Duration::from_secs_f64(1.0 / freq_background));
        }
    });

    println!("Press CTRL-C to quit...");
    std::thread::park();
}


#[derive(Parser, Clone, PartialEq, Eq, Hash, Debug)]
struct Args {
    /// Enable express mode
    #[arg(long, default_value = "false")]
    express: bool,
    #[arg(long, default_value = "Block")]
    congestion_control: String,
    /// Payload size for realtime priority messages
    #[arg(long, default_value = "100")]
    payload_size_realtime: usize,
    /// Payload size for data priority messages
    #[arg(long, default_value = "500000")]
    payload_size_data: usize,
    /// Payload size for background priority messages
    #[arg(long, default_value = "500")]
    payload_size_background: usize,
    /// Frequency for realtime priority messages (Hz)
    #[arg(long, default_value = "20")]
    frequency_realtime: usize,
    /// Frequency for data priority messages (Hz)
    #[arg(long, default_value = "20")]
    frequency_data: usize,
    /// Frequency for background priority messages (Hz)
    #[arg(long, default_value = "20")]
    frequency_background: usize,
    #[command(flatten)]
    common: CommonArgs,
}
