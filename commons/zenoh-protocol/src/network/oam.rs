//
// Copyright (c) 2022 ZettaScale Technology
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
use crate::common::ZExtBody;

pub type OamId = u16;

pub mod flag {
    pub const T: u8 = 1 << 5; // 0x20 Transport
                              // pub const X: u8 = 1 << 6; // 0x40 Reserved
    pub const Z: u8 = 1 << 7; // 0x80 Extensions    if Z==1 then an extension will follow
}

/// ```text
/// Flags:
/// - E |: Encoding     The encoding of the extension
/// - E/
/// - Z: Extension      If Z==1 then at least one extension is present
///
///  7 6 5 4 3 2 1 0
/// +-+-+-+-+-+-+-+-+
/// |Z|ENC|  OAM    |
/// +-+-+-+---------+
/// ~    id:z16     ~
/// +---------------+
/// ~  [oam_exts]   ~
/// +---------------+
/// %    length     % -- If ENC == u64 || ENC == ZBuf
/// +---------------+
/// ~     [u8]      ~ -- If ENC == ZBuf
/// +---------------+
/// ```
///
/// Encoding:
/// - 0b00: Unit
/// - 0b01: u64
/// - 0b10: ZBuf
/// - 0b11: Reserved
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Oam {
    pub id: OamId,
    pub body: ZExtBody,
    pub ext_qos: ext::QoS,
    pub ext_tstamp: Option<ext::Timestamp>,
}

pub mod ext {
    pub const QOS: u8 = crate::network::ext::QOS;
    pub const TSTAMP: u8 = crate::network::ext::TSTAMP;

    pub type QoS = crate::network::ext::QoS;
    pub type Timestamp = crate::network::ext::Timestamp;
}

impl Oam {
    #[cfg(feature = "test")]
    pub fn rand() -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        let id: OamId = rng.gen();
        let body = ZExtBody::rand();
        let ext_qos = ext::QoS::rand();
        let ext_tstamp = rng.gen_bool(0.5).then(ext::Timestamp::rand);

        Self {
            id,
            body,
            ext_qos,
            ext_tstamp,
        }
    }
}