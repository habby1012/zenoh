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
pub const LIST_SEPARATOR: char = ';';
pub const FIELD_SEPARATOR: char = '=';
pub const VALUE_SEPARATOR: char = '|';

fn split_once(s: &str, c: char) -> (&str, &str) {
    match s.find(c) {
        Some(index) => {
            let (l, r) = s.split_at(index);
            (l, &r[1..])
        }
        None => (s, ""),
    }
}

// tcp/localhost:7557?mymetadata=asdasd#myconfig=asdasd;asdasd=1;asdijabdiasd=1a

/// Parameters provides an `HashMap<&str, &str>`-like view over a `&str` when `&str` follows the format `a=b;c=d|e;f=g`
pub struct Parameters;

impl Parameters {
    pub fn iter(s: &str) -> impl DoubleEndedIterator<Item = (&str, &str)> {
        s.split(LIST_SEPARATOR).filter_map(|prop| {
            if prop.is_empty() {
                None
            } else {
                Some(split_once(prop, FIELD_SEPARATOR))
            }
        })
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_iter<'s, I>(iter: I) -> String
    where
        I: Iterator<Item = (&'s str, &'s str)>,
    {
        let mut into = String::new();
        Self::from_iter_into(iter, &mut into);
        into
    }

    pub fn from_iter_into<'s, I>(iter: I, into: &mut String)
    where
        I: Iterator<Item = (&'s str, &'s str)>,
    {
        let mut from = iter.collect::<Vec<(&str, &str)>>();
        from.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        Self::extend_into(from.iter().copied(), into);
    }

    pub fn get<'s>(s: &'s str, k: &str) -> Option<&'s str> {
        Self::iter(s).find(|x| x.0 == k).map(|x| x.1)
    }

    pub fn values<'s>(s: &'s str, k: &str) -> impl DoubleEndedIterator<Item = &'s str> {
        match Self::get(s, k) {
            Some(v) => v.split(VALUE_SEPARATOR),
            None => {
                let mut i = "".split(VALUE_SEPARATOR);
                i.next();
                i
            }
        }
    }

    pub fn insert<'s, I>(mut iter: I, k: &'s str, v: &'s str) -> (String, Option<&'s str>)
    where
        I: Iterator<Item = (&'s str, &'s str)>,
    {
        let item = iter.find(|(key, _)| *key == k).map(|(_, v)| v);

        let current = iter.filter(|x| x.0 != k);
        let new = Some((k, v)).into_iter();
        let iter = current.chain(new);
        (Parameters::concat(iter), item)
    }

    pub fn remove<'s, I>(mut iter: I, k: &'s str) -> (String, Option<&'s str>)
    where
        I: Iterator<Item = (&'s str, &'s str)>,
    {
        let item = iter.find(|(key, _)| *key == k).map(|(_, v)| v);
        let iter = iter.filter(|x| x.0 != k);
        (Parameters::concat(iter), item)
    }

    pub fn concat<'s, I>(iter: I) -> String
    where
        I: Iterator<Item = (&'s str, &'s str)>,
    {
        let mut into = String::new();
        Parameters::extend_into(iter, &mut into);
        into
    }

    pub fn extend_into<'s, I>(iter: I, into: &mut String)
    where
        I: Iterator<Item = (&'s str, &'s str)>,
    {
        let mut first = into.is_empty();
        for (k, v) in iter {
            if !first {
                into.push(LIST_SEPARATOR);
            }
            into.push_str(k);
            if !v.is_empty() {
                into.push(FIELD_SEPARATOR);
                into.push_str(v);
            }
            first = false;
        }
    }

    pub fn is_sorted<'s, I>(iter: I) -> bool
    where
        I: Iterator<Item = (&'s str, &'s str)>,
    {
        let mut prev = None;
        for (k, _) in iter {
            match prev.take() {
                Some(p) if k < p => return false,
                _ => prev = Some(k),
            }
        }
        true
    }

    #[cfg(feature = "test")]
    pub fn rand(into: &mut String) {
        use rand::{
            distributions::{Alphanumeric, DistString},
            Rng,
        };

        const MIN: usize = 2;
        const MAX: usize = 8;

        let mut rng = rand::thread_rng();

        let num = rng.gen_range(MIN..MAX);
        for i in 0..num {
            if i != 0 {
                into.push(LIST_SEPARATOR);
            }
            let len = rng.gen_range(MIN..MAX);
            let key = Alphanumeric.sample_string(&mut rng, len);
            into.push_str(key.as_str());

            into.push(FIELD_SEPARATOR);

            let len = rng.gen_range(MIN..MAX);
            let value = Alphanumeric.sample_string(&mut rng, len);
            into.push_str(value.as_str());
        }
    }
}