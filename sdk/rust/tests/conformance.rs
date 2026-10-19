// File: sdk\rust\tests\conformance.rs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-19
// Description: 
// License: MIT

use msmqcache::{Client, Error, Options, QPushOptions, SetOptions, Value};
use std::time::Duration;

fn port(env: &str) -> String {
    format!("127.0.0.1:{}", std::env::var(env).unwrap_or_else(|_| panic!("{env} not set: run through sdk/conformance/run.sh")))
}
fn client() -> Client { Client::connect(&port("MCACHE_TEST_PORT")).with_timeout(Duration::from_secs(2)) }
fn s(v: Option<Vec<u8>>) -> Option<String> { v.map(|b| String::from_utf8(b).unwrap()) }
fn last(c: &mut Client) -> Vec<String> {
    match c.execute(&["LASTCMD"]).unwrap() {
        Value::Array(items) => items.into_iter().map(|v| match v { Value::Bulk(b) => String::from_utf8(b).unwrap(), o => panic!("{o:?}") }).collect(),
        o => panic!("{o:?}"),
    }
}

#[test] fn ping() { assert!(client().ping().unwrap()); }

#[test] fn set_get_delete() {
    let mut c = client();
    assert!(c.set("r:k", "v", &SetOptions::default()).unwrap());
    assert_eq!(s(c.get("r:k").unwrap()), Some("v".into()));
    assert_eq!(c.del(&["r:k", "r:missing"]).unwrap(), 1);
    assert_eq!(c.get("r:k").unwrap(), None);
}

#[test] fn binary_safe() {
    let mut c = client();
    let blob: Vec<u8> = [0u8, 13, 10, b'$', b'5', 13, 10, b'*', b'1', 13, 10, 255].iter().cycle().take(1200).cloned().collect();
    c.set("r:blob", &blob, &SetOptions::default()).unwrap();
    assert_eq!(c.get("r:blob").unwrap(), Some(blob));
}

#[test] fn nil_vs_empty() {
    let mut c = client();
    c.set("r:empty", "", &SetOptions::default()).unwrap();
    assert_eq!(c.get("r:empty").unwrap(), Some(vec![]));
    assert_eq!(c.get("r:never").unwrap(), None);
}

#[test] fn set_options() {
    let mut c = client();
    c.del(&["r:opt"]).unwrap();
    assert!(c.set("r:opt", "1", &SetOptions { ex: Some(30), nx: true, ..Default::default() }).unwrap());
    assert_eq!(last(&mut c), ["SET", "r:opt", "1", "EX", "30", "NX"]);
    assert!(!c.set("r:opt", "2", &SetOptions { nx: true, ..Default::default() }).unwrap());
    assert!(!c.set("r:nokey", "2", &SetOptions { xx: true, ..Default::default() }).unwrap());
    c.set("r:opt", "x", &SetOptions { px: Some(1500), ..Default::default() }).unwrap();
    assert_eq!(&last(&mut c)[3..], ["PX", "1500"]);
    assert!(matches!(c.set("k", "v", &SetOptions { nx: true, xx: true, ..Default::default() }), Err(Error::Invalid(_))));
    assert!(matches!(c.set("", "v", &SetOptions::default()), Err(Error::Invalid(_))));
    assert!(matches!(c.set("k", "v", &SetOptions { ex: Some(0), ..Default::default() }), Err(Error::Invalid(_))));
}

#[test] fn numbers_and_multi() {
    let mut c = client();
    c.del(&["r:cnt"]).unwrap();
    assert_eq!(c.incr("r:cnt").unwrap(), 1);
    assert_eq!(c.incr_by("r:cnt", 9).unwrap(), 10);
    c.mset(&[("rm:1", "a"), ("rm:2", "b")]).unwrap();
    let vals: Vec<_> = c.mget(&["rm:1", "rm:none", "rm:2"]).unwrap().into_iter().map(s).collect();
    assert_eq!(vals, [Some("a".to_string()), None, Some("b".to_string())]);
    let mut keys: Vec<_> = c.keys("rm:*").unwrap().into_iter().map(|k| String::from_utf8(k).unwrap()).collect();
    keys.sort();
    assert_eq!(keys, ["rm:1", "rm:2"]);
}

#[test] fn server_error() {
    let mut c = client();
    c.set("r:str", "abc", &SetOptions::default()).unwrap();
    match c.incr("r:str") { Err(Error::Response(m)) => assert!(m.contains("not an integer"), "{m}"), o => panic!("{o:?}") }
    assert!(matches!(c.execute(&["BOGUS"]), Err(Error::Response(_))));
    assert!(c.ping().unwrap(), "connection must stay usable");
}

#[test] fn auth() {
    let addr = port("MCACHE_TEST_AUTH_PORT");
    let pw = std::env::var("MCACHE_TEST_PASSWORD").unwrap();
    assert!(Client::connect(&addr).with_password(&pw).ping().unwrap());
    assert!(Client::connect(&addr).with_password("wrong").ping().unwrap_err().is_auth());
    assert!(Client::connect(&addr).ping().unwrap_err().is_auth());
}

#[test] fn timeout_reconnect() {
    let mut c = client().with_timeout(Duration::from_millis(300));
    assert!(c.execute(&["SLOW", "1"]).unwrap_err().is_timeout());
    assert!(c.ping().unwrap(), "must work on a fresh connection");
}

#[test] fn dropped_connection() {
    let mut c = client();
    match c.execute(&["DROP"]) { Err(e @ Error::Connection { .. }) => assert!(!e.is_timeout()), o => panic!("{o:?}") }
    assert!(c.ping().unwrap());
}

#[test] fn pipeline() {
    let mut c = client();
    c.del(&["rp:1", "rp:2", "rp:cnt"]).unwrap();
    let res = c.pipeline().set("rp:1", "a", &SetOptions::default()).set("rp:2", "b", &SetOptions::default()).get("rp:1").incr("rp:cnt").exec().unwrap();
    assert_eq!(res[2], Value::Bulk(b"a".to_vec()));
    assert_eq!(res[3], Value::Int(1));
    let one = c.pipeline().get("rp:1").exec().unwrap();
    assert_eq!(one.len(), 1, "single-command pipeline must still return a list");
    let mixed = c.pipeline().set("rp:s", "abc", &SetOptions::default()).incr("rp:s").get("rp:s").exec_lenient().unwrap();
    assert!(matches!(mixed[1], Value::Error(_)));
    assert_eq!(mixed[2], Value::Bulk(b"abc".to_vec()));
    assert!(matches!(c.pipeline().incr("rp:s").exec(), Err(Error::Response(_))));
}

#[test] fn queues() {
    let mut c = client();
    c.qpush("r:jobs", "a", &QPushOptions { priority: Some(5), ttl: Some(60) }).unwrap();
    assert_eq!(last(&mut c), ["QPUSH", "r:jobs", "a", "PRIORITY", "5", "TTL", "60"]);
    assert_eq!(s(c.qpop("r:jobs").unwrap()), Some("a".into()));
    assert_eq!(c.qpop("r:jobs").unwrap(), None);
    assert!(matches!(c.qpush("q", "v", &QPushOptions { priority: Some(8), ttl: None }), Err(Error::Invalid(_))));
}

#[test] fn info() {
    let mut c = Client::new(Options { addr: port("MCACHE_TEST_PORT"), ..Options::default() });
    assert_eq!(c.info().unwrap()["server"]["mcache_version"], "1.0.0");
}
