extern crate ws;
extern crate serde;
extern crate serde_json;
extern crate json;
extern crate clap;
extern crate ansi_term;
extern crate futures;
extern crate hyper;
extern crate tokio_core;
extern crate reqwest;

#[macro_use]
extern crate serde_derive;
extern crate chrono;


use std::sync::{Arc, Mutex};
use std::rc::Rc;
use std::cell::Cell;
use futures::{Future, Stream};
use std::collections::HashMap;
use std::fs::File;
use ws::{connect, Handler, Sender, Handshake, Result, Message, CloseCode, Response};
use std::thread;
use chrono::prelude::*;
use chrono::{DateTime, TimeZone, NaiveDateTime, Utc};

static WRITE_ON_DISK: bool = false;
static SEND_TO_DB: bool = true;

mod broker {
    pub fn get_url(pair: String) -> String {
        let mut s = "wss://stream.binance.com:9443/ws/".to_owned();
        let pairl = pair.to_lowercase();
        s.push_str(&pairl);
        s.push_str("@kline_1m");
        s
    }

    #[derive(Serialize, Deserialize)]
    pub struct RawTick {
        B: String,
        L: i64,
        Q: String,
        T: i64,
        V: String,
        pub c: String,
        f: i64,
        pub h: String,
        i: String,
        pub l: String,
        n: i64,
        pub o: String,
        q: String,
        s: String,
        pub t: i64,
        pub v: String,
        pub x: bool,
    }

    #[derive(Serialize, Deserialize)]
    pub struct ParsedBrokerMessage {
        E: i64,
        e: String,
        pub k: RawTick,
        pub  s: String,
    }

    impl ParsedBrokerMessage {
        pub fn get_tick(&self) -> ::GenericTick {
            ::GenericTick {
                ts: self.k.t,
                p: super::parsef64(&self.k.c),
                v: super::parsef64(&self.k.v),
            }
        }
        pub fn get_generic_OHLC(&self) -> ::GenericOHLC {
            ::GenericOHLC {
                ts: self.k.t,
                o: super::parsef64(&self.k.o),
                h: super::parsef64(&self.k.h),
                l: super::parsef64(&self.k.l),
                c: super::parsef64(&self.k.c),
                v: super::parsef64(&self.k.v),
            }
        }
    }
}

fn parsei64(i: &String) -> i64 {
    i.parse::<i64>().unwrap()
}

fn parsef64(i: &String) -> f64 {
    i.parse::<f64>().unwrap()
}

fn concat(a: &str, b: &str) -> String {
    let mut owned_str: String = "".to_owned();
    owned_str.push_str(a);
    owned_str.push_str(b);
    owned_str
}

pub struct GenericTick {
    ts: i64,
    p: f64,
    v: f64,
}

impl GenericTick {
    fn to_string(&self) -> String {
        let mut owned_str: String = "".to_owned();
        owned_str.push_str(&(self.ts.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.p.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.v.to_string().to_owned()));
        owned_str.push_str("\n");
        owned_str
    }
    fn to_json(&self, pair: &str) -> String {
        let ts = chrono::Utc.timestamp(self.ts / 1000, 0).format("%Y-%m-%d %H:%M:%S");
        let s = format!(r#"{{"ts" :"{}","pair"  :"{}","close" :"{}","volume":"{}"}}"#, ts, pair, self.p, self.v);
        s
    }
}

pub struct StringGenericOHLC {
    ts: i64,
    o: String,
    h: String,
    c: String,
    l: String,
    v: String,
}

impl StringGenericOHLC {
    fn to_json(&self, pair: &str) -> String {
        let ts = chrono::Utc.timestamp(self.ts / 1000, 0).format("%Y-%m-%d %H:%M:%S");
        let s = format!(r#"{{"ts" :"{}","pair"  :"{}","open"  :"{}","high"  :"{}","low":"{}","close":"{}","volume":"{}"}}"#, ts, pair, self.o, self.h, self.l, self.c, self.v);
        s
    }
    fn to_string(&self) -> String {
        let mut owned_str: String = "".to_owned();
        owned_str.push_str(&(self.ts.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.o.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.h.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.l.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.c.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.v.to_string().to_owned()));
        owned_str.push_str("\n");
        owned_str
    }
}


pub struct GenericOHLC {
    ts: i64,
    o: f64,
    h: f64,
    c: f64,
    l: f64,
    v: f64,
}

impl GenericOHLC {
    fn to_json(&self, broker: &str, pair: &str) -> String {
        let ts = chrono::Utc.timestamp(self.ts / 1000, 0).format("%Y-%m-%d %H:%M:%S");
        let s = format!(r#"{{"ts" :"{}","pair"  :"{}","open"  :"{}","high"  :"{}","low":"{}","close":"{}","volume":"{}"}}"#, ts, pair, self.o, self.h, self.l, self.c, self.v);
        s
    }
    fn to_string(&self) -> String {
        let mut owned_str: String = "".to_owned();
        owned_str.push_str(&(self.ts.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.o.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.h.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.l.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.c.to_string()).to_owned());
        owned_str.push_str(",");
        owned_str.push_str(&(self.v.to_string().to_owned()));
        owned_str.push_str("\n");
        owned_str
    }
}

pub struct Client {
    out: Sender,
    //writer: Option<BufWriter<File>>,
    name: String,
    broker: String,

    buffer_level: u8,
    buffer_max: u8,
    textbuffer: String,

    ohlc_1m_buffer_level: u8,
    ohlc_1m_buffer_max: u8,
    ohlc_1m_textbuffer: String,

    client: reqwest::Client,
    last_today_str: String,
    path_tick: String,

    current_ts: i64,
    old_ts: i64,

}

impl Client {
    fn save_tick(&mut self, tick: &GenericTick) {
        self.write_tick_in_buffer(tick);

        let mut do_send = false;
        self.buffer_level = self.buffer_level + 1;
        if self.is_buffer_full() {
            do_send = true;
            self.buffer_level = 0;
        } else {
            self.textbuffer = concat(&self.textbuffer, ",");
        }
        if do_send {
            let json = concat(&self.textbuffer, &"]".to_string());
            let uri = format!("http://0.0.0.0:3000/{}_tick", self.broker);
            //println!("[POST] ticks {}", json);
            if let Ok(mut res) = self.client.post(&uri).body(json).send() {
                println!("[{}] [POST] {}_tick {} {}", self.name, self.broker, res.status(), res.text().unwrap());
                self.textbuffer = "[".to_string();
            } else {
                println!("[{}] [POST] nok uri", self.name);
            }
        }
    }

    fn is_buffer_full(&self) -> bool {
        self.buffer_level >= self.buffer_max
    }

    fn save_1m(&mut self, mut ohlc: &GenericOHLC) {
        if ohlc.ts != self.current_ts {
            println!("[{}] ohlc_1m {} {} {}", self.name, ohlc.ts, self.current_ts, ohlc.ts - self.current_ts);
            let diff = ohlc.ts - self.current_ts;
            if diff == 60000 || self.current_ts == 0 {} else {
                println!("  !!! Missing tick !!! ")
            }
            self.old_ts = self.current_ts;
            self.current_ts = ohlc.ts;
        } else {
            println!("same tick")
        }

        //self.ohlc_1m_textbuffer = concat(&self.ohlc_1m_textbuffer, &ohlc.to_json(&self.broker, &self.name));


        //send request post
        let json = ohlc.to_json("BIN", &self.name);
        let uri = format!("http://0.0.0.0:3000/{}_ohlc_1m", self.broker);
        if let Ok(mut res) = self.client.post(&uri).body(json).send() {
            println!("[{}] [POST] {}_ohlc_1m {} {}", self.name, self.broker, res.status(), res.text().unwrap());
        } else {
            println!("[{}] [POST] nok uri", self.name);
        }
    }

    fn write_tick_in_buffer(&mut self, tick: &GenericTick) {
        self.textbuffer = concat(&self.textbuffer, &tick.to_json(&self.name));
    }
}

impl Handler for Client {
    fn on_open(&mut self, _: Handshake) -> Result<()> {
        println!("[{}] Open ws", self.name);
        self.out.send("Hello WebSocket")
    }
    fn on_error(&mut self, err: ws::Error) {
        println!("err kind{:?}", err.kind);
        println!("err {}", err);
    }
    fn on_response(&mut self, _res: &Response) -> Result<()> {
        self.out.send("Hello WebSocket res")
    }
    fn on_timeout(&mut self, event: ws::util::Token) -> Result<()> {
        println!("timeout ");
        Ok(())
    }
    fn on_shutdown(&mut self) {
        println!("shutdown");
    }
    fn on_message(&mut self, msg: Message) -> Result<()> {
        let m = msg.as_text().unwrap();
        let mut v: broker::ParsedBrokerMessage = serde_json::from_str(m).unwrap();
        let tick = v.get_tick();
        self.save_tick(&tick);
        if v.k.x {//isfinal
            let mut ohlc = v.get_generic_OHLC();
            self.save_1m(&ohlc);
        }
        self.out.close(CloseCode::Normal)
    }
}

fn getPairsFromArgs() -> Vec<Pair> {
    let args: Vec<String> = std::env::args().collect();
    let mut pairs: String;
    if let Ok(val) = std::env::var("PAIRS") {
        pairs = val.to_owned();
    } else {
        pairs = "bin:ETHUSDT".to_string()
    }
    println!("ENV PAIRS {}", pairs);

    let mut PAIRS: Vec<Pair> = Vec::new();
    let pairssp: Vec<&str> = pairs.split(",").collect();
    for p in &pairssp {
        let ppp: Vec<&str> = p.split(":").collect();
        if ppp.len() != 2 { println!("wrong format {}", p); }
        PAIRS.push(Pair { name: ppp[1].to_string(), broker: ppp[0].to_string() })
    }
    PAIRS
}

struct Pair {
    name: String,
    broker: String,
}

fn main() {
    println!("Coinamics Server Websockets");
    let mut children = vec![];
    let PAIRS = getPairsFromArgs();
    let nb = PAIRS.len();
    println!("Loading {} pairs", nb);
    let mut c: HashMap<String, usize> = HashMap::new();//Vec::with_capacity(nb);//(0..nb).collect();
    for i in 0..nb {
        c.insert(PAIRS[i].name.to_string(), 0);
    }


    let mut v: Vec<usize> = Vec::with_capacity(nb);//(0..nb).collect();
    for i in 0..nb {
        v.push(0);
    }
    println!("{:?}", c);
    let data = Arc::new(Mutex::new(c));
    let count = Rc::new(Cell::new(0));
    let client_data = v.clone();

    println!("Starting pair threads");
    for p in PAIRS.iter() {
        println!("[{}/{}] Starting thread", p.broker, p.name);
        let pp = p.name.clone();
        let bb = p.broker.clone();
        let data_inner = data.clone();
        children.push(thread::spawn(move || {
            let url = broker::get_url(pp.to_string());
            let client = reqwest::Client::new();
            let mut lastTs=0;
            println!("[{}/{}] Launch connection {}", bb.to_string(), pp.to_string(), url);

            let mut result: Vec<StringGenericOHLC> = Vec::new();
            if (bb == "bin") {
                let uri = format!("https://api.binance.com/api/v1/klines?symbol={}&interval={}", pp.to_string(), "1m");
                if let Ok(mut res) = client.get(&uri).send() {
                    println!("[{}] [GET] {}_ohlc ", pp.to_string(), res.status());
                    let restext = res.text().unwrap();

                    let res1 = &restext[2..restext.len() - 2];
                    println!("row {}", res1);

                    let resspl: Vec<&str> = res1.split("],[").collect();
                    for row in resspl {
                        if row.len() > 1 {
                            let res21: &str = &row[0..row.len()];
                            println!("  {}", res21);
                            let r: Vec<&str> = res21.split(",").collect();
                            let oo = r[1];
                            let o = oo[1..oo.len() - 1].to_string();
                            let h = r[2][1..r[2].len() - 1].to_string();
                            let l = r[3][1..r[3].len() - 1].to_string();
                            let c = r[4][1..r[4].len() - 1].to_string();
                            let v = r[5][1..r[5].len() - 1].to_string();
                            let ohlc: StringGenericOHLC = StringGenericOHLC {
                                ts: parsei64(&r[0].to_string()),
                                o: o,
                                h: h,
                                l: l,
                                c: c,
                                v: v,
                            };
                            println!("    -> {}", ohlc.to_string());
                            if ohlc.ts != lastTs {
                                lastTs = ohlc.ts;

                                let json = ohlc.to_json(&pp.to_string());
                                result.push(ohlc);


                                let uri = format!("http://0.0.0.0:3000/{}_ohlc_1m", bb.to_string());
                                if let Ok(mut res) = client.post(&uri).body(json).send() {
                                    println!("[{}] [POST] {}_ohlc_1m {} {}", pp.to_string(), bb.to_string(), res.status(), res.text().unwrap());
                                } else {
                                    println!("[{}] [POST] nok uri", pp.to_string());
                                }
                            }
                        } else {
                            println!("  err row {}", row);
                        }
                    }
                } else {
                    println!("[{}] [GET] nok uri {}", pp, uri);
                }
            }
        }));
    }
    for child in children {
        let _ = child.join();
    }
}