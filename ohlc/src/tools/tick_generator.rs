use crate::tools::datas::TickData;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;

pub fn read_lines<P>(filename: &P) -> io::Result<io::Lines<io::BufReader<File>>>
where P: AsRef<Path> {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

pub struct TickGenerator {}

impl TickGenerator {
    pub fn new() -> Self {
        TickGenerator{}
    }

    pub fn from_file(&self, tick_path: &str) -> Vec<TickData>{
        let mut tick_datas: Vec<TickData> = Vec::with_capacity(1024);
        if let Ok(lines) = read_lines(&tick_path) {
            for line in lines {
                if let Ok(line) = line {
                    let mut tick: TickData = serde_json::from_str(line.as_str()).expect("JSON was not well-formatted");
                    tick.populate_price();
                    tick_datas.push(tick);
                }
            }
        }
        else {
            panic!("read file failed:{}", tick_path);
        }
        tick_datas
    }

    pub fn from_mock(&self, size: usize) -> Vec<TickData>{
        let mut tick_datas: Vec<TickData> = Vec::with_capacity(size);
        let bit = 1.122;
        let ask = 1.123;
        for i in 0..size {
            tick_datas.push(TickData::new(
                "".to_string(),
                0,
                "s".to_string(),
                bit.to_string(),
                "".to_string(),
                ask.to_string(),
                "".to_string(),
                1662022800000 + (i as u64),
                0
            ))
        }
        tick_datas
    }
}