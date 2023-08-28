extern crate clap;
use clap::{App, Arg};
use csv::Reader;
use ethabi::{ethereum_types::H256, Contract, Event, RawLog, Token};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::Path,
};

const SUBGRAPH_ENDPOINT: &str =
    "https://api.thegraph.com/subgraphs/name/blocklytics/ethereum-blocks";

#[derive(Debug, Deserialize, Serialize)]
struct RawLogEntry {
    block_number: u64,
    transaction_index: u64,
    log_index: u64,
    transaction_hash: String,
    contract_address: String,
    topic0: Option<String>,
    topic1: Option<String>,
    topic2: Option<String>,
    topic3: Option<String>,
    data: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct ParsedLogEntry {
    block_number: u64,
    block_timestamp: Option<u64>,
    transaction_index: u64,
    log_index: u64,
    transaction_hash: String,
    contract_address: String,
    topic0: Option<String>,
    topic1: Option<String>,
    topic2: Option<String>,
    topic3: Option<String>,
    data: String,
}

struct Timestamps {
    timestamps: BTreeMap<u64, u64>,
    avg_block_time: u64,
}

#[tokio::main]
async fn main() {
    let matches = App::new("Ethereum Event Decoder")
        .version("1.0")
        .author("Your Name. <your_email@example.com>")
        .about("Decodes Ethereum contract events")
        .arg(
            Arg::with_name("ABI")
                .short("a")
                .long("abi")
                .value_name("FILE")
                .help("Sets the ABI JSON file for the contract")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("LOG")
                .short("l")
                .long("log")
                .value_name("FILE or DIRECTORY")
                .help("File or directory containing logs to be decoded")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("TIMESTAMP")
                .short("t")
                .long("timestamp")
                .help("Should the timestamp be included in the output")
                .takes_value(false)
                .required(false),
        )
        .arg(
            Arg::with_name("INTERVAL")
                .short("i")
                .long("interval")
                .value_name("INTERVAL")
                .help("Interval for querying timestamps. E.g., -i 10 means every 10th block's timestamp will be queried.")
                .takes_value(true)
                .required(false),
        )
        .get_matches();

    let abi_path = matches.value_of("ABI").unwrap();
    let csv_path = matches.value_of("LOG").unwrap();

    let timestamps_arg = matches.is_present("TIMESTAMP");
    let interval: u64 = matches
        .value_of("INTERVAL")
        .unwrap_or("1")
        .parse()
        .expect("Failed to parse interval");

    process_input(abi_path, csv_path, timestamps_arg, interval).await;
}

async fn process_input(abi_path: &str, target_path: &str, timestamps: bool, interval: u64) {
    let abi_content = fs::read_to_string(abi_path).expect("Failed to read ABI file");
    let contract = Contract::load(abi_content.as_bytes()).expect("Failed to load ABI");
    let mut event_map: HashMap<H256, &Event> = HashMap::new();
    let mut event_fields_map: HashMap<String, Vec<String>> = HashMap::new();
    for event in contract.events() {
        event_map.insert(event.signature(), event);
        let fields: Vec<String> = event
            .inputs
            .iter()
            .map(|input| input.name.clone())
            .collect();
        event_fields_map.insert(event.name.clone(), fields);
    }

    let path = Path::new(&target_path);
    if path.is_dir() {
        // Process all files in the directory
        let entries = fs::read_dir(path).expect("Unable to read directory");
        for entry in entries {
            let file_path = entry.expect("Unable to process directory entry").path();
            if file_path.is_file() {
                process_file(&event_map, &file_path, timestamps, interval).await;
            }
        }
        // in the directory
        let abi_outpath = path.join("abi_summary.json");
        let mut abi_summary = fs::File::create(abi_outpath).expect("Unable to create file");
        serde_json::to_writer(&mut abi_summary, &event_fields_map).expect("Unable to write file");
    } else if path.is_file() {
        process_file(&event_map, &path, timestamps, interval).await;

        // in the parent directory of this file
        let abi_outpath = path.with_file_name("abi_summary.json");
        let mut abi_summary = fs::File::create(abi_outpath).expect("Unable to create file");
        serde_json::to_writer(&mut abi_summary, &event_fields_map).expect("Unable to write file");
    } else {
        println!("Invalid input. Please provide a valid file or directory.");
    }
}

async fn process_file(
    event_map: &HashMap<H256, &Event>,
    file_path: &std::path::Path,
    use_timestamps: bool,
    interval: u64,
) {
    let file_name = file_path
        .file_name()
        .expect("Unable to get file name")
        .to_str()
        .expect("Unable to convert file name to string");
    let file_extension = file_path
        .extension()
        .expect("Unable to get file extension")
        .to_str()
        .expect("Unable to convert file extension to string");
    // check if file ends in csv and does not start with decoded_
    if file_extension == "csv" && !file_name.starts_with("decoded_") {
        let mut rdr = Reader::from_path(file_path).expect("Failed to open CSV file");
        // same as the input file, but with a "decoded_" prefix
        let decoded_file_path = file_path.with_file_name(format!("decoded_{}", file_name));
        let mut wtr =
            csv::Writer::from_path(decoded_file_path).expect("Failed to create CSV writer");
        // Check the first and last value for the block number
        let first_record = rdr.records().next().expect("Failed to read first record");
        let first_entry: RawLogEntry = first_record
            .expect("Failed to deserialize record")
            .deserialize(None)
            .expect("Failed to deserialize entry");
        let last_record = rdr.records().last().expect("Failed to read last record");
        let last_entry: RawLogEntry = last_record
            .expect("Failed to deserialize record")
            .deserialize(None)
            .expect("Failed to deserialize entry");

        let start_block = first_entry.block_number;
        let end_block = last_entry.block_number;

        let timestamps = if use_timestamps {
            Some(get_block_timestamps(start_block, end_block, interval).await)
        } else {
            None
        };

        let mut rdr = Reader::from_path(file_path).expect("Failed to open CSV file");
        for result in rdr.deserialize() {
            let entry: RawLogEntry = result.expect("Failed to deserialize entry");

            let mut results = parse_entry(entry, event_map).into();
            if use_timestamps {
                results = set_timestamp(results, &timestamps.as_ref().unwrap());
            }
            wtr.serialize(results).expect("Failed to write entry");
        }
    } else {
        println!("Skipping {}", file_name);
    }
}

fn set_timestamp(mut entry: ParsedLogEntry, timestamps: &Timestamps) -> ParsedLogEntry {
    if let Some(&timestamp) = timestamps.timestamps.get(&entry.block_number) {
        entry.block_timestamp = Some(timestamp);
    } else {
        let previous = timestamps
            .timestamps
            .range(..entry.block_number)
            .rev()
            .next();
        match previous {
            Some((block_number, timestamp)) => {
                let block_diff = entry.block_number - block_number;
                entry.block_timestamp = Some(timestamp + timestamps.avg_block_time * block_diff);
            }
            None => {
                let next = timestamps.timestamps.range(entry.block_number..).next();
                match next {
                    Some((block_number, timestamp)) => {
                        let block_diff = block_number - entry.block_number;
                        entry.block_timestamp =
                            Some(timestamp - timestamps.avg_block_time * block_diff);
                    }
                    None => {
                        println!("No timestamp found for block {}", entry.block_number);
                    }
                }
            }
        }
    }
    entry
}

fn parse_entry(entry: RawLogEntry, event_map: &HashMap<H256, &Event>) -> RawLogEntry {
    let log = RawLog {
        topics: entry
            .topics()
            .iter()
            .map(|t| t.parse().expect("Failed to parse topic"))
            .collect(),
        data: hex_str_to_bytes(&entry.data),
    };
    let mut parsed_entry = RawLogEntry::new(
        entry.block_number,
        entry.transaction_index,
        entry.log_index,
        entry.transaction_hash,
        entry.contract_address,
    );
    if let Some(event) = event_map.get(&log.topics[0]) {
        let event_name = event.name.clone();
        let decoded_log = event.parse_log(log.clone()).expect("Failed to parse log");
        parsed_entry.topic0 = Some(event_name);
        let mut topic_count = 1;
        for param in decoded_log.params {
            let param_value_string = match param.value {
                Token::Uint(u) => format!("{}", u),
                _ => format!("{}", param.value),
            };
            match topic_count {
                1 => parsed_entry.topic1 = Some(format!("{}", param_value_string)),
                2 => parsed_entry.topic2 = Some(format!("{}", param_value_string)),
                3 => parsed_entry.topic3 = Some(format!("{}", param_value_string)),
                _ => println!("Invalid topic count"),
            }
            topic_count += 1;
        }
    } else {
        println!(
            "No event found for topic: {:?}, tx: {:?}, log_index: {:?} , is the ABI correct?",
            format!("{:x}", log.topics[0]),
            parsed_entry.transaction_hash,
            entry.log_index
        );
    }
    parsed_entry
}

async fn fetch_page(query: Value) -> Result<Value, reqwest::Error> {
    let client = reqwest::Client::new();
    let response: Value = client
        .post(SUBGRAPH_ENDPOINT)
        .json(&query)
        .send()
        .await?
        .json()
        .await?;

    Ok(response)
}
use futures::stream::{self, StreamExt};
use std::sync::{Arc, Mutex};

async fn get_block_timestamps(start_block: u64, end_block: u64, _: u64) -> Timestamps {
    let timestamps = Arc::new(Mutex::new(BTreeMap::new()));
    let mut skip_count = 0;
    let mut start_block_arg = start_block;
    const PAGE_SIZE: usize = 1_000; // Adjust as needed

    let mut queries = Vec::new();
    loop {
        let query = json!({
            "query": r#"
                query GetBlocks($pageSize: Int!, $skipCount: Int!, $startBlock: Int!) {
                    blocks(
                        first: $pageSize,
                        skip: $skipCount,
                        orderBy: number,
                        orderDirection: asc,
                        where: { number_gt: $startBlock }
                    ) {
                        number
                        timestamp
                    }
                }
            "#,
            "variables": {
                "pageSize": PAGE_SIZE,
                "skipCount": skip_count,
                "startBlock": start_block_arg
            }
        });

        queries.push(query);
        skip_count += PAGE_SIZE;
        if skip_count > 5000 {
            start_block_arg += (skip_count - PAGE_SIZE) as u64;
            skip_count = 0;
        }
        if start_block_arg + skip_count as u64 >= end_block {
            break;
        }
    }

    stream::iter(queries.into_iter())
        .for_each_concurrent(25, |query| async {
            let maybe_response = fetch_page(query).await;
            match maybe_response {
                Ok(response) => {
                    if response.get("errors").is_some() {
                        eprintln!("Error: {:?}", response);
                        // You can return here, or handle the error as needed
                        return;
                    }

                    if let Some(blocks) = response["data"]["blocks"].as_array() {
                        if blocks.is_empty() {
                            // This will not break the outer loop, but just skip the current iteration
                            return;
                        }

                        for block in blocks {
                            let block_nr = block["number"]
                                .as_str()
                                .unwrap_or("")
                                .parse::<u64>()
                                .ok()
                                .unwrap();
                            let timestamp = block["timestamp"]
                                .as_str()
                                .unwrap_or("")
                                .parse::<u64>()
                                .ok()
                                .unwrap();
                            timestamps.lock().unwrap().insert(block_nr, timestamp);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                    // Handle the error as needed or return to skip this iteration
                }
            }
        })
        .await;

    let timestamps = Arc::try_unwrap(timestamps).unwrap().into_inner().unwrap();
    Timestamps::new(timestamps)
}

fn hex_str_to_bytes(hex_str: &str) -> Vec<u8> {
    let without_prefix = hex_str.strip_prefix("0x").unwrap_or(hex_str);
    hex::decode(without_prefix).expect("Decoding failed")
}

impl RawLogEntry {
    fn new(
        block_number: u64,
        transaction_index: u64,
        log_index: u64,
        transaction_hash: String,
        contract_address: String,
    ) -> Self {
        Self {
            block_number,
            transaction_index,
            log_index,
            transaction_hash,
            contract_address,
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: String::new(),
        }
    }

    fn topics(&self) -> Vec<String> {
        let mut topics = Vec::new();
        if let Some(ref t0) = self.topic0 {
            topics.push(t0.clone());
        }
        if let Some(ref t1) = self.topic1 {
            topics.push(t1.clone());
        }
        if let Some(ref t2) = self.topic2 {
            topics.push(t2.clone());
        }
        if let Some(ref t3) = self.topic3 {
            topics.push(t3.clone());
        }
        topics
    }
}

impl From<RawLogEntry> for ParsedLogEntry {
    fn from(raw: RawLogEntry) -> Self {
        ParsedLogEntry {
            block_number: raw.block_number,
            block_timestamp: None, // Initialize as None, or fetch the timestamp here if you wish
            transaction_index: raw.transaction_index,
            log_index: raw.log_index,
            transaction_hash: raw.transaction_hash,
            contract_address: raw.contract_address,
            topic0: raw.topic0,
            topic1: raw.topic1,
            topic2: raw.topic2,
            topic3: raw.topic3,
            data: raw.data,
        }
    }
}

impl Timestamps {
    fn new(timestamps: BTreeMap<u64, u64>) -> Self {
        let (first_ts, first_block) = timestamps.iter().next().unwrap();
        let (last_ts, last_block) = timestamps.iter().last().unwrap();
        let avg_block_time = (last_ts - first_ts) / (last_block - first_block);

        Self {
            timestamps,
            avg_block_time,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_timestamps() {
        let start_block: u64 = 17004000;
        let end_block: u64 = 18005038;
        let _ = get_block_timestamps(start_block, end_block, 1).await;
    }
}
