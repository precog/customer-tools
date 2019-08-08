#![recursion_limit = "1024"]

#[macro_use]
extern crate error_chain;

mod errors {
    // Create the Error, ErrorKind, ResultExt, and Result types
    error_chain! {
        errors {
            JqError(t: String) {
                description("jq error")
                display("jq error: {}", t)
            }
        }
    }
}

use errors::*;

use std::io::Read;
use std::io::{self, BufRead};

use jq_rs;
use base64::decode;
use flate2::bufread::GzDecoder;
use jq_rs::JqProgram;
use std::error::Error;

quick_main!(run);

const DEFAULT_BIN_PATH: &str = ".projectBinaryData.B";
const DEFAULT_TEXT_PATH: &str = ".projectData.S";

fn run() -> Result<()> {
    let bin_path = DEFAULT_BIN_PATH;
    let jq_bin_query = &mut jq_bin_query(bin_path);
    let jq_bin_update = &mut jq_bin_update(bin_path);

    let text_path = DEFAULT_TEXT_PATH;
    let jq_text_update = &mut jq_text_update(text_path);

    let stdin = io::stdin();
    for (index, line) in stdin.lock().lines().enumerate() {
            let line_num = index + 1;
            let line = line
                .chain_err(|| "unable to unwrap line from enumerator")
                .chain_err(|| format!("Error on line {}", line_num))?;
            let recoded_bin = recode_binary_data(&line, jq_bin_query, jq_bin_update)
                .chain_err(|| "unable to recode binary data")
                .chain_err(|| format!("Error on line {}", line_num))?;
            let recoded_text = jq_text_update.run(&recoded_bin)
                .map_err( |e| jq_err("jq error running text update", e))
                .chain_err(|| format!("Error on line {}", line_num))?;
            println!("{}", recoded_text.trim());
    }

    Ok(())
}

fn jq_text_update(text_path: &str) -> JqProgram {
    let text_update = format!("if {path} then {path} |= fromjson else . end", path = text_path);
    jq_rs::compile(&text_update).unwrap()
}

fn jq_bin_update(bin_path: &str) -> JqProgram {
    let bin_update = format!(".[0]{} = .[1] | .[0]", bin_path);
    jq_rs::compile(&bin_update).unwrap()
}

fn jq_bin_query(bin_path: &str) -> JqProgram {
    let bin_query = format!("{} // empty", bin_path);
    jq_rs::compile(&bin_query).unwrap()
}

/// Convert jq error into a chained error
fn jq_err(msg: &str, e: jq_rs::Error) -> errors::Error {
    let message = format!("{}: {}", msg, e.description());
    let result: Result<()> = Err(ErrorKind::JqError(message).into());
    result.unwrap_err()
}

/// Replace strings containing base64-encoded, gzipped json with that json
fn recode_binary_data(json: &str, query: &mut JqProgram, update: &mut JqProgram) -> Result<String> {
    let query_output = query.run(json)
        .map_err(|e| jq_err("jq error running binary query", e))?;
    let binary_data = raw_output(&query_output);

    if !binary_data.is_empty() {
        let decoded = decode_binary_data(&(binary_data.trim()))
            .chain_err(|| "error decoding the binary data")?;
        update.run(["[", json, ",", &decoded, "]"].concat().as_str())
            .map_err(|e| jq_err("jq error running binary update", e))
    } else {
        Ok(String::from(json))
    }
}

/// Trims newlines and removes quotes if json is string
fn raw_output(json: &str) -> &str {
    let trimmed = json.trim();
    if trimmed.len() > 1 &&
        trimmed.chars().next().map_or_else(|| false, |c| c == '"') &&
        trimmed.chars().last().map_or_else(|| false, |c| c == '"') {
        &trimmed[1..trimmed.len() - 2]
    } else {
        trimmed
    }
}

/// Decode a string created by gzipping and then base64 encoding a text
fn decode_binary_data(base64_encoded_string: &str) -> Result<String> {
    let gzipped_data = decode(base64_encoded_string)
        .chain_err(|| "failed to decode base64")?;
    let mut gz_decoder = GzDecoder::new(&*gzipped_data);
    let mut uncompressed_data = String::new();
    gz_decoder.read_to_string(&mut uncompressed_data)
        .chain_err(|| "fail to decompress data")?;
    Ok(uncompressed_data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode() {
        assert_eq!(decode_binary_data("H4sIABWa/lwCA6uu5QIABrCh3QMAAAA=").unwrap(),
                   "{}\n")
    }

    #[test]
    fn test_decode_fail_base64() {
        assert!(decode_binary_data("not base64-encoded").is_err(),
                "data not base64-encoded should return an error")
    }

    #[test]
    fn test_decode_fail_gzip() {
        assert!(decode_binary_data("bm90IGd6aXBwZWQK").is_err(),
                "data not gzipped should return an error")
    }

    #[test]
    fn test_recode_binary_data() {
        let json = r#"{ "projectBinaryData" : { "B": "H4sIABWa/lwCA6uu5QIABrCh3QMAAAA=" } }"#;
        let update = &mut jq_bin_update(DEFAULT_BIN_PATH);
        let query = &mut jq_bin_query(DEFAULT_BIN_PATH);
        let s = recode_binary_data(json, query, update).unwrap();
        assert_eq!(s.trim(), r#"{"projectBinaryData":{"B":{}}}"#)
    }

    #[test]
    fn test_record_binary_data_does_not_add_it() {
        let json = r#"{ "a": 1 }"#;
        let update = &mut jq_bin_update(DEFAULT_BIN_PATH);
        let query = &mut jq_bin_query(DEFAULT_BIN_PATH);
        let s = recode_binary_data(json, query, update).unwrap();
        assert_eq!(s, json)
    }
}
