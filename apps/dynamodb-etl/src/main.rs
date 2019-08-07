use std::error;
use std::io::Read;
use std::io::{self, BufRead};

use jq_rs;
use base64::decode;
use flate2::bufread::GzDecoder;
use jq_rs::JqProgram;

/** We are boxing errors since we'll just report and ignore them */
type Result<T> = std::result::Result<T, Box<dyn error::Error>>;

fn main() {
    let bin_path = ".projectBinaryData.B";
    let bin_query = format!("{} // empty", bin_path);
    let bin_update = format!(".[0]{} = .[1] | .[0]", bin_path);
    let jq_bin_query = &mut jq_rs::compile(&bin_query).unwrap();
    let jq_bin_update = &mut jq_rs::compile(&bin_update).unwrap();

    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        println!("{}", decode_json(line.unwrap().as_str(), jq_bin_query, jq_bin_update).unwrap())
    }
}

//    let json = r#"{ "projectBinaryData" : { "B": "H4sIABWa/lwCA6uu5QIABrCh3QMAAAA=" } }"#;
//    let s = decode_json(json, jq_bin_query, jq_bin_update)
//        .unwrap();
//    println!("Binary conversion: '{}'", s.trim());
//
//    let s2 = decode_json(r#"{ "a": 1 }"#, jq_bin_query, jq_bin_update)
//        .unwrap();
//    println!("No conversion: {}", s2);

fn decode_json(json: &str, query: &mut JqProgram, update: &mut JqProgram) -> Result<String> {
    let query_output = query.run(json)?;
    let binary_data = raw_output(&query_output);

    if !binary_data.is_empty() {
        let decoded = decode_binary_data(&(binary_data.trim()))?;
        Ok(update.run(["[", json, ",", &decoded, "]"].concat().as_str())?)
    } else {
        Ok(String::from(json))
    }
}

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

/** Decode a string created by gzipping and then base64 encoding a text */
fn decode_binary_data(base64_encoded_string: &str) -> Result<String> {
    let gzipped_data = decode(base64_encoded_string)?;
    let mut gz_decoder = GzDecoder::new(&*gzipped_data);
    let mut uncompressed_data = String::new();
    gz_decoder.read_to_string(&mut uncompressed_data)?;
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
}
