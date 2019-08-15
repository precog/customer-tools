#![recursion_limit = "1024"]
#![warn(absolute_paths_not_starting_with_crate,deprecated_in_future,
elided_lifetimes_in_paths,macro_use_extern_crate,missing_copy_implementations,
missing_debug_implementations,single_use_lifetimes,unreachable_pub,unused_extern_crates,
unused_import_braces,unused_lifetimes,unused_qualifications,unused_results)]

mod errors;
mod json_queries;

use std::io::{self, BufRead, Read, Write};

use ::error_chain::quick_main;
use ::base64::decode;
use ::flate2::bufread::GzDecoder;
use ::structopt::{self, StructOpt};

use crate::errors::*;
use crate::json_queries::*;

quick_main!(run);

#[cfg(test)]
const DEFAULT_BIN_PATH: &str = ".projectBinaryData.B";
#[cfg(test)]
const DEFAULT_TEXT_PATH: &str = ".projectData.S";

/// Rewrites json replacing string field values with their json content
///
/// Paths are specified as .x.y.z for { "x": { "y": { "z": data }}}. More
/// generally, they must be valid "jq" paths.
///
/// Binary paths must point to a string that contains a base64-encoded,
/// gzipped json, so that "base64 --decode | gzip -d" will turn that
/// string into valid json.
///
/// String paths must point to a string that contains valid json. For
/// example, .x in { "x": "{ \"a\": 5 }" }.
///
/// Use a non-existing path if there's no binary or string path. For
/// example, ".no.binary.path .path.to.string" if there's string data
/// on the .path.to.string, but not binary data, and ".no.binary.path"
/// is not an existing path in the input data.
#[derive(Debug,StructOpt)]
#[structopt(name = "dynamodb-etl", about = "", author = "")]
struct Opt {
    /// Binary data path
    #[structopt(short, long, default_value = ".projectBinaryData.B")]
    binpath: String,

    /// Text data path
    #[structopt(short, long, default_value = ".projectData.S")]
    textpath: String,
}

fn run() -> Result<()> {
    let opt: Opt = Opt::from_args();

    let stdin = io::stdin();
    let input = stdin.lock();
    let stdout = io::stdout();
    let mut output = stdout.lock();

    let bin_path = &opt.binpath;
    let text_path = &opt.textpath;

    let bin_queries = &mut json_queries::Queries::new(bin_path)?;
    let text_queries = &mut json_queries::Queries::new(text_path)?;

    process_input(input, &mut output, bin_queries, text_queries)
}

fn process_input(input: impl BufRead,
                 mut output: impl Write,
                 bin_queries: &mut Queries,
                 text_queries: &mut Queries) -> Result<()> {
    for (index, next_line) in input.lines().enumerate() {
        let processed_line =
            process_line(next_line.map_err(|e| e.into()), index, bin_queries, text_queries);
        match processed_line {
            Err(ref error) if error.is_fatal() => processed_line.map(|_| ())?,
            Err(ref error) => {
                eprintln!("Error: {}", error);
                for e in error.iter().skip(1) {
                    eprintln!("caused by: {}", e);
                }
            },
            Ok(ref message) => writeln!(output, "{}", message)?,
        }
    }
    Ok(())
}

fn process_line(next_line: Result<String>,
                index: usize,
                bin_queries: &mut Queries,
                text_queries: &mut Queries) -> Result<String> {
    let line_num = index + 1;
    let result = next_line
        .and_then(|line| {
            re_encode_json(&line, bin_queries, text_queries)
        });
    // TODO: print "line" on error, if available
    match result {
        Err(ref error) if error.is_fatal() =>
            result.chain_err(|| ErrorKind::LineNo(line_num, true)),
        Err(_) =>
            result.chain_err(|| ErrorKind::LineNo(line_num, false)),
        _ => result
    }
}

fn re_encode_json(str_line: &str, bin_queries: &mut Queries, text_queries: &mut Queries) -> Result<String> {
    let re_encoded_bin = re_encode_binary_data(str_line, bin_queries)?;
    re_encode_text_data(&re_encoded_bin, text_queries)
}

/// Replace strings containing json with that json
fn re_encode_text_data(json: &str, queries: &mut Queries) -> Result<String> {
    queries.update(&json)
}

/// Replace strings containing base64-encoded, gzipped json with that json
fn re_encode_binary_data(json: &str, queries: &mut Queries) -> Result<String> {
    let binary_data = queries.get(json)?;
    if !binary_data.is_empty() {
        let decoded = decode_binary_data(&(binary_data.trim()))?;
        queries.set(json, &decoded)
    } else {
        Ok(raw_output(json))
    }
}

/// Decode a string created by gzipping and then base64 encoding a text
fn decode_binary_data(base64_encoded_string: &str) -> Result<String> {
    let gzipped_data = decode(base64_encoded_string)
        .chain_err(|| ErrorKind::Base64Error)?;
    let mut gz_decoder = GzDecoder::new(&*gzipped_data);
    let mut uncompressed_data = String::new();
    let _ = gz_decoder.read_to_string(&mut uncompressed_data)
        .chain_err(|| ErrorKind::GzipError)?;
    Ok(uncompressed_data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use ::assert_matches::assert_matches;

    #[test]
    fn test_decode_binary_data() {
        let json = "H4sIABWa/lwCA6uu5QIABrCh3QMAAAA=";
        let result = decode_binary_data(json);
        assert_matches!(result, Ok(ref actual) if actual == "{}\n")
    }

    #[test]
    fn test_decode_fail_base64() {
        let result = decode_binary_data("not base64-encoded");
        assert_matches!(result, Err(Error(ErrorKind::Base64Error, _)));
        assert_matches!(result, Err(ref error) if !error.is_fatal())
    }

    #[test]
    fn test_decode_fail_gzip() {
        let result = decode_binary_data("bm90IGd6aXBwZWQK");
        assert_matches!(result, Err(Error(ErrorKind::GzipError, _)));
        assert_matches!(result, Err(ref error) if !error.is_fatal())
    }

    #[test]
    fn test_re_encode_binary_data() {
        let json = r#"{ "projectBinaryData" : { "B": "H4sIABWa/lwCA6uu5QIABrCh3QMAAAA=" } }"#;
        let expected = r#"{"projectBinaryData":{"B":{}}}"#;
        let queries = &mut Queries::new(DEFAULT_BIN_PATH).unwrap();
        let result = re_encode_binary_data(json, queries);
        assert_matches!(result, Ok(ref actual) if actual == expected)
    }

    #[test]
    fn test_re_encode_binary_data_does_not_add_it() {
        let json = r#"{ "a": 1 }"#;
        let queries = &mut Queries::new(DEFAULT_BIN_PATH).unwrap();
        let result = re_encode_binary_data(json, queries);
        assert_matches!(result, Ok(ref actual) if actual == json)
    }

    #[test]
    fn test_re_encode_binary_data_fail_not_encoded() {
        let json = r#"{ "projectBinaryData" : { "B": {} } }"#;
        let queries = &mut Queries::new(DEFAULT_BIN_PATH).unwrap();
        let result = re_encode_binary_data(json, queries);
        assert_matches!(result, Err(ref error) if !error.is_fatal())
    }

    #[test]
    fn test_re_encode_binary_data_fail_not_json() {
        let json = r#"{ "projectBinaryData" : { "B": "H4sIAEafTF0AA8vMK0vMyUxRyCrOz+MCAIg5TZANAAAA" } }"#;
        let queries = &mut Queries::new(DEFAULT_BIN_PATH).unwrap();
        let result = re_encode_binary_data(json, queries);
        assert_matches!(result, Err(ref error) if !error.is_fatal())
    }

    #[test]
    fn test_re_encode_text_data() {
        let json = r#"{ "projectData" : { "S": "{}" } }"#;
        let expected = r#"{"projectData":{"S":{}}}"#;
        let queries = &mut Queries::new(DEFAULT_TEXT_PATH).unwrap();
        let result = re_encode_text_data(json, queries);
        assert_matches!(result, Ok(ref actual) if actual == expected)
    }

    #[test]
    fn test_re_encode_text_data_does_not_add_it() {
        let json = r#"{"a":1}"#;
        let queries = &mut Queries::new(DEFAULT_TEXT_PATH).unwrap();
        let result = re_encode_text_data(json, queries);
        assert_matches!(result, Ok(ref actual) if actual == json)
    }

    #[test]
    fn test_re_encode_text_data_fail_json() {
        let json = r#"{ "projectData" : { "S": "invalid json" } }"#;
        let queries = &mut Queries::new(DEFAULT_TEXT_PATH).unwrap();
        let result = re_encode_text_data(json, queries);
        assert_matches!(result, Err(ref error) if !error.is_fatal())
    }

    #[test]
    fn test_re_encode_json_no_data() {
        let json = r#"{"a":1}"#;
        let expected = &json.to_owned();
        let bin_queries = &mut Queries::new(DEFAULT_BIN_PATH).unwrap();
        let text_queries = &mut Queries::new(DEFAULT_TEXT_PATH).unwrap();
        let result = re_encode_json(json, bin_queries, text_queries);
        assert_matches!(result, Ok(ref actual) if actual == expected);
    }

    #[test]
    fn test_re_encode_json_text_data() {
        let json = r#"{ "projectData" : { "S": "{}" } }"#;
        let expected = r#"{"projectData":{"S":{}}}"#;
        let bin_queries = &mut Queries::new(DEFAULT_BIN_PATH).unwrap();
        let text_queries = &mut Queries::new(DEFAULT_TEXT_PATH).unwrap();
        let result = re_encode_json(json, bin_queries, text_queries);
        assert_matches!(result, Ok(ref actual) if actual == expected)
    }

    #[test]
    fn test_re_encode_json_binary_data() {
        let json = r#"{ "projectBinaryData" : { "B": "H4sIABWa/lwCA6uu5QIABrCh3QMAAAA=" } }"#;
        let expected = r#"{"projectBinaryData":{"B":{}}}"#;
        let bin_queries = &mut Queries::new(DEFAULT_BIN_PATH).unwrap();
        let text_queries = &mut Queries::new(DEFAULT_TEXT_PATH).unwrap();
        let result = re_encode_json(json, bin_queries, text_queries);
        assert_matches!(result, Ok(ref actual) if actual == expected)
    }

    #[test]
    fn test_re_encode_json_both_data() {
        let json = r#"
            {
                "projectData" : { "S": "{}" },
                "projectBinaryData" : { "B": "H4sIABWa/lwCA6uu5QIABrCh3QMAAAA=" }
            }
        "#;
        let expected = &r#"
            {
                "projectData" : { "S": {} },
                "projectBinaryData" : { "B": {} }
            }
        "#.replace(|c: char| c.is_whitespace(), "");
        let bin_queries = &mut Queries::new(DEFAULT_BIN_PATH).unwrap();
        let text_queries = &mut Queries::new(DEFAULT_TEXT_PATH).unwrap();
        let result = re_encode_json(json, bin_queries, text_queries);
        assert_matches!(result, Ok(ref actual) if actual == expected)
    }

    #[test]
    fn test_re_encode_json_extra_data() {
        let json = r#"
            {
                "a": 1,
                "projectData": { "S": "{}" },
                "projectBinaryData": { "B": "H4sIABWa/lwCA6uu5QIABrCh3QMAAAA=" }
            }
        "#;
        let expected = &r#"
            {
                "a": 1,
                "projectData" : { "S": {} },
                "projectBinaryData" : { "B": {} }
            }
        "#.replace(|c: char| c.is_whitespace(), "");
        let bin_queries = &mut Queries::new(DEFAULT_BIN_PATH).unwrap();
        let text_queries = &mut Queries::new(DEFAULT_TEXT_PATH).unwrap();
        let result = re_encode_json(json, bin_queries, text_queries);
        assert_matches!(result, Ok(ref actual) if actual == expected)
    }

    #[test]
    fn test_re_encode_json_bad_text_data() {
        let json = r#"
            {
                "a": 1,
                "projectData": { "S": "not json" },
                "projectBinaryData": { "B": "H4sIABWa/lwCA6uu5QIABrCh3QMAAAA=" }
            }
        "#;
        let bin_queries = &mut Queries::new(DEFAULT_BIN_PATH).unwrap();
        let text_queries = &mut Queries::new(DEFAULT_TEXT_PATH).unwrap();
        let result = re_encode_json(json, bin_queries, text_queries);
        assert_matches!(result, Err(ref error) if !error.is_fatal())
    }

    #[test]
    fn test_re_encode_json_bad_binary_data() {
        let json = r#"
            {
                "a": 1,
                "projectData": { "S": "{}" },
                "projectBinaryData": { "B": "not encoded" }
            }
        "#;
        let bin_queries = &mut Queries::new(DEFAULT_BIN_PATH).unwrap();
        let text_queries = &mut Queries::new(DEFAULT_TEXT_PATH).unwrap();
        let result = re_encode_json(json, bin_queries, text_queries);
        assert_matches!(result, Err(ref error) if !error.is_fatal())
    }

    #[test]
    fn test_process_line() {
        let json = r#"
            {
                "a": 1,
                "projectData": { "S": "{}" },
                "projectBinaryData": { "B": "H4sIABWa/lwCA6uu5QIABrCh3QMAAAA=" }
            }
        "#;
        let expected = &r#"
            {
                "a": 1,
                "projectData" : { "S": {} },
                "projectBinaryData" : { "B": {} }
            }
        "#.replace(|c: char| c.is_whitespace(), "");
        let bin_queries = &mut Queries::new(DEFAULT_BIN_PATH).unwrap();
        let text_queries = &mut Queries::new(DEFAULT_TEXT_PATH).unwrap();
        let result = process_line(Ok(json.to_owned()), 0, bin_queries, text_queries);
        assert_matches!(result, Ok(ref actual) if actual == expected)
    }

    #[test]
    fn test_process_line_invalid_unicode() {
        let invalid_two_octet_sequence = [0xc3u8, 0x28u8];
        let cursor = Cursor::new(invalid_two_octet_sequence);
        let mut lines_iter = cursor.lines();
        let bin_queries = &mut Queries::new(DEFAULT_BIN_PATH).unwrap();
        let text_queries = &mut Queries::new(DEFAULT_TEXT_PATH).unwrap();
        let line = lines_iter.next().unwrap().map_err(|e| e.into());
        let result = process_line(line, 17, bin_queries, text_queries);
        assert_matches!(result, Err(Error(ErrorKind::LineNo(18, false), _)))
    }

    #[test]
    fn test_process_input() {
        let bin_json = r#"{ "projectBinaryData" : { "B": "H4sIABWa/lwCA6uu5QIABrCh3QMAAAA=" } }"#;
        let bin_expected = r#"{"projectBinaryData":{"B":{}}}"#;
        let text_json = r#"{ "projectData" : { "S": "{}" } }"#;
        let text_expected = r#"{"projectData":{"S":{}}}"#;
        let bad_bin = r#"{ "projectBinaryData" : { "B": "H4sIAEafTF0AA8vMK0vMyUxRyCrOz+MCAIg5TZANAAAA" } }"#;
        let bad_text = r#"{ "projectData" : { "S": "invalid json" } }"#;
        let data = [bad_text, text_json, bad_bin, bin_json].join("\n");
        let input = Cursor::new(data);
        let mut output = Vec::<u8>::with_capacity(1024);
        let bin_queries = &mut Queries::new(DEFAULT_BIN_PATH).unwrap();
        let text_queries = &mut Queries::new(DEFAULT_TEXT_PATH).unwrap();
        let result = process_input(input, &mut output, bin_queries, text_queries);
        assert_matches!(result, Ok(()));
        let result_as_text = std::str::from_utf8(&output);
        if let Ok(text) = result_as_text {
            let lines = text.lines().collect::<Vec<&str>>();
            assert_eq!(lines.len(), 2, "Expected 2 output lines, got {}", lines.len());
            assert_eq!(lines[1], bin_expected, "Unexpected binary path decoding");
            assert_eq!(lines[0], text_expected, "Unexpected text path decoding");
        } else {
            panic!("Unexpected error on output: {:?}\nOutput: {:?}", result_as_text, result);
        }
    }

    // TODO: assert stderr output on bad input data from process_input
}
