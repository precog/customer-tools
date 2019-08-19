use std::fmt::Formatter;

use ::jq_rs;
use ::jq_rs::JqProgram;

use crate::errors::*;

pub(crate) struct Queries {
    get: JqProgram,
    set: JqProgram,
    update: JqProgram,
    desc: String
}

impl Queries {
    // TODO: make "fromjson" a parameter on update
    pub(crate) fn new(path: &str) -> Result<Queries> {
        let get = jq_get_query(path)?;
        let set = jq_set_query(path)?;
        let update = jq_update_query(path)?;
        let programs = Queries { get, set, update, desc: path.to_owned() };
        Ok(programs)
    }

    pub(crate) fn get(&mut self, json: &str) -> Result<String> {
        self.get.run(json)
            .map_err(|e| e.to_error("querying binary data"))
            .map(|result| raw_output(&result))
    }

    pub(crate) fn set(&mut self, json: &str, value: &str) -> Result<String> {
        self.set.run(["[", json, ",", value, "]"].concat().as_str())
            .map_err(|e| e.to_error("updating binary data"))
            .map(|result| raw_output(&result))
    }

    pub(crate) fn update(&mut self, json: &str) -> Result<String> {
        self.update.run(&json)
            .map_err(|e| e.to_error("updating text data"))
            .map(|result| raw_output(&result))
    }
}

impl std::fmt::Debug for Queries {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "Queries {{ path = \"{}\" }}", self.desc)
    }
}

fn jq_get_query(path: &str) -> Result<JqProgram> {
    let query = format!("if {path} | type != \"null\" then {path} else empty end",
                            path = path);
    jq_rs::compile(&query).map_err(|e| e.to_error("compiling get query"))
}

fn jq_set_query(path: &str) -> Result<JqProgram> {
    let query = format!(".[0]{path} = .[1] | .[0]",
                             path = path);
    jq_rs::compile(&query).map_err(|e| e.to_error("compiling set query"))
}

fn jq_update_query(path: &str) -> Result<JqProgram> {
    let query = format!("if {path} | type != \"null\" then {path} |= {expr} else . end",
                              path = path, expr = "fromjson");
    jq_rs::compile(&query).map_err(|e| e.to_error("compiling update query"))
}

/// Trims newlines and removes quotes if json is string
pub(crate) fn raw_output(json: &str) -> String {
    let trimmed = json.trim();
    let unquoted = if trimmed.len() > 1 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    };
    unquoted.to_owned()
}

trait ToError {
    fn to_error(&self, when: &str) -> Error;
}

impl ToError for jq_rs::Error {
    fn to_error(&self, when: &str) -> Error {
        match self {
            jq_rs::Error::System { ref reason } =>
                match reason {
                    Some(message) if message.starts_with("JQ: Parse error:") =>
                        ErrorKind::JqParseError(when.to_owned(), message[12..].to_owned()).into(),
                    _ => ErrorKind::JqError(when.to_owned(), format!("{}", self)).into()
                },
            jq_rs::Error::InvalidProgram => ErrorKind::JqInvalidProgram(when.to_owned()).into(),
            _ => ErrorKind::JqError(when.to_owned(), format!("{}", self)).into()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::assert_matches::assert_matches;

    const JSON: &str = r#"{"some":{"path":"{}"}}"#;

    #[test]
    fn test_get_existing_path() {
        let path = ".some.path";
        let queries = &mut Queries::new(path).unwrap();
        let result = queries.get(JSON);
        assert_matches!(result, Ok(ref actual) if actual == "{}");
    }

    #[test]
    fn test_get_non_existing_path() {
        let path = ".some.other.path";
        let queries = &mut Queries::new(path).unwrap();
        let result = queries.get(JSON);
        assert_matches!(result, Ok(ref actual) if actual.is_empty());
    }

    #[test]
    fn test_set_existing_path() {
        let path = ".some.path";
        let queries = &mut Queries::new(path).unwrap();
        let result = queries.set(JSON, &quote("there"));
        assert_matches!(result, Ok(ref actual) if actual == r#"{"some":{"path":"there"}}"#);
    }

    #[test]
    fn test_set_non_existing_path() {
        let path = ".some.other.path";
        let queries = &mut Queries::new(path).unwrap();
        let result = queries.set(JSON, &quote("there"));
        assert_matches!(result, Ok(ref actual)
        if actual == r#"{"some":{"path":"{}","other":{"path":"there"}}}"#);
    }

    #[test]
    fn test_update_existing_path() {
        let path = ".some.path";
        let queries = &mut Queries::new(path).unwrap();
        let result = queries.update(JSON);
        assert_matches!(result, Ok(ref actual) if actual == r#"{"some":{"path":{}}}"#);
    }

    #[test]
    fn test_update_non_existing_path() {
        let path = ".some.other.path";
        let queries = &mut Queries::new(path).unwrap();
        let result = queries.update(JSON);
        assert_matches!(result, Ok(ref actual) if actual == JSON);
    }

    #[test]
    fn test_remove_newline_set_existing_path() {
        let path = ".some.path";
        let queries = &mut Queries::new(path).unwrap();
        let result = queries.set(&nl(JSON), &quote("there"));
        assert_matches!(result, Ok(ref actual) if actual == r#"{"some":{"path":"there"}}"#);
    }

    #[test]
    fn test_remove_newline_set_non_existing_path() {
        let path = ".some.other.path";
        let queries = &mut Queries::new(path).unwrap();
        let result = queries.set(&nl(JSON), &quote("there"));
        assert_matches!(result, Ok(ref actual)
        if actual == r#"{"some":{"path":"{}","other":{"path":"there"}}}"#);
    }

    #[test]
    fn test_remove_newline_update_existing_path() {
        let path = ".some.path";
        let queries = &mut Queries::new(path).unwrap();
        let result = queries.update(&nl(JSON));
        assert_matches!(result, Ok(ref actual) if actual == r#"{"some":{"path":{}}}"#);
    }

    #[test]
    fn test_remove_newline_update_non_existing_path() {
        let path = ".some.other.path";
        let queries = &mut Queries::new(path).unwrap();
        let result = queries.update(&nl(JSON));
        assert_matches!(result, Ok(ref actual) if actual == JSON);
    }

    #[test]
    fn test_remove_quotes_get_existing_path() {
        let json = r#"{"some":{"path":"here"}}"#;
        let path = ".some.path";
        let queries = &mut Queries::new(path).unwrap();
        let result = queries.get(json);
        // string 'here' instead of string '"here"'
        assert_matches!(result, Ok(ref actual) if actual == "here");
    }

    #[test]
    fn test_invalid_path() {
        let path = "this is not jq code";
        let result = &mut Queries::new(path);
        assert_matches!(result, Err(Error(ErrorKind::JqInvalidProgram(_), _)));
        assert_matches!(result, Err(ref error) if error.is_fatal())
    }

    #[test]
    fn test_get_invalid_json() {
        let json = "not a json";
        let path = ".some.path";
        let queries = &mut Queries::new(path).unwrap();
        let result = queries.get(json);
        assert_matches!(result, Err(Error(ErrorKind::JqParseError(_, _), _)));
        assert_matches!(result, Err(ref error) if !error.is_fatal())
    }

    #[test]
    fn test_set_invalid_json() {
        let json = "not a json";
        let path = ".some.path";
        let queries = &mut Queries::new(path).unwrap();
        let result = queries.set(json, "{}");
        assert_matches!(result, Err(Error(ErrorKind::JqParseError(_, _), _)));
        assert_matches!(result, Err(ref error) if !error.is_fatal())
    }

    #[test]
    fn test_set_invalid_json_value_being_set() {
        let path = ".some.path";
        let queries = &mut Queries::new(path).unwrap();
        let result = queries.set(JSON, "not a json");
        assert_matches!(result, Err(Error(ErrorKind::JqParseError(_, _), _)));
        assert_matches!(result, Err(ref error) if !error.is_fatal())
    }

    #[test]
    fn test_update_invalid_json() {
        let json = "not a json";
        let path = ".some.path";
        let queries = &mut Queries::new(path).unwrap();
        let result = queries.update(json);
        assert_matches!(result, Err(Error(ErrorKind::JqParseError(_, _), _)));
        assert_matches!(result, Err(ref error) if !error.is_fatal())
    }

    #[test]
    fn test_update_invalid_json_inside_field() {
        let json = r#"{"some":{"path":"not a json"}}"#;
        let path = ".some.path";
        let queries = &mut Queries::new(path).unwrap();
        let result = queries.update(json);
        assert_matches!(result, Err(Error(ErrorKind::JqParseError(_, _), _)));
        assert_matches!(result, Err(ref error) if !error.is_fatal())
    }

    #[test]
    fn test_raw_output() {
        assert_eq!(raw_output("{}"), "{}");
        assert_eq!(raw_output("{}\n"), "{}");
        assert_eq!(raw_output(r#""string""#), "string");
        assert_eq!(raw_output([r#""string""#, "\n"].concat().as_str()), "string");
    }

    fn nl(text: &str) -> String {
        [text, "\n"].concat()
    }

    #[test]
    fn test_nl() {
        assert_eq!(nl("text"), "text\n");
    }

    fn quote(text: &str) -> String {
        ["\"", text, "\""].concat()
    }

    #[test]
    fn test_quote() {
        assert_eq!(quote("text"), "\"text\"");
    }
}