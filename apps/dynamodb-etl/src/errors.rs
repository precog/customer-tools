use error_chain::error_chain;
#[allow(unused_imports)]
use error_chain::error_chain_processing;
#[allow(unused_imports)]
use error_chain::impl_error_chain_processed;
#[allow(unused_imports)]
use error_chain::impl_error_chain_kind;
#[allow(unused_imports)]
use error_chain::impl_extract_backtrace;

// Create the Error, ErrorKind, ResultExt, and Result types
error_chain! {
// Waiting on a new version of jq_rs (post 0.4.0)
//    foreign_links {
//        Jq(jq_rs::Error);
//    }
    foreign_links {
        Fmt(::std::fmt::Error);
        Io(::std::io::Error) #[cfg(unix)];
    }

    // ErrorKind additional errors
    errors {
        Base64Error {
            display("Error: binary data is not valid base64 encoding")
        }
        GzipError {
            display("Error: binary data is not valid gzip compression")
        }
        JqInvalidProgram(when: String) {
            display("Invalid JQ Program {}", when)
        }
        JqParseError(when: String, d: String) {
            display("Error {}: data is not valid json; {}", when, d)
        }
        JqError(when: String, d: String) {
            display("jq error {}: {}", when, d)
        }
        LineNo(number: usize, is_fatal: bool) {
            display("Error processing record number {}", number)
        }
    }
}

pub(crate) trait ErrorIsFatal {
    fn is_fatal(&self) -> bool;
}

impl ErrorIsFatal for Error {
    fn is_fatal(&self) -> bool {
        match self.0 {
            ErrorKind::Base64Error => false,
            ErrorKind::GzipError => false,
            ErrorKind::JqParseError(_, _) => false,
            ErrorKind::LineNo(_, is_fatal) => is_fatal,
            ErrorKind::Io(ref err) if err.kind() == ::std::io::ErrorKind::InvalidData => false,
            _ => true
        }
    }
}
