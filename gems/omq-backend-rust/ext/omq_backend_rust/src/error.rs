use magnus::{Error, Ruby};
use omq_proto::error::Error as OmqError;

pub fn map_err(ruby: &Ruby, e: OmqError) -> Error {
    match e {
        OmqError::Closed => Error::new(ruby.exception_io_error(), "socket closed"),
        OmqError::Timeout => Error::new(ruby.exception_runtime_error(), "operation timed out"),
        OmqError::Unroutable => Error::new(ruby.exception_runtime_error(), "no route to peer"),
        OmqError::InvalidEndpoint(msg) => Error::new(ruby.exception_arg_error(), msg),
        OmqError::Protocol(msg) => Error::new(ruby.exception_runtime_error(), msg),
        OmqError::Io(e) => Error::new(ruby.exception_runtime_error(), e.to_string()),
        OmqError::HandshakeFailed(msg) => Error::new(ruby.exception_runtime_error(), msg),
        _ => Error::new(ruby.exception_runtime_error(), format!("{e}")),
    }
}
