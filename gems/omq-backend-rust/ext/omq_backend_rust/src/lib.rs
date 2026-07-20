mod error;
mod notify;
mod options;
mod runtime;
mod socket;

use magnus::{Error, Ruby, function, prelude::*};

fn set_io_threads(n: usize) {
    socket::set_io_threads(n);
}

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let omq = ruby.define_module("OMQ")?;
    let rust = omq.define_module("Rust")?;
    let native = rust.define_module("Native")?;

    native.define_module_function("io_threads=", function!(set_io_threads, 1))?;

    socket::register(ruby)?;

    Ok(())
}
