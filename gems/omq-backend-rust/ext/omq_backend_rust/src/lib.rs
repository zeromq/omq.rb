mod error;
mod notify;
mod options;
mod rb;
mod runtime;
mod socket;

use rb_sys::VALUE;

use crate::rb::{RbResult, RubyErr};

fn set_io_threads_impl(n: VALUE) -> RbResult<VALUE> {
    let n = rb::value_to_i64(n)?;
    if n < 0 {
        return Err(RubyErr::arg("io_threads must be non-negative"));
    }
    let n = usize::try_from(n).map_err(|_| RubyErr::arg("io_threads too large"))?;
    socket::set_io_threads(n);
    Ok(rb::qnil())
}

unsafe extern "C" fn set_io_threads(_module: VALUE, n: VALUE) -> VALUE {
    rb::wrap(|| set_io_threads_impl(n))
}

#[unsafe(no_mangle)]
/// # Safety
///
/// Ruby calls this once while loading the native extension.
pub unsafe extern "C" fn Init_omq_backend_rust() {
    rb::wrap_init(init);
}

fn init() -> RbResult<()> {
    #[cfg(ruby_engine = "mri")]
    unsafe {
        rb_sys::rb_ext_ractor_safe(true);
    }

    let omq = unsafe { rb::define_module(c"OMQ")? };
    let rust = unsafe { rb::define_module_under(omq, c"Rust")? };
    let native = unsafe { rb::define_module_under(rust, c"Native")? };

    unsafe {
        rb::define_module_function_1(native, c"io_threads=", set_io_threads)?;
    }

    socket::register(native)?;

    Ok(())
}
