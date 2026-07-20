use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};

pub struct PipeNotify {
    read_fd: RawFd,
    write_fd: RawFd,
    parking: AtomicBool,
}

unsafe impl Send for PipeNotify {}
unsafe impl Sync for PipeNotify {}

impl PipeNotify {
    pub fn new() -> Self {
        let mut fds = [0i32; 2];
        let ret = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) };
        assert!(
            ret == 0,
            "pipe2 failed: {}",
            std::io::Error::last_os_error()
        );
        Self {
            read_fd: fds[0],
            write_fd: fds[1],
            parking: AtomicBool::new(false),
        }
    }

    pub fn notify(&self) {
        if self.parking.load(Ordering::Acquire) {
            self.write_byte();
        }
    }

    pub fn force_wake(&self) {
        self.write_byte();
    }

    pub fn read_fd(&self) -> RawFd {
        self.read_fd
    }

    pub fn park_begin(&self) {
        self.parking.store(true, Ordering::Release);
    }

    fn write_byte(&self) {
        let val: u8 = 1;
        loop {
            let ret =
                unsafe { libc::write(self.write_fd, &val as *const u8 as *const libc::c_void, 1) };
            if ret >= 0 {
                break;
            }
            if std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break;
            }
        }
    }
}

impl Drop for PipeNotify {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.read_fd);
            libc::close(self.write_fd);
        }
    }
}
