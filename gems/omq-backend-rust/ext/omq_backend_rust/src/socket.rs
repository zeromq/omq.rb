use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;
use magnus::r_hash::RHash;
use magnus::{Error, Ruby, function, method, prelude::*, r_array::RArray, r_string::RString};

use crate::error::map_err;
use crate::notify::PipeNotify;
use crate::runtime::{self, Materialized};

static IO_THREADS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);

pub fn set_io_threads(n: usize) {
    IO_THREADS.store(n, Ordering::Relaxed);
}

fn io_threads() -> usize {
    IO_THREADS.load(Ordering::Relaxed)
}

#[magnus::wrap(class = "OMQ::Rust::Native::RustSocket", free_immediately, size)]
pub struct RustSocket {
    socket_type: omq_tokio::SocketType,
    options: Mutex<Option<omq_tokio::Options>>,
    materialized: RwLock<Option<Materialized>>,
    closed: AtomicBool,
    linger: Mutex<Option<std::time::Duration>>,
}

unsafe impl Send for RustSocket {}
unsafe impl Sync for RustSocket {}

fn parse_socket_type(s: &str) -> Result<omq_tokio::SocketType, String> {
    match s {
        "REQ" => Ok(omq_tokio::SocketType::Req),
        "REP" => Ok(omq_tokio::SocketType::Rep),
        "PUB" => Ok(omq_tokio::SocketType::Pub),
        "SUB" => Ok(omq_tokio::SocketType::Sub),
        "XPUB" => Ok(omq_tokio::SocketType::XPub),
        "XSUB" => Ok(omq_tokio::SocketType::XSub),
        "PUSH" => Ok(omq_tokio::SocketType::Push),
        "PULL" => Ok(omq_tokio::SocketType::Pull),
        "DEALER" => Ok(omq_tokio::SocketType::Dealer),
        "ROUTER" => Ok(omq_tokio::SocketType::Router),
        "PAIR" => Ok(omq_tokio::SocketType::Pair),
        "CLIENT" => Ok(omq_tokio::SocketType::Client),
        "SERVER" => Ok(omq_tokio::SocketType::Server),
        "RADIO" => Ok(omq_tokio::SocketType::Radio),
        "DISH" => Ok(omq_tokio::SocketType::Dish),
        "SCATTER" => Ok(omq_tokio::SocketType::Scatter),
        "GATHER" => Ok(omq_tokio::SocketType::Gather),
        "CHANNEL" => Ok(omq_tokio::SocketType::Channel),
        "PEER" => Ok(omq_tokio::SocketType::Peer),
        _ => Err(format!("unknown socket type: {s}")),
    }
}

fn rust_socket_new(ruby: &Ruby, type_str: String) -> Result<RustSocket, Error> {
    let st = parse_socket_type(&type_str).map_err(|e| Error::new(ruby.exception_arg_error(), e))?;
    Ok(RustSocket {
        socket_type: st,
        options: Mutex::new(None),
        materialized: RwLock::new(None),
        closed: AtomicBool::new(false),
        linger: Mutex::new(None),
    })
}

fn rust_socket_set_options(ruby: &Ruby, rb_self: &RustSocket, hash: RHash) -> Result<(), Error> {
    let opts = crate::options::build_options(ruby, hash)?;
    *rb_self.linger.lock().unwrap() = opts.linger;
    *rb_self.options.lock().unwrap() = Some(opts);
    Ok(())
}

fn rust_socket_materialize(ruby: &Ruby, rb_self: &RustSocket) -> Result<(), Error> {
    if rb_self.closed.load(Ordering::Relaxed) {
        return Err(Error::new(ruby.exception_io_error(), "socket closed"));
    }
    {
        let slot = rb_self.materialized.read().unwrap();
        if slot.is_some() {
            return Ok(());
        }
    }
    let mut slot = rb_self.materialized.write().unwrap();
    if slot.is_some() {
        return Ok(());
    }

    let opts = rb_self.options.lock().unwrap().take().unwrap_or_default();
    let send_cap = opts.send_hwm.max(1) as usize;
    let recv_cap = opts.recv_hwm.max(1) as usize;
    let (send_prod, send_cons) = yring::async_spsc(send_cap);
    let (recv_prod, recv_cons) = yring::spsc(recv_cap);
    let recv_notify = Arc::new(PipeNotify::new());
    let send_notify = Arc::new(PipeNotify::new());
    let recv_space = Arc::new(tokio::sync::Notify::new());

    let (monitor_tx, monitor_rx) = flume::bounded(64);
    let monitor_notify = Arc::new(PipeNotify::new());
    let peer_connected_notify = Arc::new(PipeNotify::new());
    let all_peers_gone_notify = Arc::new(PipeNotify::new());
    let subscriber_joined_notify = Arc::new(PipeNotify::new());

    let (socket, send_pump, recv_pump, monitor_pump) = runtime::materialize(
        io_threads(),
        rb_self.socket_type,
        opts,
        send_cons,
        recv_prod,
        recv_notify.clone(),
        send_notify.clone(),
        recv_space.clone(),
        monitor_tx,
        monitor_notify.clone(),
        peer_connected_notify.clone(),
        all_peers_gone_notify.clone(),
        subscriber_joined_notify.clone(),
    );

    *slot = Some(Materialized {
        socket,
        send_prod: Mutex::new(send_prod),
        recv_cons: Mutex::new(recv_cons),
        recv_notify,
        send_notify,
        recv_space,
        send_pump,
        recv_pump,
        monitor_rx,
        monitor_notify,
        peer_connected_notify,
        all_peers_gone_notify,
        subscriber_joined_notify,
        monitor_pump,
    });
    Ok(())
}

fn rust_socket_bind(ruby: &Ruby, rb_self: &RustSocket, endpoint: String) -> Result<String, Error> {
    let sock = ensure_socket(ruby, rb_self)?;
    let ep = omq_tokio::Endpoint::from_str(&endpoint).map_err(|e| map_err(ruby, e))?;
    let result = runtime::spawn_blocking(io_threads(), async move { sock.bind(ep).await });
    result
        .map(|ep| ep.to_string())
        .map_err(|e| map_err(ruby, e))
}

fn rust_socket_connect(ruby: &Ruby, rb_self: &RustSocket, endpoint: String) -> Result<(), Error> {
    let sock = ensure_socket(ruby, rb_self)?;
    let ep = omq_tokio::Endpoint::from_str(&endpoint).map_err(|e| map_err(ruby, e))?;
    let result = runtime::spawn_blocking(io_threads(), async move { sock.connect(ep).await });
    result.map_err(|e| map_err(ruby, e))
}

fn rust_socket_disconnect(
    ruby: &Ruby,
    rb_self: &RustSocket,
    endpoint: String,
) -> Result<(), Error> {
    let sock = ensure_socket(ruby, rb_self)?;
    let ep = omq_tokio::Endpoint::from_str(&endpoint).map_err(|e| map_err(ruby, e))?;
    let result = runtime::spawn_blocking(io_threads(), async move { sock.disconnect(ep).await });
    result.map_err(|e| map_err(ruby, e))
}

fn rust_socket_unbind(ruby: &Ruby, rb_self: &RustSocket, endpoint: String) -> Result<(), Error> {
    let sock = ensure_socket(ruby, rb_self)?;
    let ep = omq_tokio::Endpoint::from_str(&endpoint).map_err(|e| map_err(ruby, e))?;
    let result = runtime::spawn_blocking(io_threads(), async move { sock.unbind(ep).await });
    result.map_err(|e| map_err(ruby, e))
}

fn rust_socket_enqueue_send(
    ruby: &Ruby,
    rb_self: &RustSocket,
    parts: RArray,
) -> Result<magnus::Symbol, Error> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let mat = mat_guard
        .as_ref()
        .ok_or_else(|| Error::new(ruby.exception_runtime_error(), "socket not materialized"))?;

    let msg = ruby_parts_to_message(ruby, parts)?;
    let mut prod = mat.send_prod.lock().unwrap();
    match prod.push(msg) {
        Ok(()) => {
            prod.flush();
            Ok(ruby.to_symbol("ok"))
        }
        Err(returned) => {
            prod.flush();
            match prod.push(returned) {
                Ok(()) => {
                    prod.flush();
                    Ok(ruby.to_symbol("ok"))
                }
                Err(_) => Ok(ruby.to_symbol("full")),
            }
        }
    }
}

fn rust_socket_try_recv(ruby: &Ruby, rb_self: &RustSocket) -> Result<Option<RArray>, Error> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let mat = match mat_guard.as_ref() {
        Some(m) => m,
        None => return Ok(None),
    };

    let mut cons = mat.recv_cons.lock().unwrap();
    match cons.prefetch_and_pop() {
        Some(msg) => {
            mat.recv_space.notify_one();
            Ok(Some(message_to_ruby_parts(ruby, msg)?))
        }
        None => Ok(None),
    }
}

fn rust_socket_try_recv_batch(ruby: &Ruby, rb_self: &RustSocket) -> Result<Option<RArray>, Error> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let mat = match mat_guard.as_ref() {
        Some(m) => m,
        None => return Ok(None),
    };

    let mut cons = mat.recv_cons.lock().unwrap();
    let count = cons.prefetch();
    if count == 0 {
        return Ok(None);
    }

    let batch = ruby.ary_new_capa(count);
    let mut popped = 0usize;
    while let Some(msg) = cons.pop() {
        batch.push(message_to_ruby_parts(ruby, msg)?)?;
        popped += 1;
    }
    cons.release();

    if popped > 0 {
        mat.recv_space.notify_one();
        Ok(Some(batch))
    } else {
        Ok(None)
    }
}

fn rust_socket_recv_fd(ruby: &Ruby, rb_self: &RustSocket) -> Result<i32, Error> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let mat = mat_guard
        .as_ref()
        .ok_or_else(|| Error::new(ruby.exception_runtime_error(), "socket not materialized"))?;
    mat.recv_notify.park_begin();
    Ok(mat.recv_notify.read_fd())
}

fn rust_socket_send_fd(ruby: &Ruby, rb_self: &RustSocket) -> Result<i32, Error> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let mat = mat_guard
        .as_ref()
        .ok_or_else(|| Error::new(ruby.exception_runtime_error(), "socket not materialized"))?;
    mat.send_notify.park_begin();
    Ok(mat.send_notify.read_fd())
}

fn rust_socket_peer_connected_fd(ruby: &Ruby, rb_self: &RustSocket) -> Result<i32, Error> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let mat = mat_guard
        .as_ref()
        .ok_or_else(|| Error::new(ruby.exception_runtime_error(), "socket not materialized"))?;
    Ok(mat.peer_connected_notify.read_fd())
}

fn rust_socket_all_peers_gone_fd(ruby: &Ruby, rb_self: &RustSocket) -> Result<i32, Error> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let mat = mat_guard
        .as_ref()
        .ok_or_else(|| Error::new(ruby.exception_runtime_error(), "socket not materialized"))?;
    Ok(mat.all_peers_gone_notify.read_fd())
}

fn rust_socket_subscriber_joined_fd(ruby: &Ruby, rb_self: &RustSocket) -> Result<i32, Error> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let mat = mat_guard
        .as_ref()
        .ok_or_else(|| Error::new(ruby.exception_runtime_error(), "socket not materialized"))?;
    Ok(mat.subscriber_joined_notify.read_fd())
}

fn rust_socket_monitor_fd(ruby: &Ruby, rb_self: &RustSocket) -> Result<i32, Error> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let mat = mat_guard
        .as_ref()
        .ok_or_else(|| Error::new(ruby.exception_runtime_error(), "socket not materialized"))?;
    mat.monitor_notify.park_begin();
    Ok(mat.monitor_notify.read_fd())
}

fn rust_socket_try_recv_monitor(ruby: &Ruby, rb_self: &RustSocket) -> Result<Option<RHash>, Error> {
    let mat_guard = rb_self.materialized.read().unwrap();
    let mat = match mat_guard.as_ref() {
        Some(m) => m,
        None => return Ok(None),
    };

    match mat.monitor_rx.try_recv() {
        Ok(data) => {
            let hash = ruby.hash_new();
            hash.aset(ruby.to_symbol("type"), ruby.to_symbol(data.event_type))?;
            if let Some(ep) = data.endpoint {
                hash.aset(ruby.to_symbol("endpoint"), ruby.str_new(&ep))?;
            }
            if !data.detail.is_empty() {
                let detail = ruby.hash_new();
                for (k, v) in &data.detail {
                    detail.aset(ruby.to_symbol(k), ruby.str_new(v))?;
                }
                hash.aset(ruby.to_symbol("detail"), detail)?;
            }
            Ok(Some(hash))
        }
        Err(_) => Ok(None),
    }
}

fn rust_socket_subscribe(ruby: &Ruby, rb_self: &RustSocket, prefix: RString) -> Result<(), Error> {
    let sock = ensure_socket(ruby, rb_self)?;
    let bytes = Bytes::from(unsafe { prefix.as_slice() }.to_vec());
    let result = runtime::spawn_blocking(io_threads(), async move { sock.subscribe(bytes).await });
    result.map_err(|e| map_err(ruby, e))
}

fn rust_socket_unsubscribe(
    ruby: &Ruby,
    rb_self: &RustSocket,
    prefix: RString,
) -> Result<(), Error> {
    let sock = ensure_socket(ruby, rb_self)?;
    let bytes = Bytes::from(unsafe { prefix.as_slice() }.to_vec());
    let result =
        runtime::spawn_blocking(io_threads(), async move { sock.unsubscribe(bytes).await });
    result.map_err(|e| map_err(ruby, e))
}

fn rust_socket_join(ruby: &Ruby, rb_self: &RustSocket, group: RString) -> Result<(), Error> {
    let sock = ensure_socket(ruby, rb_self)?;
    let bytes = Bytes::from(unsafe { group.as_slice() }.to_vec());
    let result = runtime::spawn_blocking(io_threads(), async move { sock.join(bytes).await });
    result.map_err(|e| map_err(ruby, e))
}

fn rust_socket_leave(ruby: &Ruby, rb_self: &RustSocket, group: RString) -> Result<(), Error> {
    let sock = ensure_socket(ruby, rb_self)?;
    let bytes = Bytes::from(unsafe { group.as_slice() }.to_vec());
    let result = runtime::spawn_blocking(io_threads(), async move { sock.leave(bytes).await });
    result.map_err(|e| map_err(ruby, e))
}

fn rust_socket_close(rb_self: &RustSocket) {
    rb_self.closed.store(true, Ordering::Relaxed);
    let mat = rb_self.materialized.write().unwrap().take();
    if let Some(m) = mat {
        m.recv_notify.force_wake();
        m.send_notify.force_wake();
        m.peer_connected_notify.force_wake();
        m.all_peers_gone_notify.force_wake();
        m.subscriber_joined_notify.force_wake();
        m.monitor_notify.force_wake();
        let linger = *rb_self.linger.lock().unwrap();
        runtime::destroy_socket(
            io_threads(),
            m.socket,
            m.send_prod,
            m.send_pump,
            m.recv_pump,
            m.monitor_pump,
            linger,
        );
    }
}

fn rust_socket_closed(rb_self: &RustSocket) -> bool {
    rb_self.closed.load(Ordering::Relaxed)
}

fn rust_socket_type_name(rb_self: &RustSocket) -> &'static str {
    rb_self.socket_type.as_str()
}

fn ensure_socket(ruby: &Ruby, rb_self: &RustSocket) -> Result<Arc<omq_tokio::Socket>, Error> {
    let slot = rb_self.materialized.read().unwrap();
    slot.as_ref()
        .map(|m| m.socket.clone())
        .ok_or_else(|| Error::new(ruby.exception_runtime_error(), "socket not materialized"))
}

fn ruby_parts_to_message(_ruby: &Ruby, parts: RArray) -> Result<omq_tokio::Message, Error> {
    let len = parts.len();
    if len == 1 {
        let part: RString = parts.entry(0)?;
        let data = unsafe { part.as_slice() }.to_vec();
        Ok(omq_tokio::Message::from_slice(&data))
    } else {
        let mut frames: Vec<Bytes> = Vec::with_capacity(len);
        for i in 0..len {
            let part: RString = parts.entry(i as isize)?;
            let data = unsafe { part.as_slice() }.to_vec();
            frames.push(Bytes::from(data));
        }
        Ok(omq_tokio::Message::multipart(frames))
    }
}

fn message_to_ruby_parts(ruby: &Ruby, msg: omq_tokio::Message) -> Result<RArray, Error> {
    let arr = ruby.ary_new();
    for part in msg.iter() {
        let s = ruby.str_from_slice(&part);
        s.freeze();
        arr.push(s)?;
    }
    Ok(arr)
}

pub fn register(ruby: &Ruby) -> Result<(), Error> {
    let omq = ruby.define_module("OMQ")?;
    let rust = omq.define_module("Rust")?;
    let native = rust.define_module("Native")?;

    let class = native.define_class("RustSocket", ruby.class_object())?;
    class.define_singleton_method("new", function!(rust_socket_new, 1))?;
    class.define_method("set_options", method!(rust_socket_set_options, 1))?;
    class.define_method("materialize", method!(rust_socket_materialize, 0))?;
    class.define_method("bind", method!(rust_socket_bind, 1))?;
    class.define_method("connect", method!(rust_socket_connect, 1))?;
    class.define_method("disconnect", method!(rust_socket_disconnect, 1))?;
    class.define_method("unbind", method!(rust_socket_unbind, 1))?;
    class.define_method("enqueue_send", method!(rust_socket_enqueue_send, 1))?;
    class.define_method("try_recv", method!(rust_socket_try_recv, 0))?;
    class.define_method("try_recv_batch", method!(rust_socket_try_recv_batch, 0))?;
    class.define_method("recv_fd", method!(rust_socket_recv_fd, 0))?;
    class.define_method("send_fd", method!(rust_socket_send_fd, 0))?;
    class.define_method(
        "peer_connected_fd",
        method!(rust_socket_peer_connected_fd, 0),
    )?;
    class.define_method(
        "all_peers_gone_fd",
        method!(rust_socket_all_peers_gone_fd, 0),
    )?;
    class.define_method(
        "subscriber_joined_fd",
        method!(rust_socket_subscriber_joined_fd, 0),
    )?;
    class.define_method("monitor_fd", method!(rust_socket_monitor_fd, 0))?;
    class.define_method("try_recv_monitor", method!(rust_socket_try_recv_monitor, 0))?;
    class.define_method("subscribe", method!(rust_socket_subscribe, 1))?;
    class.define_method("unsubscribe", method!(rust_socket_unsubscribe, 1))?;
    class.define_method("join", method!(rust_socket_join, 1))?;
    class.define_method("leave", method!(rust_socket_leave, 1))?;
    class.define_method("close", method!(rust_socket_close, 0))?;
    class.define_method("closed?", method!(rust_socket_closed, 0))?;
    class.define_method("socket_type_name", method!(rust_socket_type_name, 0))?;

    Ok(())
}
