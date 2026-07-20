use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use omq_tokio::Socket as InnerSocket;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use crate::notify::PipeNotify;

type Job = Box<dyn FnOnce() + Send + 'static>;

struct RuntimeState {
    pid: u32,
    handle: Handle,
    submit: flume::Sender<Job>,
}

static RUNTIME: Mutex<Option<RuntimeState>> = Mutex::new(None);
static TERMINATED: AtomicBool = AtomicBool::new(false);

pub fn ensure_runtime(io_threads: usize) -> Handle {
    if TERMINATED.load(Ordering::Acquire) {
        panic!("omq-backend-rust: runtime terminated");
    }
    let mut guard = RUNTIME.lock().unwrap();
    let pid = std::process::id();
    if let Some(ref rt) = *guard {
        if rt.pid == pid {
            return rt.handle.clone();
        }
    }
    let (tx, rx) = flume::unbounded::<Job>();
    let (handle_tx, handle_rx) = flume::bounded::<Handle>(1);
    let n = io_threads.max(1);
    thread::Builder::new()
        .name("omq-rust-tokio".into())
        .spawn(move || {
            let rt = if n <= 1 {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("omq-backend-rust: tokio runtime build")
            } else {
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(n)
                    .enable_all()
                    .build()
                    .expect("omq-backend-rust: tokio runtime build")
            };
            let _ = handle_tx.send(rt.handle().clone());
            rt.block_on(async move {
                while let Ok(job) = rx.recv_async().await {
                    job();
                }
            });
        })
        .expect("omq-backend-rust: spawn tokio thread");
    let handle = handle_rx.recv().expect("omq-backend-rust: runtime handle");
    *guard = Some(RuntimeState {
        pid,
        handle: handle.clone(),
        submit: tx,
    });
    handle
}

fn submit_job(io_threads: usize) -> flume::Sender<Job> {
    let guard = RUNTIME.lock().unwrap();
    if let Some(ref rt) = *guard {
        if rt.pid == std::process::id() {
            return rt.submit.clone();
        }
    }
    drop(guard);
    ensure_runtime(io_threads);
    RUNTIME.lock().unwrap().as_ref().unwrap().submit.clone()
}

pub fn spawn_blocking<F, T>(io_threads: usize, fut: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let handle = ensure_runtime(io_threads);
    let (otx, orx) = flume::bounded::<T>(1);
    handle.spawn(async move {
        let out = fut.await;
        let _ = otx.send(out);
    });

    struct RecvBox<U> {
        rx: flume::Receiver<U>,
        result: Option<U>,
    }

    extern "C" fn blocking_recv<U>(data: *mut libc::c_void) -> *mut libc::c_void {
        let rd = unsafe { &mut *(data as *mut RecvBox<U>) };
        rd.result = rd.rx.recv().ok();
        std::ptr::null_mut()
    }

    let mut rd = RecvBox {
        rx: orx,
        result: None,
    };
    unsafe {
        rb_sys::rb_thread_call_without_gvl(
            Some(blocking_recv::<T>),
            &mut rd as *mut RecvBox<T> as *mut libc::c_void,
            None,
            std::ptr::null_mut(),
        );
    }
    rd.result.expect("omq-backend-rust: runtime dropped result")
}

pub struct Materialized {
    pub socket: Arc<InnerSocket>,

    pub send_prod: Mutex<yring::AsyncProducer<omq_tokio::Message>>,
    pub recv_cons: Mutex<yring::Consumer<omq_tokio::Message>>,
    pub recv_notify: Arc<PipeNotify>,
    pub send_notify: Arc<PipeNotify>,
    pub recv_space: Arc<tokio::sync::Notify>,
    pub send_pump: JoinHandle<()>,
    pub recv_pump: JoinHandle<()>,

    pub monitor_rx: flume::Receiver<MonitorEventData>,
    pub monitor_notify: Arc<PipeNotify>,
    pub peer_connected_notify: Arc<PipeNotify>,
    pub all_peers_gone_notify: Arc<PipeNotify>,
    pub subscriber_joined_notify: Arc<PipeNotify>,
    pub monitor_pump: JoinHandle<()>,
}

#[derive(Clone)]
pub struct MonitorEventData {
    pub event_type: &'static str,
    pub endpoint: Option<String>,
    pub detail: Vec<(&'static str, String)>,
}

fn convert_monitor_event(event: &omq_tokio::MonitorEvent) -> MonitorEventData {
    use omq_tokio::MonitorEvent::*;
    match event {
        Listening { endpoint } => MonitorEventData {
            event_type: "listening",
            endpoint: Some(endpoint.to_string()),
            detail: vec![],
        },
        Accepted {
            endpoint,
            connection_id,
            ..
        } => MonitorEventData {
            event_type: "accepted",
            endpoint: Some(endpoint.to_string()),
            detail: vec![("connection_id", connection_id.to_string())],
        },
        Connected {
            endpoint,
            connection_id,
            ..
        } => MonitorEventData {
            event_type: "connected",
            endpoint: Some(endpoint.to_string()),
            detail: vec![("connection_id", connection_id.to_string())],
        },
        HandshakeSucceeded { endpoint, peer } => {
            let mut detail = vec![("connection_id", peer.connection_id.to_string())];
            if let Some(ref ident) = peer.peer_identity {
                if !ident.is_empty() {
                    detail.push(("identity", format!("{:?}", ident)));
                }
            }
            MonitorEventData {
                event_type: "handshake_succeeded",
                endpoint: Some(endpoint.to_string()),
                detail,
            }
        }
        HandshakeFailed {
            endpoint, reason, ..
        } => MonitorEventData {
            event_type: "handshake_failed",
            endpoint: Some(endpoint.to_string()),
            detail: vec![("reason", reason.clone())],
        },
        ConnectDelayed {
            endpoint,
            retry_in,
            attempt,
        } => MonitorEventData {
            event_type: "connect_delayed",
            endpoint: Some(endpoint.to_string()),
            detail: vec![
                ("interval", format!("{:.3}", retry_in.as_secs_f64())),
                ("attempt", attempt.to_string()),
            ],
        },
        Disconnected {
            endpoint,
            peer,
            reason,
        } => MonitorEventData {
            event_type: "disconnected",
            endpoint: Some(endpoint.to_string()),
            detail: vec![
                ("reason", format!("{reason:?}")),
                ("connection_id", peer.connection_id.to_string()),
            ],
        },
        SubscribeReceived { prefix } => MonitorEventData {
            event_type: "subscribe_received",
            endpoint: None,
            detail: vec![("prefix", String::from_utf8_lossy(prefix).into_owned())],
        },
        UnsubscribeReceived { prefix } => MonitorEventData {
            event_type: "unsubscribe_received",
            endpoint: None,
            detail: vec![("prefix", String::from_utf8_lossy(prefix).into_owned())],
        },
        JoinReceived { group } => MonitorEventData {
            event_type: "join_received",
            endpoint: None,
            detail: vec![("group", String::from_utf8_lossy(group).into_owned())],
        },
        LeaveReceived { group } => MonitorEventData {
            event_type: "leave_received",
            endpoint: None,
            detail: vec![("group", String::from_utf8_lossy(group).into_owned())],
        },
        Closed => MonitorEventData {
            event_type: "closed",
            endpoint: None,
            detail: vec![],
        },
        _ => MonitorEventData {
            event_type: "unknown",
            endpoint: None,
            detail: vec![],
        },
    }
}

async fn push_to_ring(
    recv_prod: &mut yring::Producer<omq_tokio::Message>,
    msg: omq_tokio::Message,
    recv_space: &tokio::sync::Notify,
) {
    let mut m = msg;
    loop {
        match recv_prod.push(m) {
            Ok(()) => break,
            Err(returned) => {
                recv_prod.flush();
                m = returned;
                let notified = recv_space.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                match recv_prod.push(m) {
                    Ok(()) => break,
                    Err(r2) => {
                        m = r2;
                        notified.await;
                    }
                }
            }
        }
    }
}

#[expect(clippy::too_many_arguments)]
pub fn materialize(
    io_threads: usize,
    socket_type: omq_tokio::SocketType,
    options: omq_tokio::Options,
    send_cons: yring::AsyncConsumer<omq_tokio::Message>,
    mut recv_prod: yring::Producer<omq_tokio::Message>,
    recv_notify: Arc<PipeNotify>,
    send_notify: Arc<PipeNotify>,
    recv_space: Arc<tokio::sync::Notify>,
    monitor_tx: flume::Sender<MonitorEventData>,
    monitor_notify: Arc<PipeNotify>,
    peer_connected_notify: Arc<PipeNotify>,
    all_peers_gone_notify: Arc<PipeNotify>,
    subscriber_joined_notify: Arc<PipeNotify>,
) -> (
    Arc<InnerSocket>,
    JoinHandle<()>,
    JoinHandle<()>,
    JoinHandle<()>,
) {
    let (otx, orx) = flume::bounded(1);
    let tx = submit_job(io_threads);
    let job: Job = Box::new(move || {
        let sock = Arc::new(InnerSocket::new(socket_type, options));

        const SEND_YIELD_INTERVAL: u32 = 256;
        let s = sock.clone();
        let sn = send_notify.clone();
        let send_pump = tokio::spawn(async move {
            futures::pin_mut!(send_cons);
            let mut batch = 0u32;
            while let Some(msg) = futures::StreamExt::next(&mut send_cons).await {
                let _ = s.send(msg).await;
                sn.notify();
                batch += 1;
                if batch >= SEND_YIELD_INTERVAL {
                    batch = 0;
                    tokio::task::yield_now().await;
                }
            }
            sn.notify();
        });

        let s = sock.clone();
        let rn = recv_notify.clone();
        let rs = recv_space.clone();
        let recv_pump = tokio::spawn(async move {
            loop {
                match s.recv().await {
                    Ok(msg) => {
                        push_to_ring(&mut recv_prod, msg, &rs).await;

                        while !recv_prod.is_full() {
                            match s.try_recv() {
                                Ok(msg) => push_to_ring(&mut recv_prod, msg, &rs).await,
                                Err(_) => break,
                            }
                        }

                        recv_prod.flush();
                        rn.notify();
                    }
                    Err(omq_tokio::Error::Closed) => break,
                    Err(_) => continue,
                }
            }
        });

        let monitor_sock = sock.clone();
        let monitor_pump = tokio::spawn(async move {
            let mut stream = monitor_sock.monitor();
            let mut peer_count: u32 = 0;
            let mut had_peers = false;
            let mut peer_connected_fired = false;
            let mut subscriber_joined_fired = false;

            loop {
                match stream.recv().await {
                    Ok(event) => {
                        match &event {
                            omq_tokio::MonitorEvent::HandshakeSucceeded { .. } => {
                                peer_count += 1;
                                had_peers = true;
                                if !peer_connected_fired {
                                    peer_connected_fired = true;
                                    peer_connected_notify.force_wake();
                                }
                            }
                            omq_tokio::MonitorEvent::Disconnected { .. } => {
                                peer_count = peer_count.saturating_sub(1);
                                if had_peers && peer_count == 0 {
                                    all_peers_gone_notify.force_wake();
                                }
                            }
                            omq_tokio::MonitorEvent::SubscribeReceived { .. } => {
                                if !subscriber_joined_fired {
                                    subscriber_joined_fired = true;
                                    subscriber_joined_notify.force_wake();
                                }
                            }
                            _ => {}
                        }

                        let data = convert_monitor_event(&event);
                        let _ = monitor_tx.try_send(data);
                        monitor_notify.notify();
                    }
                    Err(omq_tokio::MonitorRecvError::Lagged(_)) => continue,
                    Err(omq_tokio::MonitorRecvError::Closed) => break,
                    Err(_) => break,
                }
            }
        });

        let _ = otx.send((sock, send_pump, recv_pump, monitor_pump));
    });
    tx.send(job).expect("omq-backend-rust: tokio runtime gone");

    struct RecvBox {
        rx: flume::Receiver<(
            Arc<InnerSocket>,
            JoinHandle<()>,
            JoinHandle<()>,
            JoinHandle<()>,
        )>,
        result: Option<(
            Arc<InnerSocket>,
            JoinHandle<()>,
            JoinHandle<()>,
            JoinHandle<()>,
        )>,
    }

    extern "C" fn blocking_recv(data: *mut libc::c_void) -> *mut libc::c_void {
        let rd = unsafe { &mut *(data as *mut RecvBox) };
        rd.result = rd.rx.recv().ok();
        std::ptr::null_mut()
    }

    let mut rd = RecvBox {
        rx: orx,
        result: None,
    };
    unsafe {
        rb_sys::rb_thread_call_without_gvl(
            Some(blocking_recv),
            &mut rd as *mut RecvBox as *mut libc::c_void,
            None,
            std::ptr::null_mut(),
        );
    }
    rd.result.expect("omq-backend-rust: materialize failed")
}

pub fn destroy_socket(
    io_threads: usize,
    sock: Arc<InnerSocket>,
    send_prod: Mutex<yring::AsyncProducer<omq_tokio::Message>>,
    send_pump: JoinHandle<()>,
    recv_pump: JoinHandle<()>,
    monitor_pump: JoinHandle<()>,
    linger: Option<Duration>,
) {
    recv_pump.abort();
    monitor_pump.abort();
    send_pump.abort();
    drop(send_prod);
    let Ok(handle) = (|| -> std::result::Result<Handle, ()> { Ok(ensure_runtime(io_threads)) })()
    else {
        return;
    };
    let close_timeout = linger
        .unwrap_or(Duration::from_secs(30))
        .max(Duration::from_millis(10));
    handle.spawn(async move {
        let s = Arc::try_unwrap(sock).unwrap_or_else(|arc| (*arc).clone());
        let _ = tokio::time::timeout(close_timeout, s.close()).await;
    });
}
