use std::time::Duration;

use bytes::Bytes;
use magnus::{Error, Ruby, TryConvert, r_hash::RHash, value::ReprValue};

pub fn build_options(ruby: &Ruby, hash: RHash) -> Result<omq_tokio::Options, Error> {
    let mut opts = omq_tokio::Options::default();

    if let Some(v) = get_opt::<i64>(ruby, hash, "send_hwm")? {
        opts.send_hwm = v.max(0) as u32;
    }
    if let Some(v) = get_opt::<i64>(ruby, hash, "recv_hwm")? {
        opts.recv_hwm = v.max(0) as u32;
    }
    if let Some(v) = get_opt::<f64>(ruby, hash, "linger")? {
        opts.linger = if v.is_infinite() {
            None
        } else {
            Some(Duration::from_secs_f64(v))
        };
    }
    if let Some(v) = get_opt::<Vec<u8>>(ruby, hash, "identity")? {
        if !v.is_empty() {
            opts.identity = Bytes::from(v);
        }
    }
    if let Some(v) = get_opt::<bool>(ruby, hash, "router_mandatory")? {
        opts.router_mandatory = v;
    }
    if let Some(v) = get_opt::<bool>(ruby, hash, "conflate")? {
        opts.conflate = v;
    }
    if let Some(v) = get_opt_duration(ruby, hash, "heartbeat_interval")? {
        opts.heartbeat_interval = Some(v);
    }
    if let Some(v) = get_opt_duration(ruby, hash, "heartbeat_ttl")? {
        opts.heartbeat_ttl = Some(v);
    }
    if let Some(v) = get_opt_duration(ruby, hash, "heartbeat_timeout")? {
        opts.heartbeat_timeout = Some(v);
    }
    if let Some(v) = get_opt::<i64>(ruby, hash, "max_message_size")? {
        opts.max_message_size = Some(v as usize);
    }
    if let Some(v) = get_opt::<i64>(ruby, hash, "sndbuf")? {
        opts.send_buffer_size = Some(v as usize);
    }
    if let Some(v) = get_opt::<i64>(ruby, hash, "rcvbuf")? {
        opts.recv_buffer_size = Some(v as usize);
    }
    if let Some(v) = get_opt::<String>(ruby, hash, "on_mute")? {
        opts.on_mute = match v.as_str() {
            "drop_newest" | "drop" => omq_tokio::OnMute::DropNewest,
            _ => omq_tokio::OnMute::Block,
        };
    }

    if let Some(v) = get_opt::<f64>(ruby, hash, "reconnect_interval")? {
        opts.reconnect = omq_proto::options::ReconnectPolicy::Fixed(Duration::from_secs_f64(v));
    }
    if let Some(min) = get_opt::<f64>(ruby, hash, "reconnect_interval_min")? {
        let max = get_opt::<f64>(ruby, hash, "reconnect_interval_max")?.unwrap_or(min * 16.0);
        opts.reconnect = omq_proto::options::ReconnectPolicy::Exponential {
            min: Duration::from_secs_f64(min),
            max: Duration::from_secs_f64(max),
        };
    }

    if let Some(mech_type) = get_opt::<String>(ruby, hash, "mechanism_type")? {
        apply_mechanism(ruby, hash, &mech_type, &mut opts)?;
    }

    Ok(opts)
}

fn apply_mechanism(
    ruby: &Ruby,
    hash: RHash,
    mech_type: &str,
    opts: &mut omq_tokio::Options,
) -> Result<(), Error> {
    match mech_type {
        "null" => {}

        #[cfg(feature = "curve")]
        "curve" => {
            let is_server = get_opt::<bool>(ruby, hash, "mechanism_server")?.unwrap_or(false);
            let pub_key = get_opt_bytes(ruby, hash, "mechanism_public_key")?;
            let sec_key = get_opt_bytes(ruby, hash, "mechanism_secret_key")?;

            if is_server {
                if let (Some(pk), Some(sk)) = (pub_key, sec_key) {
                    let keypair = omq_proto::CurveKeypair {
                        public: omq_proto::CurvePublicKey::from_bytes(to_32(
                            ruby,
                            &pk,
                            "public key",
                        )?),
                        secret: omq_proto::CurveSecretKey::from_bytes(to_32(
                            ruby,
                            &sk,
                            "secret key",
                        )?),
                    };
                    opts.mechanism = omq_proto::MechanismSetup::CurveServer {
                        our_keypair: keypair,
                        cookie_keyring: std::sync::Arc::new(omq_proto::CurveCookieKeyring::new()),
                        authenticator: None,
                    };
                }
            } else {
                let srv_key = get_opt_bytes(ruby, hash, "mechanism_server_key")?;
                if let (Some(pk), Some(sk), Some(svk)) = (pub_key, sec_key, srv_key) {
                    let keypair = omq_proto::CurveKeypair {
                        public: omq_proto::CurvePublicKey::from_bytes(to_32(
                            ruby,
                            &pk,
                            "public key",
                        )?),
                        secret: omq_proto::CurveSecretKey::from_bytes(to_32(
                            ruby,
                            &sk,
                            "secret key",
                        )?),
                    };
                    opts.mechanism = omq_proto::MechanismSetup::CurveClient {
                        our_keypair: keypair,
                        server_public: omq_proto::CurvePublicKey::from_bytes(to_32(
                            ruby,
                            &svk,
                            "server key",
                        )?),
                    };
                }
            }
        }

        _ => {}
    }
    Ok(())
}

fn to_32(ruby: &Ruby, bytes: &[u8], label: &str) -> Result<[u8; 32], Error> {
    bytes.try_into().map_err(|_| {
        Error::new(
            ruby.exception_arg_error(),
            format!("{label} must be exactly 32 bytes, got {}", bytes.len()),
        )
    })
}

fn get_opt_bytes(ruby: &Ruby, hash: RHash, key: &str) -> Result<Option<Vec<u8>>, Error> {
    let k = ruby.str_new(key);
    match hash.get(k) {
        Some(v) => {
            if v.is_nil() {
                return Ok(None);
            }
            let s = magnus::r_string::RString::try_convert(v)?;
            Ok(Some(unsafe { s.as_slice() }.to_vec()))
        }
        None => Ok(None),
    }
}

fn get_opt<T: TryConvert>(ruby: &Ruby, hash: RHash, key: &str) -> Result<Option<T>, Error> {
    let k = ruby.str_new(key);
    match hash.get(k) {
        Some(v) => {
            if v.is_nil() {
                Ok(None)
            } else {
                Ok(Some(T::try_convert(v)?))
            }
        }
        None => Ok(None),
    }
}

fn get_opt_duration(ruby: &Ruby, hash: RHash, key: &str) -> Result<Option<Duration>, Error> {
    match get_opt::<f64>(ruby, hash, key)? {
        Some(v) => Ok(Some(Duration::from_secs_f64(v))),
        None => Ok(None),
    }
}
