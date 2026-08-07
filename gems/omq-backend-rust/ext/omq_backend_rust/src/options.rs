use std::time::Duration;

use bytes::Bytes;

use rb_sys::VALUE;

use crate::rb::{self, RbResult, RubyErr};

pub fn build_options(hash: VALUE) -> RbResult<omq_tokio::Options> {
    rb::check_hash(hash)?;

    let mut opts = omq_tokio::Options::default();

    if let Some(v) = get_opt_i64(hash, "send_hwm")? {
        opts.send_hwm = v.max(0) as u32;
    }
    if let Some(v) = get_opt_i64(hash, "recv_hwm")? {
        opts.recv_hwm = v.max(0) as u32;
    }
    if let Some(v) = get_opt_f64(hash, "linger")? {
        opts.linger = if v.is_infinite() && v.is_sign_positive() {
            None
        } else {
            Some(duration_from_seconds("linger", v)?)
        };
    }
    if let Some(v) = get_opt_bytes(hash, "identity")?
        && !v.is_empty()
    {
        opts.identity = Bytes::from(v);
    }
    if let Some(v) = get_opt_bool(hash, "router_mandatory")? {
        opts.router_mandatory = v;
    }
    if let Some(v) = get_opt_bool(hash, "conflate")? {
        opts.conflate = v;
    }
    if let Some(v) = get_opt_duration(hash, "heartbeat_interval")? {
        opts.heartbeat_interval = Some(v);
    }
    if let Some(v) = get_opt_duration(hash, "heartbeat_ttl")? {
        opts.heartbeat_ttl = Some(v);
    }
    if let Some(v) = get_opt_duration(hash, "heartbeat_timeout")? {
        opts.heartbeat_timeout = Some(v);
    }
    if let Some(v) = get_opt_usize(hash, "max_message_size")? {
        opts.max_message_size = Some(v);
    }
    if let Some(v) = get_opt_usize(hash, "sndbuf")? {
        opts.send_buffer_size = Some(v);
    }
    if let Some(v) = get_opt_usize(hash, "rcvbuf")? {
        opts.recv_buffer_size = Some(v);
    }
    if let Some(v) = get_opt_bytes(hash, "compression_dict")?
        && !v.is_empty()
    {
        opts.compression_dict = Some(Bytes::from(v));
    }
    if let Some(v) = get_opt_bool(hash, "compression_auto_train")? {
        opts.compression_auto_train = v;
    }
    if let Some(v) = get_opt_usize(hash, "compression_threshold")? {
        opts.compression_threshold = Some(v);
    }
    if let Some(v) = get_opt_i64(hash, "compression_level")? {
        opts.compression_level = Some(v as i32);
    }
    if let Some(v) = get_opt_usize(hash, "compression_dict_capacity")? {
        opts.compression_dict_capacity = Some(v);
    }
    if let Some(v) = get_opt_usize(hash, "max_recv_dict_size")? {
        opts.max_recv_dict_size = Some(v);
    }
    if let Some(v) = get_opt_i64(hash, "compression_offload_threshold")? {
        opts.compression_offload_threshold = if v < 0 { None } else { Some(v as usize) };
    }
    if let Some(v) = get_opt_string(hash, "on_mute")? {
        opts.on_mute = match v.as_str() {
            "drop_newest" | "drop" => omq_tokio::OnMute::DropNewest,
            _ => omq_tokio::OnMute::Block,
        };
    }

    if let Some(v) = get_opt_f64(hash, "reconnect_interval")? {
        opts.reconnect = omq_proto::options::ReconnectPolicy::Fixed(duration_from_seconds(
            "reconnect_interval",
            v,
        )?);
    }
    if let Some(min) = get_opt_f64(hash, "reconnect_interval_min")? {
        let max = get_opt_f64(hash, "reconnect_interval_max")?.unwrap_or(min * 16.0);
        opts.reconnect = omq_proto::options::ReconnectPolicy::Exponential {
            min: duration_from_seconds("reconnect_interval min", min)?,
            max: duration_from_seconds("reconnect_interval max", max)?,
        };
    }

    if let Some(mech_type) = get_opt_string(hash, "mechanism_type")? {
        apply_mechanism(hash, &mech_type, &mut opts)?;
    }

    Ok(opts)
}

fn apply_mechanism(hash: VALUE, mech_type: &str, opts: &mut omq_tokio::Options) -> RbResult<()> {
    match mech_type {
        "null" => {}

        #[cfg(feature = "curve")]
        "curve" => {
            let is_server = get_opt_bool(hash, "mechanism_server")?.unwrap_or(false);
            let pub_key = get_opt_bytes(hash, "mechanism_public_key")?;
            let sec_key = get_opt_bytes(hash, "mechanism_secret_key")?;

            if is_server {
                if let (Some(pk), Some(sk)) = (pub_key, sec_key) {
                    let keypair = omq_proto::CurveKeypair {
                        public: omq_proto::CurvePublicKey::from_bytes(to_32(&pk, "public key")?),
                        secret: omq_proto::CurveSecretKey::from_bytes(to_32(&sk, "secret key")?),
                    };
                    opts.mechanism = omq_proto::MechanismSetup::CurveServer {
                        our_keypair: keypair,
                        options: omq_proto::CurveServerOptions::default(),
                    };
                }
            } else {
                let srv_key = get_opt_bytes(hash, "mechanism_server_key")?;
                if let (Some(pk), Some(sk), Some(svk)) = (pub_key, sec_key, srv_key) {
                    let keypair = omq_proto::CurveKeypair {
                        public: omq_proto::CurvePublicKey::from_bytes(to_32(&pk, "public key")?),
                        secret: omq_proto::CurveSecretKey::from_bytes(to_32(&sk, "secret key")?),
                    };
                    opts.mechanism = omq_proto::MechanismSetup::CurveClient {
                        our_keypair: keypair,
                        server_public: omq_proto::CurvePublicKey::from_bytes(to_32(
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

fn to_32(bytes: &[u8], label: &str) -> RbResult<[u8; 32]> {
    bytes.try_into().map_err(|_| {
        RubyErr::arg(format!(
            "{label} must be exactly 32 bytes, got {}",
            bytes.len()
        ))
    })
}

fn get_opt_bytes(hash: VALUE, key: &str) -> RbResult<Option<Vec<u8>>> {
    match rb::hash_get(hash, key)? {
        Some(v) if v == rb::qnil() => Ok(None),
        Some(v) => Ok(Some(rb::value_to_bytes(v)?)),
        None => Ok(None),
    }
}

fn get_opt_string(hash: VALUE, key: &str) -> RbResult<Option<String>> {
    match rb::hash_get(hash, key)? {
        Some(v) if v == rb::qnil() => Ok(None),
        Some(v) => Ok(Some(rb::value_to_string(v)?)),
        None => Ok(None),
    }
}

fn get_opt_i64(hash: VALUE, key: &str) -> RbResult<Option<i64>> {
    match rb::hash_get(hash, key)? {
        Some(v) if v == rb::qnil() => Ok(None),
        Some(v) => Ok(Some(rb::value_to_i64(v)?)),
        None => Ok(None),
    }
}

fn get_opt_f64(hash: VALUE, key: &str) -> RbResult<Option<f64>> {
    match rb::hash_get(hash, key)? {
        Some(v) if v == rb::qnil() => Ok(None),
        Some(v) => Ok(Some(rb::value_to_f64(v)?)),
        None => Ok(None),
    }
}

fn get_opt_usize(hash: VALUE, key: &str) -> RbResult<Option<usize>> {
    let Some(v) = get_opt_i64(hash, key)? else {
        return Ok(None);
    };

    usize::try_from(v)
        .map(Some)
        .map_err(|_| RubyErr::arg(format!("{key} must be non-negative")))
}

fn get_opt_bool(hash: VALUE, key: &str) -> RbResult<Option<bool>> {
    match rb::hash_get(hash, key)? {
        Some(v) if v == rb::qnil() => Ok(None),
        Some(v) => Ok(Some(rb::value_to_bool(v)?)),
        None => Ok(None),
    }
}

fn get_opt_duration(hash: VALUE, key: &str) -> RbResult<Option<Duration>> {
    match get_opt_f64(hash, key)? {
        Some(v) => Ok(Some(duration_from_seconds(key, v)?)),
        None => Ok(None),
    }
}

fn duration_from_seconds(label: &str, value: f64) -> RbResult<Duration> {
    Duration::try_from_secs_f64(value)
        .map_err(|_| RubyErr::arg(format!("{label} must be finite and non-negative")))
}
