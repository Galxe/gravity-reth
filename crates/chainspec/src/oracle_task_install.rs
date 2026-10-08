//! Deterministic inputs for the Testnet bridge-task installation hardfork.

use crate::{EthChainSpec, LONGEVITY_TESTNET_CHAIN_ID};
use alloc::{format, string::String};
use alloy_primitives::{Address, Bytes, B256, U256};

/// Complete immutable input for the one-shot bridge installation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OracleTaskInstallConfig {
    /// Unix seconds at which the installation boundary is crossed.
    pub timestamp: u64,
    /// External source chain identifier.
    pub source_id: U256,
    /// The sole task name for this source.
    pub task_name: B256,
    /// Opaque task bytes, independently interpreted by the relayer.
    pub task_config: Bytes,
    /// Exact deployed Receiver runtime hash, including its immutables.
    pub receiver_code_hash: B256,
    /// Sender trusted by the fixed Receiver.
    pub trusted_bridge: Address,
}

/// Parse explicit inputs without silently disabling malformed present keys.
///
/// `From<Genesis>` remains infallible. Gravity execution validates this result
/// before mutating block state. Other chains never execute this migration.
pub fn oracle_task_install_config<S: EthChainSpec + ?Sized>(
    spec: &S,
) -> Result<Option<OracleTaskInstallConfig>, String> {
    if spec.chain().id() != LONGEVITY_TESTNET_CHAIN_ID {
        return Ok(None)
    }
    let fields = &spec.genesis().config.extra_fields;
    let (time, config) = (fields.get("oracleTaskInstallTime"), fields.get("oracleTaskInstall"));
    if time.is_none() && config.is_none() {
        return Ok(None)
    }
    let error = |detail: &str| format!("OracleTaskInstall: {detail}");
    let timestamp = time
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| error("oracleTaskInstallTime must be a Unix-seconds integer"))?;
    let object = config
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| error("oracleTaskInstall must be an object"))?;
    const KEYS: [&str; 5] =
        ["sourceId", "taskName", "taskConfig", "receiverCodeHash", "trustedBridge"];
    if object.len() != KEYS.len() || object.keys().any(|key| !KEYS.contains(&key.as_str())) {
        return Err(error("oracleTaskInstall requires exactly sourceId, taskName, taskConfig, receiverCodeHash, trustedBridge"))
    }
    let hex = |key: &str| -> Result<&str, String> {
        object
            .get(key)
            .and_then(serde_json::Value::as_str)
            .filter(|value| value.starts_with("0x") && value.len() > 2)
            .ok_or_else(|| error(&format!("{key} must be a nonempty 0x-prefixed hex string")))
    };
    let source_id = hex("sourceId")?.parse::<U256>().map_err(|_| error("invalid sourceId"))?;
    let task_name = hex("taskName")?.parse::<B256>().map_err(|_| error("invalid taskName"))?;
    let task_config =
        hex("taskConfig")?.parse::<Bytes>().map_err(|_| error("invalid taskConfig"))?;
    let receiver_code_hash =
        hex("receiverCodeHash")?.parse::<B256>().map_err(|_| error("invalid receiverCodeHash"))?;
    let trusted_bridge =
        hex("trustedBridge")?.parse::<Address>().map_err(|_| error("invalid trustedBridge"))?;
    if source_id.is_zero() ||
        task_name.is_zero() ||
        receiver_code_hash.is_zero() ||
        trusted_bridge.is_zero()
    {
        return Err(error("sourceId, taskName, receiverCodeHash, trustedBridge must be nonzero"))
    }
    if task_config.is_empty() || task_config.len() > 4096 {
        return Err(error("taskConfig length must be between 1 and 4096 bytes"))
    }
    let gamma = fields
        .get("gammaTime")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| error("gammaTime must be configured"))?;
    if timestamp <= spec.genesis().timestamp || gamma >= timestamp {
        return Err(error("installation must follow genesis and gammaTime"))
    }
    Ok(Some(OracleTaskInstallConfig {
        timestamp,
        source_id,
        task_name,
        task_config,
        receiver_code_hash,
        trusted_bridge,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ChainSpec;
    use alloy_genesis::Genesis;
    use serde_json::json;

    fn genesis() -> Genesis {
        let mut genesis = Genesis::default();
        genesis.config.chain_id = LONGEVITY_TESTNET_CHAIN_ID;
        genesis.timestamp = 10;
        genesis.config.extra_fields.insert("gammaTime".into(), json!(20));
        genesis.config.extra_fields.insert("oracleTaskInstallTime".into(), json!(30));
        genesis.config.extra_fields.insert(
            "oracleTaskInstall".into(),
            json!({
                "sourceId":"0x7b", "taskName":format!("{:#x}", B256::repeat_byte(1)),
                "taskConfig":"0x616263", "receiverCodeHash":format!("{:#x}", B256::repeat_byte(2)),
                "trustedBridge":"0x0000000000000000000000000000000000001234"
            }),
        );
        genesis
    }

    #[test]
    fn oracle_task_install_strict_schema_and_other_chain_scope() {
        let base = genesis();
        let config = oracle_task_install_config(&ChainSpec::from(base.clone())).unwrap().unwrap();
        assert_eq!(config.timestamp, 30);
        assert_eq!(config.source_id, U256::from(123));
        assert_eq!(config.task_config.as_ref(), b"abc");
        for (key, value) in [
            ("oracleTaskInstallTime", json!("30")),
            ("oracleTaskInstallTime", json!(null)),
            ("oracleTaskInstallTime", json!(10)),
            ("oracleTaskInstall", json!({})),
            ("gammaTime", json!(30)),
        ] {
            let mut invalid = base.clone();
            invalid.config.extra_fields.insert(key.into(), value);
            assert!(oracle_task_install_config(&ChainSpec::from(invalid.clone())).is_err());
            invalid.config.chain_id = 1625;
            assert!(oracle_task_install_config(&ChainSpec::from(invalid)).unwrap().is_none());
        }
        for key in ["oracleTaskInstall", "oracleTaskInstallTime", "gammaTime"] {
            let mut invalid = base.clone();
            invalid.config.extra_fields.remove(key);
            assert!(oracle_task_install_config(&ChainSpec::from(invalid)).is_err());
        }
        let mut disabled = base;
        disabled.config.extra_fields.remove("oracleTaskInstall");
        disabled.config.extra_fields.remove("oracleTaskInstallTime");
        assert!(oracle_task_install_config(&ChainSpec::from(disabled)).unwrap().is_none());
    }

    #[test]
    fn oracle_task_install_rejects_invalid_hex_zero_and_unbounded_config() {
        for (key, value) in [
            ("sourceId", json!(123)),
            ("sourceId", json!("0x0")),
            ("taskName", json!("0x01")),
            ("taskConfig", json!("0x")),
            ("taskConfig", json!(format!("0x{}", "ab".repeat(4097)))),
            ("receiverCodeHash", json!(format!("{:#x}", B256::ZERO))),
            ("trustedBridge", json!(format!("{:#x}", Address::ZERO))),
            ("caller", json!("0x1234")),
        ] {
            let mut invalid = genesis();
            invalid
                .config
                .extra_fields
                .get_mut("oracleTaskInstall")
                .unwrap()
                .as_object_mut()
                .unwrap()
                .insert(key.into(), value);
            assert!(oracle_task_install_config(&ChainSpec::from(invalid)).is_err(), "{key}");
        }
    }
}
