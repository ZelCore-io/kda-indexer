//! The node's `/info` facts that stock's `networkInfo` serves (`apiVersion`, `nodeBlockDelay`,
//! `nodeChains`, `numberOfChains`, `genesisHeights`, `nodePackageVersion`, `nodeServiceDate`,
//! `nodeLatestBehaviorHeight`).
//!
//! Stock reads `/info` into a memory cache at start and every 30 s, keeps the last good
//! answer when a read fails, and parses it with a strict schema (a missing or mistyped field
//! keeps the old answer). Ours: beside the `/cut` poll, `serve` and `run` read `/info` on their
//! own timer, every [`NODE_INFO_TTL`] after a good answer and every [`NODE_INFO_RETRY`] after
//! a failed one, one short try each, on the poll's node client (one request per TTL, nothing
//! per GraphQL request). The last
//! good answer is kept on failure, as stock keeps it.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde_json::Value;

/// How long a good `/info` answer is used before the poll reads it again.
pub const NODE_INFO_TTL: Duration = Duration::from_secs(300);
/// How soon the poll reads `/info` again after a failed or unusable answer (stock's cache
/// interval).
pub const NODE_INFO_RETRY: Duration = Duration::from_secs(30);

/// The fields of one good `/info` answer, as the node sent them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeFacts {
    /// `nodeApiVersion` (stock's `networkInfo.apiVersion`).
    pub api_version: String,
    /// `nodeBlockDelay` (microseconds).
    pub block_delay: i64,
    /// `nodeChains`, in the node's order.
    pub chains: Vec<String>,
    /// `nodeNumberOfChains`.
    pub number_of_chains: i64,
    /// `nodeGenesisHeights`: `(chain, height)` in the node's order.
    pub genesis_heights: Vec<(String, i64)>,
    /// `nodePackageVersion`.
    pub package_version: String,
    /// `nodeServiceDate` text, `None` when the node sends `null` or omits it.
    pub service_date: Option<String>,
    /// `nodeLatestBehaviorHeight`.
    pub latest_behavior_height: i64,
    /// When the answer was read.
    pub at: SystemTime,
}

impl NodeFacts {
    /// Stock's `/info` schema: every field present with its type, else `None` (stock then
    /// keeps its previous answer). Bounded by the body the node client already capped.
    pub fn from_info(v: &Value) -> Option<NodeFacts> {
        let int = |k: &str| v.get(k).and_then(Value::as_i64);
        let text = |k: &str| v.get(k).and_then(Value::as_str).map(str::to_owned);
        let chains = v
            .get("nodeChains")?
            .as_array()?
            .iter()
            .map(|c| c.as_str().map(str::to_owned))
            .collect::<Option<Vec<_>>>()?;
        let genesis_heights = v
            .get("nodeGenesisHeights")?
            .as_array()?
            .iter()
            .map(|e| {
                let a = e.as_array().filter(|a| a.len() == 2)?;
                Some((a[0].as_str()?.to_owned(), a[1].as_i64()?))
            })
            .collect::<Option<Vec<_>>>()?;
        // `nodeServiceDate` is nullable in stock's schema; chainweb-node 3.2.1 omits it, so an
        // absent key is `None` like `null` (any other type still rejects the answer).
        let service_date = match v.get("nodeServiceDate") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(s.clone()),
            Some(_) => return None,
        };
        // Checked as stock checks it, though `networkId` comes from the config.
        text("nodeVersion")?;
        Some(NodeFacts {
            api_version: text("nodeApiVersion")?,
            block_delay: int("nodeBlockDelay")?,
            chains,
            number_of_chains: int("nodeNumberOfChains")?,
            genesis_heights,
            package_version: text("nodePackageVersion")?,
            service_date,
            latest_behavior_height: int("nodeLatestBehaviorHeight")?,
            at: SystemTime::now(),
        })
    }
}

/// The last good answer, shared by the poll and the readers.
pub type SharedFacts = arc_swap::ArcSwapOption<NodeFacts>;

/// Keep `facts` when it parsed; a failed parse leaves the previous answer.
pub fn store(slot: &SharedFacts, raw: &Value) -> bool {
    match NodeFacts::from_info(raw) {
        Some(f) => {
            slot.store(Some(Arc::new(f)));
            true
        }
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A mainnet `/info` answer (the values of stock's network-info fixture).
    fn mainnet_info() -> Value {
        json!({
            "nodeApiVersion": "0.0",
            "nodeBlockDelay": 30000000,
            "nodeChains": ["0", "1", "10", "19"],
            "nodeGenesisHeights": [["0", 0], ["10", 852054], ["19", 852054]],
            "nodeGraphHistory": [],
            "nodeHistoricalChains": [],
            "nodeLatestBehaviorHeight": 5785924,
            "nodeNumberOfChains": 20,
            "nodePackageVersion": "2.29",
            "nodeServiceDate": "2025-07-23T00:00:00Z",
            "nodeVersion": "mainnet01"
        })
    }

    #[test]
    fn a_full_answer_parses_in_the_node_s_order() {
        let f = NodeFacts::from_info(&mainnet_info()).unwrap();
        assert_eq!(f.api_version, "0.0");
        assert_eq!(f.block_delay, 30_000_000);
        assert_eq!(f.chains, ["0", "1", "10", "19"]);
        assert_eq!(f.number_of_chains, 20);
        assert_eq!(
            f.genesis_heights,
            [
                ("0".to_owned(), 0),
                ("10".to_owned(), 852_054),
                ("19".to_owned(), 852_054)
            ]
        );
        assert_eq!(f.package_version, "2.29");
        assert_eq!(f.service_date.as_deref(), Some("2025-07-23T00:00:00Z"));
        assert_eq!(f.latest_behavior_height, 5_785_924);
        let mut v = mainnet_info();
        v["nodeServiceDate"] = Value::Null;
        assert_eq!(NodeFacts::from_info(&v).unwrap().service_date, None);
    }

    /// chainweb-node 3.2.1 omits `nodeServiceDate`: the answer still parses, every other
    /// field is served and the date is `None`.
    #[test]
    fn an_answer_without_node_service_date_still_parses() {
        let mut v = mainnet_info();
        v.as_object_mut().unwrap().remove("nodeServiceDate");
        let f = NodeFacts::from_info(&v).expect("parses without nodeServiceDate");
        assert_eq!(f.service_date, None);
        assert_eq!(f.api_version, "0.0");
        assert_eq!(f.package_version, "2.29");
        assert_eq!(f.number_of_chains, 20);
        assert_eq!(f.latest_behavior_height, 5_785_924);
        assert!(store(&SharedFacts::empty(), &v));
    }

    #[test]
    fn a_missing_or_mistyped_field_keeps_the_last_good_answer() {
        let slot = SharedFacts::empty();
        assert!(store(&slot, &mainnet_info()));
        let good = slot.load_full().unwrap();
        for (k, bad) in [
            ("nodeBlockDelay", json!("30000000")),
            ("nodeChains", json!(["0", 1])),
            ("nodeGenesisHeights", json!([["0"]])),
            ("nodeServiceDate", json!(5)),
            ("nodePackageVersion", Value::Null),
            ("nodeLatestBehaviorHeight", json!(1.5)),
            ("nodeVersion", json!(1)),
        ] {
            let mut v = mainnet_info();
            v[k] = bad;
            assert!(NodeFacts::from_info(&v).is_none(), "{k}");
            assert!(!store(&slot, &v), "{k}");
            assert_eq!(slot.load_full().unwrap(), good, "{k}");
            let mut v = mainnet_info();
            v.as_object_mut().unwrap().remove(k);
            // `nodeServiceDate` is the one nullable field: absent is `None`, not a reject.
            assert_eq!(
                NodeFacts::from_info(&v).is_none(),
                k != "nodeServiceDate",
                "{k} absent"
            );
        }
        assert!(NodeFacts::from_info(&json!([])).is_none());
    }
}
