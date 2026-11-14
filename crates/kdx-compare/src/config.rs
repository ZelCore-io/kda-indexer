//! Harness configuration (spec section 7.4). TOML, unknown keys rejected.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};
use url::Url;

pub use kdx_node::NodeConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompareConfig {
    pub network: String,
    /// Requests per second across all targets (spec section 7.4).
    #[serde(default = "d_rate_rps")]
    pub rate_rps: f64,
    #[serde(default = "d_timeout_ms")]
    pub timeout_ms: u64,
    /// Pages followed via `next` on paginated routes.
    #[serde(default = "d_follow_depth")]
    pub follow_depth: u32,
    /// Sample heights at `min(tip) - min_depth`.
    #[serde(default = "d_min_depth")]
    pub min_depth: u64,
    /// Separate limit for node (oracle and generator) requests; 0 = unlimited. The node can
    /// take more than chainweb-data, so it does not share the targets' bucket.
    #[serde(default = "d_node_max_rps")]
    pub node_max_rps: u32,
    pub node: NodeConfig,
    #[serde(default)]
    pub targets: BTreeMap<String, TargetConfig>,
    /// `(a, b)`: target names, or `"node"` for the node oracle as B.
    #[serde(default)]
    pub pairs: Vec<(String, String)>,
    #[serde(default = "d_allowlist")]
    pub allowlist: PathBuf,
    #[serde(default)]
    pub seeds: Option<PathBuf>,
    #[serde(default)]
    pub replay: Option<PathBuf>,
    #[serde(default = "d_out_dir")]
    pub out_dir: PathBuf,
    #[serde(default)]
    pub metrics_bind: Option<String>,
    #[serde(default)]
    pub mix: Mix,
    /// Largest accepted (decompressed) target response body; a bigger one fails that request.
    #[serde(default = "d_max_body_bytes")]
    pub max_body_bytes: usize,
    /// GraphQL harness settings (spec section 7, plan Task 7).
    #[serde(default)]
    pub graphql: GraphqlConfig,
    /// `/coins` cadence (R-P5P9-57): one `/coins` compare per REST pair every this many
    /// seconds, on top of the random draws (a 2% share of them). Unset = random draws only.
    #[serde(default)]
    pub coins_every_secs: Option<u64>,
}

/// `[graphql]`: where the operation documents come from.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct GraphqlConfig {
    /// A directory of captured client operation documents (`*.graphql`, with an optional
    /// `*.txt` next to each whose `## example variables` block seeds the variables). Loaded at
    /// start and never committed: unset by default.
    pub ops_dir: Option<PathBuf>,
    /// Our own operations, written from the vendored schema.
    pub own_ops_dir: PathBuf,
    /// Operation names (file stems before the `-<sha8>` suffix) to leave out.
    pub skip_ops: Vec<String>,
    /// Heights per sampled block window and attempts per sample (generator bounds).
    pub window: u64,
    /// `totalCount` values above this compare as this (GQ-10).
    pub total_count_cap: u64,
    /// Node and native pairs (`NodeTx`, `NodeBlock`, `NodeBalance`) run in their own loop,
    /// one step every this many milliseconds, so the oracle limiter does not pace them
    /// (R-P5P9-57 cadence).
    pub node_step_ms: u64,
    /// Fresh samples drawn for the same operation when a draw gives no usable variables or an
    /// account newer than the oracle's ceiling, before the turn passes on. Redraws cost node
    /// and kda-indexer requests only, never oracle requests.
    pub redraws: u32,
    /// Per-field normalisers on the oracle pairs (R-P5P9-57 rules (a) and (b)), set by the
    /// controller per field after the node referee.
    pub field_rules: Vec<FieldRule>,
    /// R-P5P9-61 H6: pages the harness reads per side and compare to finish cut lists of one
    /// block or one tx (each page one request at that target's rate, so the oracle's share
    /// stays inside its budget). A list it cannot finish within it stays a diff, marked as a
    /// harness limit.
    pub list_page_budget: usize,
}

/// One per-field normaliser (R-P5P9-57): at `field`, a difference that `rule` explains is
/// not a diff; it shows as the visible allowlist hit `/~graphql-r57-field`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldRule {
    /// Dotted field names, matched against the end of the diff path with list indexes and
    /// connection wrappers (`edges/<n>/node`) left out: `cmd.nonce` matches
    /// `/data/transaction/cmd/nonce` and `transfers.amount` matches
    /// `/data/transfers/edges/3/node/amount`.
    pub field: String,
    pub rule: FieldRuleKind,
    /// Optional exact value of our side (`oracle_null`, `oracle_empty`): a string's text, or
    /// a number's decimal value. The rule then explains only that value (R-P5P9-58).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ours: Option<String>,
    /// Optional exact value of the oracle's side (`ours_null`), as for `ours`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oracle: Option<String>,
}

impl FieldRule {
    pub fn new(field: &str, rule: FieldRuleKind) -> FieldRule {
        FieldRule {
            field: field.into(),
            rule,
            ours: None,
            oracle: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldRuleKind {
    /// Ours is the oracle's string JSON-encoded once more, never the other way (F5
    /// `cmd.nonce`; R-P5P9-58).
    JsonUnquote,
    /// `null` and `""` are the same (F8 signer `scheme`).
    NullOrEmpty,
    /// Decimal texts or numbers with the same decimal value (trailing zeros, exponent form,
    /// number against string: `Transfer.amount`, F10).
    DecimalValue,
    /// JSON texts equal once their numbers compare by decimal value (F9 event `parameters`).
    JsonNumbersByValue,
    /// The oracle's `null` against any value of ours: only where the node confirmed ours
    /// (rule (a)).
    OracleNull,
    /// Our `null` against any value of the oracle's: only where the node confirmed that the
    /// field has no value (rule (a)).
    OursNull,
    /// The oracle's `""` against a non-null value of ours (F8 `signers.scheme` present: the
    /// node's scheme is ours, R-P5P9-58). Visible hit `/~graphql-r58-oracle-empty`.
    OracleEmpty,
    /// A count our response flags as a lower bound (`extensions.totalCountLowerBound`,
    /// R-P5P9-14) below the oracle's. Visible hit `/~graphql-r58-count-lower-bound`.
    CountLowerBound,
    /// Row rule; `field` names a root connection of transactions. A request key the oracle
    /// lists twice loses each copy outside our block once the node shows our block is
    /// canonical (`oracle_orphans`); the oracle's `totalCount` drops by the copies dropped.
    /// Visible hit `/~graphql-r58-oracle-orphan-dupes`.
    OracleOrphanDupes,
    /// Row rule; `field` names a root by-key account list. A row only ours lists leaves when
    /// the node's `/local` `(coin.details "<account>")` guard, on the chain ours lists for the
    /// key, holds the key. Visible hit `/~graphql-r58-by-key-extra-row`.
    ByKeyExtraRow,
    /// R-P5P9-60 `cmd.nonce` inner quotes: ours decodes (one JSON string layer) to a string
    /// that contains `"`, and the oracle's is that string with every `"` removed. Any other
    /// pair stays a diff. Visible hit `/~graphql-r60-oracle-strips-quotes`.
    OracleStripsQuotes,
    /// R-P5P9-60 count self-consistency; `field` is `<root>.totalCount` of a root connection.
    /// Explains the count only when neither side's page is cut, the two edge sets match,
    /// ours' count equals its own edge count and the oracle's count is below its own edge count
    /// (an oracle count above its edges stays a diff).
    /// Visible hit `/~graphql-r60-count-self-consistent`.
    CountSelfConsistent,
    /// R-P5P9-60 amendment (`transfers.amount`): the oracle sends some amounts as
    /// f64-rounded strings, ours is the exact Pact decimal. Explains only when the two
    /// strings differ as exact decimals (`1.0` against `1` is a format diff and stays), both
    /// parse as decimals, they parse to the same f64, and ours has at least as many
    /// significant digits as the oracle's.
    OracleF64Rounded,
    /// R-P5P9-62 H9 (`creationTime` fraction, the tx's Pact `ParsedInteger`): the oracle's
    /// time has a non-zero fraction of a second, ours has none (or zeros only), and ours is
    /// the oracle's rounded half to even to whole seconds. Whole-second differences, and a
    /// fraction on our side, stay diffs. Visible hit `/~graphql-h9-creation-time-fraction`.
    CreationTimeHalfEven,
}

impl FieldRuleKind {
    /// Rules on the rows of a root field, applied before the diff (with node reads).
    pub fn is_row(self) -> bool {
        matches!(
            self,
            FieldRuleKind::OracleOrphanDupes | FieldRuleKind::ByKeyExtraRow
        )
    }

    /// Rules on a whole root connection (its count against its edges), applied on the
    /// normalised answers before the diff.
    pub fn is_connection(self) -> bool {
        self == FieldRuleKind::CountSelfConsistent
    }

    /// The exact-value matchers the rule takes: (ours, oracle).
    pub fn matchers(self) -> (bool, bool) {
        match self {
            FieldRuleKind::OursNull => (false, true),
            FieldRuleKind::OracleNull | FieldRuleKind::OracleEmpty => (true, false),
            _ => (false, false),
        }
    }
}

impl Default for GraphqlConfig {
    fn default() -> Self {
        Self {
            ops_dir: None,
            own_ops_dir: PathBuf::from("compare/graphql"),
            skip_ops: Vec::new(),
            window: 50,
            total_count_cap: 10_000,
            node_step_ms: 10_000,
            redraws: 4,
            field_rules: Vec::new(),
            list_page_budget: 40,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetConfig {
    pub url: Url,
    pub kind: TargetKind,
    /// Accept a self-signed TLS certificate (only for an internal `graphql_oracle`).
    #[serde(default)]
    pub insecure_tls: bool,
    /// Own request rate of this target in requests per second (`graphql_oracle`: capped at
    /// 1.0 whatever is set); unset = the shared `rate_rps` bucket.
    #[serde(default)]
    pub rate_rps: Option<f64>,
    /// `graphql_oracle`: compare only heights up to this (the oracle's stall height minus a
    /// margin). Required for that kind.
    #[serde(default)]
    pub max_height: Option<u64>,
    /// Local address this target's client binds for its connections (R-P5P9-60 H4), so a
    /// run can give the harness its own loopback source without forwarders. Unset = the OS
    /// picks it.
    #[serde(default)]
    pub source_addr: Option<std::net::IpAddr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetKind {
    ChainwebData,
    KdaIndexerCompat,
    KdaTxs,
    /// Answers `/v1/txs` by composing chainweb-data calls (the `url` is a chainweb-data
    /// instance); used when no live kda-txs is reachable.
    KdaTxsViaCwd,
    /// kda-indexer native API: answers `/api/v1/accounts/{a}/balances` (the balances pair, A).
    KdaIndexerNative,
    /// The node's `/local` (H-1: `details` reads only): the balances pair's B. The `url` is
    /// ignored; the harness uses `[node]`.
    NodeLocal,
    /// kda-indexer GraphQL adapter (A of the GraphQL pairs): the `url` is the full endpoint.
    KdaIndexerGraphql,
    /// The GraphQL reference server (B of the GraphQL pairs): HTTP POST only, its own
    /// limiter (at most 1 r/s, one in flight), heights capped at `max_height`.
    GraphqlOracle,
}

impl TargetKind {
    /// Whether a target of this kind answers `route`: chainweb-data every compat route but
    /// `/v1/txs`, the kda-txs kinds only `/v1/txs`, kda-indexer compat all of them.
    pub fn is_graphql(self) -> bool {
        matches!(
            self,
            TargetKind::KdaIndexerGraphql | TargetKind::GraphqlOracle
        )
    }

    pub fn serves(self, route: crate::normalize::RouteKind) -> bool {
        use crate::normalize::RouteKind;
        match self {
            // GraphQL targets answer no REST route; their pairs run in `graphql.rs`.
            TargetKind::KdaIndexerGraphql | TargetKind::GraphqlOracle => false,
            TargetKind::ChainwebData => !matches!(route, RouteKind::V1Txs | RouteKind::Balances),
            TargetKind::KdaTxs | TargetKind::KdaTxsViaCwd => route == RouteKind::V1Txs,
            TargetKind::KdaIndexerCompat => route != RouteKind::Balances,
            TargetKind::KdaIndexerNative | TargetKind::NodeLocal => route == RouteKind::Balances,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Mix {
    pub replay: f64,
    pub random: f64,
    pub seeds: f64,
}

impl Default for Mix {
    fn default() -> Self {
        Self {
            replay: 0.5,
            random: 0.4,
            seeds: 0.1,
        }
    }
}

fn d_rate_rps() -> f64 {
    5.0
}
fn d_timeout_ms() -> u64 {
    30_000
}
fn d_follow_depth() -> u32 {
    3
}
fn d_min_depth() -> u64 {
    10
}
fn d_node_max_rps() -> u32 {
    50
}
fn d_allowlist() -> PathBuf {
    PathBuf::from("compare/allowlist.toml")
}
fn d_max_body_bytes() -> usize {
    8 << 20
}

fn d_out_dir() -> PathBuf {
    PathBuf::from("test-runs/compare")
}

impl CompareConfig {
    pub fn load(path: &Path) -> anyhow::Result<CompareConfig> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let cfg: CompareConfig =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.rate_rps.is_finite() || self.rate_rps <= 0.0 {
            bail!("rate_rps must be > 0");
        }
        for (a, b) in &self.pairs {
            if !self.targets.contains_key(a) {
                bail!("pair side A {a:?} is not a configured target");
            }
            if b != "node" && !self.targets.contains_key(b) {
                bail!("pair side B {b:?} is neither \"node\" nor a configured target");
            }
        }
        for (n, t) in &self.targets {
            if t.kind == TargetKind::GraphqlOracle && t.max_height.is_none() {
                bail!("target {n}: graphql_oracle needs max_height");
            }
            if t.rate_rps.is_some_and(|r| !r.is_finite() || r <= 0.0) {
                bail!("target {n}: rate_rps must be > 0");
            }
            if t.insecure_tls && t.kind != TargetKind::GraphqlOracle {
                bail!("target {n}: insecure_tls is only for graphql_oracle");
            }
        }
        if self.coins_every_secs == Some(0) {
            bail!("coins_every_secs must be > 0");
        }
        if self.graphql.node_step_ms == 0 {
            bail!("graphql.node_step_ms must be > 0");
        }
        for r in &self.graphql.field_rules {
            if r.field.is_empty() || r.field.split('.').any(str::is_empty) {
                bail!("graphql.field_rules: bad field {:?}", r.field);
            }
            let (ours, oracle) = r.rule.matchers();
            if (r.ours.is_some() && !ours) || (r.oracle.is_some() && !oracle) {
                bail!(
                    "graphql.field_rules: {:?} on {:?} takes no matcher on that side",
                    r.rule,
                    r.field
                );
            }
            if r.rule.is_connection()
                && !matches!(r.field.split_once('.'), Some((root, "totalCount")) if !root.contains('.'))
            {
                bail!(
                    "graphql.field_rules: {:?} needs `<root>.totalCount`, not {:?}",
                    r.rule,
                    r.field
                );
            }
            if r.rule.is_row() && r.field.contains('.') {
                bail!(
                    "graphql.field_rules: {:?} needs a root field name, not {:?}",
                    r.rule,
                    r.field
                );
            }
        }
        for (w, n) in [
            (self.mix.replay, "replay"),
            (self.mix.random, "random"),
            (self.mix.seeds, "seeds"),
        ] {
            if w < 0.0 {
                bail!("mix.{n} must be >= 0");
            }
        }
        Ok(())
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    /// The first configured target that is side A of some pair (or the first target).
    pub fn primary_target(&self) -> Option<&str> {
        let rest = |n: &&str| self.targets.get(*n).is_some_and(|t| !t.kind.is_graphql());
        self.pairs
            .iter()
            .map(|(a, _)| a.as_str())
            .find(rest)
            .or_else(|| self.targets.keys().map(String::as_str).find(rest))
    }

    /// Whether any pair has a GraphQL target as side A.
    pub fn has_graphql_pairs(&self) -> bool {
        self.pairs.iter().any(|(a, _)| {
            self.targets
                .get(a)
                .is_some_and(|t| t.kind == TargetKind::KdaIndexerGraphql)
        })
    }

    pub fn node_client(&self) -> anyhow::Result<kdx_node::NodeClient> {
        let p2p_cert_sha256 = self
            .node
            .p2p_cert_sha256
            .as_deref()
            .map(kdx_node::parse_sha256_hex)
            .transpose()
            .context("node.p2p_cert_sha256")?;
        let ncfg = kdx_node::NodeClientConfig {
            base: self.node.url.clone(),
            network: self.network.clone(),
            timeout: Duration::from_millis(self.node.timeout_ms),
            max_rps: self.node_max_rps,
            concurrency: 8,
            p2p_url: self.node.p2p_url.clone(),
            p2p_cert_sha256,
            max_body_bytes: self.node.max_body_bytes,
            max_sse_event_bytes: self.node.max_sse_event_bytes,
        };
        Ok(kdx_node::NodeClient::new(&ncfg)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = r#"
network = "mainnet01"
pairs = [["gql", "oracle"], ["gql", "node"]]
[node]
url = "http://127.0.0.1:31351"
[targets.gql]
url = "http://127.0.0.1:31363/graphql"
kind = "kda_indexer_graphql"
[targets.oracle]
url = "https://oracle.invalid/graphql"
kind = "graphql_oracle"
insecure_tls = true
rate_rps = 5.0
max_height = 6000000
"#;

    #[test]
    fn graphql_targets_parse_and_the_oracle_needs_a_ceiling() {
        let c: CompareConfig = toml::from_str(BASE).unwrap();
        c.validate().unwrap();
        assert!(c.has_graphql_pairs());
        // GraphQL targets are never the REST primary.
        assert_eq!(c.primary_target(), None);
        let no_ceiling = BASE.replace("max_height = 6000000\n", "");
        let c: CompareConfig = toml::from_str(&no_ceiling).unwrap();
        assert!(c.validate().unwrap_err().to_string().contains("max_height"));
        let wrong_kind = BASE.replace(
            "kind = \"kda_indexer_graphql\"",
            "kind = \"chainweb_data\"\ninsecure_tls = true",
        );
        let c: CompareConfig = toml::from_str(&wrong_kind).unwrap();
        assert!(c
            .validate()
            .unwrap_err()
            .to_string()
            .contains("insecure_tls"));
    }

    #[test]
    fn cadence_and_field_rule_keys_parse_and_validate() {
        let c: CompareConfig = toml::from_str(BASE).unwrap();
        // Defaults: node pairs every 10 s, 4 redraws, no field rules, no /coins cadence.
        assert_eq!(
            (
                c.graphql.node_step_ms,
                c.graphql.redraws,
                c.coins_every_secs
            ),
            (10_000, 4, None)
        );
        assert!(c.graphql.field_rules.is_empty());
        let text = BASE.replace(
            "[node]",
            "coins_every_secs = 480\n[graphql]\nnode_step_ms = 5000\nredraws = 2\nfield_rules = [{ field = \"cmd.nonce\", rule = \"json_unquote\" }, { field = \"signers.scheme\", rule = \"null_or_empty\" }]\n[node]",
        );
        let c: CompareConfig = toml::from_str(&text).unwrap();
        c.validate().unwrap();
        assert_eq!(c.coins_every_secs, Some(480));
        assert_eq!(c.graphql.node_step_ms, 5000);
        assert_eq!(
            c.graphql.field_rules[1],
            FieldRule::new("signers.scheme", FieldRuleKind::NullOrEmpty)
        );
        let bad = text.replace("signers.scheme", "signers..scheme");
        let c: CompareConfig = toml::from_str(&bad).unwrap();
        assert!(c.validate().is_err());
        let zero = text.replace("coins_every_secs = 480", "coins_every_secs = 0");
        let c: CompareConfig = toml::from_str(&zero).unwrap();
        assert!(c.validate().is_err());
        let unknown = text.replace("json_unquote", "ignore");
        assert!(toml::from_str::<CompareConfig>(&unknown).is_err());
    }

    /// The CG-2 `field_rules` (R-P5P9-58), as recorded in the P7H report: they parse and
    /// validate, the matchers land on the right side, and a matcher a kind does not take, or
    /// a row rule on a dotted field, is refused at load.
    #[test]
    fn cg2_field_rules_parse_with_matchers_and_row_kinds() {
        let rules = r#"[graphql]
field_rules = [
  { field = "cmd.nonce",                   rule = "json_unquote" },
  { field = "payload.pactId",              rule = "oracle_null" },
  { field = "result.transactionId",        rule = "ours_null", oracle = "0" },
  { field = "result.goodResult",           rule = "oracle_null", ours = "\"\"" },
  { field = "signers.scheme",              rule = "oracle_empty" },
  # events.parameters: F9: keep ours (P7F-2 evidence, 23:10Z)
  { field = "events.parameters",           rule = "json_numbers_by_value" },
  { field = "transfers.totalCount",        rule = "count_lower_bound" },
  { field = "transactions.totalCount",     rule = "count_lower_bound" },
  { field = "transactionsByPublicKey",     rule = "oracle_orphan_dupes" },
  { field = "fungibleAccountsByPublicKey", rule = "by_key_extra_row" },
]
[node]"#;
        let text = BASE.replace("[node]", rules);
        let c: CompareConfig = toml::from_str(&text).unwrap();
        c.validate().unwrap();
        let r = &c.graphql.field_rules;
        assert_eq!(r.len(), 10);
        assert_eq!(
            (r[2].ours.as_deref(), r[2].oracle.as_deref()),
            (None, Some("0"))
        );
        assert_eq!(r[3].ours.as_deref(), Some("\"\""));
        assert_eq!(r[4].rule, FieldRuleKind::OracleEmpty);
        assert!(r[8].rule.is_row() && r[9].rule.is_row());
        for bad in [
            text.replace("rule = \"ours_null\", oracle", "rule = \"ours_null\", ours"),
            text.replace(
                "rule = \"oracle_null\", ours",
                "rule = \"oracle_null\", oracle",
            ),
            text.replace("\"json_unquote\" }", "\"json_unquote\", ours = \"x\" }"),
            text.replace(
                "\"transactionsByPublicKey\"",
                "\"x.transactionsByPublicKey\"",
            ),
        ] {
            let c: CompareConfig = toml::from_str(&bad).unwrap();
            assert!(c.validate().is_err(), "{bad}");
        }
    }

    /// The CG-3 block (R-P5P9-60): CG-2's rules plus the three ruled normalisers, two rules
    /// on one field where the ruling keeps both, and `source_addr` on a target.
    #[test]
    fn cg3_field_rules_and_source_addr_parse_and_validate() {
        let rules = r#"[graphql]
field_rules = [
  { field = "cmd.nonce",                   rule = "json_unquote" },
  { field = "cmd.nonce",                   rule = "oracle_strips_quotes" },
  { field = "payload.pactId",              rule = "oracle_null" },
  { field = "result.transactionId",        rule = "ours_null", oracle = "0" },
  { field = "result.goodResult",           rule = "oracle_null", ours = "\"\"" },
  { field = "result.goodResult",           rule = "json_numbers_by_value" },
  { field = "result.continuation",         rule = "json_numbers_by_value" },
  { field = "signers.scheme",              rule = "oracle_empty" },
  { field = "events.parameters",           rule = "json_numbers_by_value" },
  { field = "transfers.totalCount",        rule = "count_lower_bound" },
  { field = "transactions.totalCount",     rule = "count_lower_bound" },
  { field = "transactions.totalCount",     rule = "count_self_consistent" },
  { field = "transactionsByPublicKey",     rule = "oracle_orphan_dupes" },
  { field = "fungibleAccountsByPublicKey", rule = "by_key_extra_row" },
  { field = "signers.clist.args",          rule = "json_numbers_by_value" },
  { field = "transfers.amount",            rule = "oracle_f64_rounded" },
]
[node]"#;
        let text = BASE.replace("[node]", rules).replace(
            "rate_rps = 5.0\n",
            "rate_rps = 5.0\nsource_addr = \"127.0.0.1\"\n",
        );
        let c: CompareConfig = toml::from_str(&text).unwrap();
        c.validate().unwrap();
        let r = &c.graphql.field_rules;
        assert_eq!(r.len(), 16);
        assert_eq!(r[1].rule, FieldRuleKind::OracleStripsQuotes);
        assert_eq!(r[15].rule, FieldRuleKind::OracleF64Rounded);
        assert!(r[11].rule.is_connection() && !r[11].rule.is_row());
        assert_eq!(
            c.targets["oracle"].source_addr,
            Some("127.0.0.1".parse().unwrap())
        );
        assert_eq!(c.targets["gql"].source_addr, None);
        for bad in [
            // The count rule names a root's own totalCount, nothing deeper or shallower.
            text.replace(
                "\"transactions.totalCount\",     rule = \"count_self_consistent\"",
                "\"transactions\", rule = \"count_self_consistent\"",
            ),
            text.replace(
                "\"transactions.totalCount\",     rule = \"count_self_consistent\"",
                "\"block.transactions.totalCount\", rule = \"count_self_consistent\"",
            ),
            text.replace(
                "\"transactions.totalCount\",     rule = \"count_self_consistent\"",
                "\"transactions.edges\", rule = \"count_self_consistent\"",
            ),
            // The new kinds take no matcher.
            text.replace(
                "\"oracle_strips_quotes\" }",
                "\"oracle_strips_quotes\", ours = \"x\" }",
            ),
        ] {
            let c: CompareConfig = toml::from_str(&bad).unwrap();
            assert!(c.validate().is_err(), "{bad}");
        }
        assert!(toml::from_str::<CompareConfig>(&text.replace("127.0.0.1\"", "lo0\"")).is_err());
    }
}
