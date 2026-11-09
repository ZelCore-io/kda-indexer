//! `probe` command (implemented in a later task).

use crate::config::Config;

pub async fn run(_cfg: &Config) -> anyhow::Result<()> {
    anyhow::bail!("probe: not implemented yet")
}
