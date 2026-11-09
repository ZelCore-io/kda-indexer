use crate::types::NodeInfo;

/// Chain set per height, from `/info` graph history.
#[derive(Debug, Clone)]
pub struct ChainGraph {
    /// Ascending by start height.
    history: Vec<(u64, Vec<u8>)>,
}

impl ChainGraph {
    pub fn from_info(info: &NodeInfo) -> ChainGraph {
        let mut history = info.graph_history.clone();
        history.sort_by_key(|(h, _)| *h);
        ChainGraph { history }
    }

    /// Chain ids (ascending) that exist at `height`.
    pub fn chains_at(&self, height: u64) -> &[u8] {
        let idx = self.history.partition_point(|(h, _)| *h <= height);
        match idx.checked_sub(1) {
            Some(i) => &self.history[i].1,
            None => self
                .history
                .first()
                .map(|(_, c)| c.as_slice())
                .unwrap_or(&[]),
        }
    }

    /// Graph change heights (start height of each graph, ascending).
    pub fn change_heights(&self) -> Vec<u64> {
        self.history.iter().map(|(h, _)| *h).collect()
    }
}
