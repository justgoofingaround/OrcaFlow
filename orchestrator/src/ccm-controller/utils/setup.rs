use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::{HashMap, VecDeque};

use crate::dto::{Config, SetupArgs, Protocol};

impl SetupArgs {
    pub async fn setup_config(&self, config: &Arc<RwLock<Config>>) {
        let mut guard = config.write().await;
        guard.parent_pids = Some(VecDeque::from(self.parent_pids.clone()));
        guard.default_protocol = Some(self.default_protocol.clone());
        drop(guard);
    }
    pub async fn setup_protocol_map(&self, protocol_map: &Arc<RwLock<HashMap<String, Protocol>>>, ignore_error: bool) {
        match super::create_protcols(
            &self.default_protocol,
            &protocol_map,
            &self.workarea,
            ignore_error
        )
        .await
        {
            Ok(_) => {}
            Err(e) => {
                panic!("{}", e);
            }
        };
    }
}

