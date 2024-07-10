use std::future::Future;
use std::sync::{Arc, Mutex};

use crate::cbconfig::TerseConfig;
use crate::error::Error;
use crate::error::Result;
use crate::memdx::response::TryFromClientResponse;
use crate::vbucketmap::VbucketMap;

pub(crate) trait VbucketRouter {
    fn update_vbucket_info(&self, info: VbucketRoutingInfo);
    fn dispatch_by_key(&self, key: &[u8], vbucket_server_idx: u32) -> Result<(String, u16)>;
    fn dispatch_to_vbucket(&self, vb_id: u16) -> Result<String>;
    fn num_replicas(&self) -> usize;
}

pub(crate) struct VbucketRoutingInfo {
    pub vbucket_info: VbucketMap,
    pub server_list: Vec<String>,
}

pub(crate) struct VbucketRouterOptions {}

pub(crate) struct StdVbucketRouter {
    routing_info: Arc<Mutex<VbucketRoutingInfo>>,
}

impl StdVbucketRouter {
    pub(crate) fn new(info: VbucketRoutingInfo, _opts: VbucketRouterOptions) -> Self {
        Self {
            routing_info: Arc::new(Mutex::new(info)),
        }
    }
}

impl VbucketRouter for StdVbucketRouter {
    fn update_vbucket_info(&self, info: VbucketRoutingInfo) {
        *self.routing_info.lock().unwrap() = info;
    }

    fn dispatch_by_key(&self, key: &[u8], vbucket_server_idx: u32) -> Result<(String, u16)> {
        let info = self.routing_info.lock().unwrap();
        let vb_id = info.vbucket_info.vbucket_by_key(key);
        let idx = info
            .vbucket_info
            .node_by_vbucket(vb_id, vbucket_server_idx)?;

        if idx >= 0 {
            if let Some(server) = info.server_list.get(idx as usize) {
                return Ok((server.clone(), vb_id));
            }
        }

        Err(Error::new_internal_error("No server assigned"))
    }

    fn dispatch_to_vbucket(&self, vb_id: u16) -> Result<String> {
        let info = self.routing_info.lock().unwrap();
        let idx = info.vbucket_info.node_by_vbucket(vb_id, 0)?;

        if idx > 0 {
            if let Some(server) = info.server_list.get(idx as usize) {
                return Ok(server.clone());
            }
        }

        Err(Error::new_internal_error("No server assigned"))
    }

    fn num_replicas(&self) -> usize {
        let info = self.routing_info.lock().unwrap();
        info.vbucket_info.num_replicas()
    }
}

pub(crate) trait NotMyVbucketConfigHandler {
    fn not_my_vbucket_config(&self, config: TerseConfig, source_hostname: &str);
}

pub(crate) async fn orchestrate_memd_routing<V, Resp, Fut>(
    vb: Arc<V>,
    nmvb_handler: Arc<impl NotMyVbucketConfigHandler>,
    key: &[u8],
    vb_server_idx: u32,
    mut operation: impl Fn(String, u16) -> Fut,
) -> Result<Resp>
where
    V: VbucketRouter,
    Fut: Future<Output = Result<Resp>> + Send,
{
    let (mut endpoint, mut vb_id) = vb.dispatch_by_key(key, vb_server_idx)?;

    loop {
        let err = match operation(endpoint.clone(), vb_id).await {
            Ok(r) => return Ok(r),
            Err(e) => e,
        };

        let config = if let Some(memdx_err) = err.is_memdx_error() {
            if memdx_err.is_notmyvbucket_error() {
                if let Some(config) = memdx_err.has_server_config() {
                    config
                } else {
                    return Err(err);
                }
            } else {
                return Err(err);
            }
        } else {
            return Err(err);
        };

        if config.is_empty() {
            return Err(err);
        }

        let value = match std::str::from_utf8(config.as_slice()) {
            Ok(v) => v.to_string(),
            Err(_e) => "".to_string(),
        };

        let config = value.replace("$HOST", endpoint.as_ref());

        let config_json: TerseConfig = match serde_json::from_str(&config) {
            Ok(c) => c,
            Err(_) => {
                return Err(err);
            }
        };

        nmvb_handler
            .clone()
            .not_my_vbucket_config(config_json, &endpoint);

        let (new_endpoint, new_vb_id) = vb.dispatch_by_key(key, vb_server_idx)?;
        if new_endpoint == endpoint && new_vb_id == vb_id {
            return Err(err);
        }

        endpoint = new_endpoint;
        vb_id = new_vb_id;
    }
}

#[cfg(test)]
mod tests {
    use crate::cbconfig::TerseConfig;
    use crate::kvclientmanager::KvClientManager;
    use crate::vbucketmap::VbucketMap;
    use crate::vbucketrouter::{
        NotMyVbucketConfigHandler, StdVbucketRouter, VbucketRouter, VbucketRouterOptions,
        VbucketRoutingInfo,
    };

    struct NVMBHandler {}

    impl NotMyVbucketConfigHandler for NVMBHandler {
        fn not_my_vbucket_config(&self, config: TerseConfig, source_hostname: &str) {}
    }

    #[test]
    fn dispatch_to_key() {
        let routing_info = VbucketRoutingInfo {
            vbucket_info: VbucketMap::new(
                vec![vec![0, 1], vec![1, 0], vec![0, 1], vec![0, 1], vec![1, 0]],
                1,
            )
            .unwrap(),
            server_list: vec!["endpoint1".to_string(), "endpoint2".to_string()],
        };

        let dispatcher = StdVbucketRouter::new(routing_info, VbucketRouterOptions {});

        let (endpoint, vb_id) = dispatcher.dispatch_by_key(b"key1", 0).unwrap();

        assert_eq!("endpoint2", endpoint);
        assert_eq!(1, vb_id);

        let (endpoint, vb_id) = dispatcher.dispatch_by_key(b"key2", 0).unwrap();

        assert_eq!("endpoint1", endpoint);
        assert_eq!(3, vb_id);

        let (endpoint, vb_id) = dispatcher.dispatch_by_key(b"key2", 1).unwrap();

        assert_eq!("endpoint2", endpoint);
        assert_eq!(3, vb_id);
    }
}