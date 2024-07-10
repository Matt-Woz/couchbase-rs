use std::future::Future;
use std::ops::{Deref, Sub};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::{Mutex, Notify};
use tokio::time::{Instant, sleep};

use crate::error::{Error, ErrorKind};
use crate::error::Result;
use crate::kvclient::{KvClient, KvClientConfig, KvClientOptions, OnKvClientCloseHandler};
use crate::kvclient_ops::KvClientOps;
use crate::memdx::dispatcher::{Dispatcher, OrphanResponseHandler};

// TODO: This needs some work, some more thought should go into the locking strategy as it's possible
// there are still races in this. Additionally it's extremely easy to write in deadlocks.

pub(crate) trait KvClientPool: Sized + Send + Sync {
    type Client: KvClient + KvClientOps + Send + Sync;

    fn new(
        config: KvClientPoolConfig,
        opts: KvClientPoolOptions,
    ) -> impl Future<Output = Self> + Send;
    fn get_client(&self) -> impl Future<Output = Result<Arc<Self::Client>>> + Send;
    fn shutdown_client(&self, client: Arc<Self::Client>) -> impl Future<Output = ()> + Send;
    fn close(&self) -> impl Future<Output = Result<()>> + Send;
    fn reconfigure(&self, config: KvClientPoolConfig) -> impl Future<Output = Result<()>> + Send;
}

#[derive(Debug, Clone)]
pub(crate) struct KvClientPoolConfig {
    pub num_connections: usize,
    pub client_config: KvClientConfig,
}

pub(crate) struct KvClientPoolOptions {
    pub connect_timeout: Duration,
    pub connect_throttle_period: Duration,
    pub orphan_handler: OrphanResponseHandler,
}

#[derive(Debug, Clone)]
struct ConnectionError {
    pub connect_error: Error,
    pub connect_error_time: Instant,
}

struct KvClientPoolClientSpawner {
    connect_timeout: Duration,
    connect_throttle_period: Duration,

    config: Arc<Mutex<KvClientConfig>>,

    connection_error: Mutex<Option<ConnectionError>>,

    orphan_handler: OrphanResponseHandler,
    on_client_close: OnKvClientCloseHandler,
}

struct KvClientPoolClientHandler<K: KvClient> {
    num_connections: AtomicUsize,
    clients: Arc<Mutex<Vec<Arc<K>>>>,
    fast_map: ArcSwap<Vec<Arc<K>>>,

    spawner: Mutex<KvClientPoolClientSpawner>,
    client_idx: AtomicUsize,

    new_client_watcher_notif: Notify,

    closed: AtomicBool,
}

pub(crate) struct NaiveKvClientPool<K: KvClient> {
    clients: Arc<KvClientPoolClientHandler<K>>,
}

impl<K> KvClientPoolClientHandler<K>
where
    K: KvClient + KvClientOps + PartialEq + Sync + Send + 'static,
{
    pub async fn get_client(&self) -> Result<Arc<K>> {
        let fm = self.fast_map.load();

        if !fm.is_empty() {
            let idx = self.client_idx.fetch_add(1, Ordering::SeqCst);
            // TODO: is this unwrap ok? It should be...
            let client = fm.get(idx % fm.len()).unwrap();
            return Ok(client.clone());
        }

        self.get_client_slow().await
    }

    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Err(ErrorKind::Shutdown.into());
        }

        let clients = self.clients.lock().await;
        for mut client in clients.iter() {
            // TODO: probably log
            client.close().await.unwrap_or_default();
        }

        Ok(())
    }

    pub async fn reconfigure(&self, config: KvClientPoolConfig) -> Result<()> {
        let mut old_clients = self.clients.lock().await;
        let mut new_clients = vec![];
        for client in old_clients.iter() {
            if let Err(e) = client.reconfigure(config.client_config.clone()).await {
                // TODO: log here.
                dbg!(e);
                client.close().await.unwrap_or_default();
                continue;
            };

            new_clients.push(client.clone());
        }
        self.spawner
            .lock()
            .await
            .reconfigure(config.client_config)
            .await;

        drop(old_clients);
        self.check_connections().await;

        Ok(())
    }

    async fn check_connections(&self) {
        let num_wanted_clients = self.num_connections.load(Ordering::SeqCst);

        let mut clients = self.clients.lock().await;
        let num_active_clients = clients.len();

        if num_active_clients > num_wanted_clients {
            let mut num_excess_clients = num_active_clients - num_wanted_clients;
            let mut num_closed_clients = 0;

            while num_excess_clients > 0 {
                let client_to_close = clients.remove(0);
                self.shutdown_client(client_to_close).await;

                num_excess_clients -= 1;
                num_closed_clients += 1;
            }
        }

        if num_wanted_clients > num_active_clients {
            let mut num_needed_clients = num_wanted_clients - num_active_clients;
            while num_needed_clients > 0 {
                if let Some(client) = self.spawner.lock().await.start_new_client::<K>().await {
                    if self.closed.load(Ordering::SeqCst) {
                        client.close().await.unwrap_or_default();
                    }

                    clients.push(Arc::new(client));
                    num_needed_clients -= 1;
                }
            }
        }

        drop(clients);

        self.rebuild_fast_map().await;
    }

    async fn get_client_slow(&self) -> Result<Arc<K>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(ErrorKind::Shutdown.into());
        }

        let clients = self.clients.lock().await;
        if !clients.is_empty() {
            let idx = self.client_idx.fetch_add(1, Ordering::SeqCst);
            // TODO: is this unwrap ok? It should be...
            let client = clients.get(idx % clients.len()).unwrap();
            return Ok(client.clone());
        }

        let spawner = self.spawner.lock().await;
        if let Some(e) = spawner.error().await {
            return Err(e.connect_error);
        }

        drop(clients);

        self.new_client_watcher_notif.notified();
        Box::pin(self.get_client_slow()).await
    }

    pub async fn handle_client_close(&self, client_id: String) {
        // TODO: not sure the ordering of close leading to here is great.
        if self.closed.load(Ordering::SeqCst) {
            return;
        }

        let mut clients = self.clients.lock().await;
        let idx = clients.iter().position(|x| x.id() == client_id);
        if let Some(idx) = idx {
            clients.remove(idx);
        }

        drop(clients);
        self.check_connections().await;
    }

    async fn rebuild_fast_map(&self) {
        let clients = self.clients.lock().await;
        let mut new_map = Vec::new();
        new_map.clone_from(clients.deref());
        self.fast_map.store(Arc::from(new_map));

        self.new_client_watcher_notif.notify_waiters();
    }

    pub async fn shutdown_client(&self, client: Arc<K>) {
        let mut clients = self.clients.lock().await;
        let idx = clients.iter().position(|x| *x == client);
        if let Some(idx) = idx {
            clients.remove(idx);
        }

        drop(clients);
        self.rebuild_fast_map().await;

        // TODO: Should log
        client.close().await.unwrap_or_default();
    }
}

impl KvClientPoolClientSpawner {
    async fn reconfigure(&self, config: KvClientConfig) {
        let mut guard = self.config.lock().await;
        *guard = config;
    }

    async fn error(&self) -> Option<ConnectionError> {
        let err = self.connection_error.lock().await;
        err.clone()
    }

    async fn start_new_client<K>(&self) -> Option<K>
    where
        K: KvClient + KvClientOps + PartialEq + Sync + Send + 'static,
    {
        loop {
            let err = self.connection_error.lock().await;
            if let Some(error) = err.deref() {
                let connect_wait_period =
                    self.connect_throttle_period - Instant::now().sub(error.connect_error_time);

                if !connect_wait_period.is_zero() {
                    drop(err);
                    sleep(connect_wait_period).await;
                    continue;
                }
            }
            break;
        }

        let config = self.config.lock().await;
        match K::new(
            config.clone(),
            KvClientOptions {
                orphan_handler: self.orphan_handler.clone(),
                on_close: self.on_client_close.clone(),
            },
        )
        .await
        {
            Ok(r) => {
                let mut e = self.connection_error.lock().await;
                *e = None;
                Some(r)
            }
            Err(e) => {
                let mut err = self.connection_error.lock().await;
                *err = Some(ConnectionError {
                    connect_error: e,
                    connect_error_time: Instant::now(),
                });

                None
            }
        }
    }
}

impl<K> KvClientPool for NaiveKvClientPool<K>
where
    K: KvClient + KvClientOps + PartialEq + Sync + Send + 'static,
{
    type Client = K;

    async fn new(config: KvClientPoolConfig, opts: KvClientPoolOptions) -> Self {
        let mut clients = Arc::new(KvClientPoolClientHandler {
            num_connections: AtomicUsize::new(config.num_connections),
            clients: Arc::new(Default::default()),
            client_idx: AtomicUsize::new(0),
            fast_map: ArcSwap::from_pointee(vec![]),

            spawner: Mutex::new(KvClientPoolClientSpawner {
                connect_timeout: opts.connect_timeout,
                connect_throttle_period: opts.connect_throttle_period,
                orphan_handler: opts.orphan_handler.clone(),
                connection_error: Mutex::new(None),
                on_client_close: Arc::new(|id| Box::pin(async {})),
                config: Arc::new(Mutex::new(config.client_config)),
            }),

            new_client_watcher_notif: Notify::new(),
            closed: AtomicBool::new(false),
        });

        let clients_clone = clients.clone();
        let mut spawner = clients.spawner.lock().await;
        spawner.on_client_close = Arc::new(move |id| {
            let clients_clone = clients_clone.clone();
            Box::pin(async move { clients_clone.handle_client_close(id).await })
        });
        drop(spawner);

        clients.check_connections().await;

        NaiveKvClientPool { clients }
    }

    async fn get_client(&self) -> Result<Arc<K>> {
        self.clients.get_client().await
    }

    async fn shutdown_client(&self, client: Arc<K>) {
        self.clients.shutdown_client(client).await;
    }

    async fn close(&self) -> Result<()> {
        self.clients.close().await
    }

    async fn reconfigure(&self, config: KvClientPoolConfig) -> Result<()> {
        self.clients.reconfigure(config).await
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Add;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::time::Instant;

    use crate::authenticator::PasswordAuthenticator;
    use crate::kvclient::{KvClient, KvClientConfig, StdKvClient};
    use crate::kvclient_ops::KvClientOps;
    use crate::kvclientpool::{
        KvClientPool, KvClientPoolConfig, KvClientPoolOptions, NaiveKvClientPool,
    };
    use crate::memdx::client::Client;
    use crate::memdx::request::{GetRequest, SetRequest};

    #[tokio::test]
    async fn roundtrip_a_request() {
        let _ = env_logger::try_init();

        let instant = Instant::now().add(Duration::new(7, 0));

        let client_config = KvClientConfig {
            address: "192.168.107.128:11210"
                .parse()
                .expect("Failed to parse address"),
            root_certs: None,
            accept_all_certs: None,
            client_name: "myclient".to_string(),
            authenticator: Some(Arc::new(
                PasswordAuthenticator {
                    username: "Administrator".to_string(),
                    password: "password".to_string(),
                }
                .into(),
            )),
            selected_bucket: Some("default".to_string()),
            disable_default_features: false,
            disable_error_map: false,
            disable_bootstrap: false,
        };

        let pool_config = KvClientPoolConfig {
            num_connections: 1,
            client_config,
        };

        let pool: NaiveKvClientPool<StdKvClient<Client>> = NaiveKvClientPool::new(
            pool_config,
            KvClientPoolOptions {
                connect_timeout: Default::default(),
                connect_throttle_period: Default::default(),
                orphan_handler: Arc::new(|packet| {
                    dbg!("unexpected orphan", packet);
                }),
            },
        )
        .await;

        let client = pool.get_client().await.unwrap();

        let result = client
            .set(SetRequest {
                collection_id: 0,
                key: "test".as_bytes().into(),
                vbucket_id: 1,
                flags: 0,
                value: "test".as_bytes().into(),
                datatype: 0,
                expiry: None,
                preserve_expiry: None,
                cas: None,
                on_behalf_of: None,
                durability_level: None,
                durability_level_timeout: None,
            })
            .await
            .unwrap();

        dbg!(result);

        let get_result = client
            .get(GetRequest {
                collection_id: 0,
                key: "test".as_bytes().into(),
                vbucket_id: 1,
                on_behalf_of: None,
            })
            .await
            .unwrap();

        dbg!(get_result);

        client.close().await.unwrap();

        pool.close().await.unwrap();
    }

    #[tokio::test]
    async fn reconfigure() {
        let _ = env_logger::try_init();

        let instant = Instant::now().add(Duration::new(7, 0));

        let client_config = KvClientConfig {
            address: "192.168.107.128:11210"
                .parse()
                .expect("Failed to parse address"),
            root_certs: None,
            accept_all_certs: None,
            client_name: "myclient".to_string(),
            authenticator: Some(Arc::new(
                PasswordAuthenticator {
                    username: "Administrator".to_string(),
                    password: "password".to_string(),
                }
                .into(),
            )),
            selected_bucket: None,
            disable_default_features: false,
            disable_error_map: false,
            disable_bootstrap: false,
        };

        let pool_config = KvClientPoolConfig {
            num_connections: 1,
            client_config,
        };

        let pool: NaiveKvClientPool<StdKvClient<Client>> = NaiveKvClientPool::new(
            pool_config,
            KvClientPoolOptions {
                connect_timeout: Default::default(),
                connect_throttle_period: Default::default(),
                orphan_handler: Arc::new(|packet| {
                    dbg!("unexpected orphan", packet);
                }),
            },
        )
        .await;

        let client_config = KvClientConfig {
            address: "192.168.107.128:11210"
                .parse()
                .expect("Failed to parse address"),
            root_certs: None,
            accept_all_certs: None,
            client_name: "myclient".to_string(),
            authenticator: Some(Arc::new(
                PasswordAuthenticator {
                    username: "Administrator".to_string(),
                    password: "password".to_string(),
                }
                .into(),
            )),
            selected_bucket: Some("default".to_string()),
            disable_default_features: false,
            disable_error_map: false,
            disable_bootstrap: false,
        };

        let client = pool.get_client().await.unwrap();
        let result = client
            .set(SetRequest {
                collection_id: 0,
                key: "test".as_bytes().into(),
                vbucket_id: 1,
                flags: 0,
                value: "test".as_bytes().into(),
                datatype: 0,
                expiry: None,
                preserve_expiry: None,
                cas: None,
                on_behalf_of: None,
                durability_level: None,
                durability_level_timeout: None,
            })
            .await;
        if result.is_ok() {
            panic!("result did not contain an error");
        }

        pool.reconfigure(KvClientPoolConfig {
            num_connections: 1,
            client_config,
        })
        .await
        .unwrap();

        let client = pool.get_client().await.unwrap();

        let result = client
            .set(SetRequest {
                collection_id: 0,
                key: "test".as_bytes().into(),
                vbucket_id: 1,
                flags: 0,
                value: "test".as_bytes().into(),
                datatype: 0,
                expiry: None,
                preserve_expiry: None,
                cas: None,
                on_behalf_of: None,
                durability_level: None,
                durability_level_timeout: None,
            })
            .await
            .unwrap();

        dbg!(result);

        let get_result = client
            .get(GetRequest {
                collection_id: 0,
                key: "test".as_bytes().into(),
                vbucket_id: 1,
                on_behalf_of: None,
            })
            .await
            .unwrap();

        dbg!(get_result);

        pool.close().await.unwrap();
    }
}