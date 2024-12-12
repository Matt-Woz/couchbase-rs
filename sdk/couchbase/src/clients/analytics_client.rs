use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::error;
use crate::options::analytics_options::AnalyticsOptions;
use crate::results::analytics_results::AnalyticsResult;
use couchbase_core::analyticsx;
use std::collections::HashMap;
use uuid::Uuid;

pub(crate) struct AnalyticsClient {
    backend: AnalyticsClientBackend,
}

impl AnalyticsClient {
    pub fn new(backend: AnalyticsClientBackend) -> Self {
        Self { backend }
    }

    pub async fn query<'a>(
        &self,
        statement: &str,
        opts: Option<&AnalyticsOptions<'a>>,
    ) -> error::Result<AnalyticsResult> {
        match &self.backend {
            AnalyticsClientBackend::CouchbaseAnalyticsClientBackend(backend) => {
                backend.query(statement, opts).await
            }
            AnalyticsClientBackend::Couchbase2AnalyticsClientBackend(backend) => {
                backend.query(statement, opts).await
            }
        }
    }
}

pub(crate) enum AnalyticsClientBackend {
    CouchbaseAnalyticsClientBackend(CouchbaseAnalyticsClient),
    Couchbase2AnalyticsClientBackend(Couchbase2AnalyticsClient),
}

pub(crate) struct AnalyticsKeyspace {
    pub bucket_name: String,
    pub scope_name: String,
}

pub(crate) struct CouchbaseAnalyticsClient {
    agent_provider: CouchbaseAgentProvider,
    keyspace: Option<AnalyticsKeyspace>,
}

impl CouchbaseAnalyticsClient {
    pub fn new(agent_provider: CouchbaseAgentProvider) -> Self {
        Self {
            agent_provider,
            keyspace: None,
        }
    }

    pub fn with_keyspace(
        agent_provider: CouchbaseAgentProvider,
        keyspace: AnalyticsKeyspace,
    ) -> Self {
        Self {
            agent_provider,
            keyspace: Some(keyspace),
        }
    }

    pub async fn query<'a>(
        &self,
        statement: &str,
        opts: Option<&AnalyticsOptions<'a>>,
    ) -> error::Result<AnalyticsResult> {
        let query_context = self.keyspace.as_ref().map(|keyspace| {
            format!(
                "{}.{}",
                keyspace.bucket_name.clone(),
                keyspace.scope_name.clone()
            )
        });

        if let Some(opts) = opts {
            let named_args = if let Some(named_args) = opts.named_parameters {
                let mut collected = HashMap::default();
                for (k, v) in named_args {
                    collected.insert(*k, *v);
                }
                Some(collected)
            } else {
                None
            };
            let raw = if let Some(raw) = opts.raw {
                let mut collected = HashMap::default();
                for (k, v) in raw {
                    collected.insert(*k, *v);
                }
                Some(collected)
            } else {
                None
            };
            let positional_params = if let Some(positional_params) = opts.positional_parameters {
                let mut collected = vec![];
                for v in positional_params {
                    collected.push(*v);
                }
                Some(collected)
            } else {
                None
            };

            // TODO: this isn't great.
            let client_context_id = opts
                .client_context_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| Uuid::new_v4().to_string());

            let priority = if let Some(priority) = opts.priority {
                if priority {
                    Some(-1)
                } else {
                    None
                }
            } else {
                None
            };

            let query_opts = couchbase_core::analyticsoptions::AnalyticsOptions::builder()
                .client_context_id(client_context_id.as_str())
                .priority(priority)
                .query_context(query_context.as_deref())
                .read_only(opts.read_only)
                .scan_consistency(
                    opts.scan_consistency
                        .map(analyticsx::query_options::ScanConsistency::from),
                )
                .statement(statement)
                .args(positional_params.as_deref())
                .named_args(&named_args)
                .raw(&raw)
                .build();

            let agent = self.agent_provider.get_agent().await;
            Ok(AnalyticsResult::from(agent.analytics(&query_opts).await?))
        } else {
            let client_context_id = Uuid::new_v4().to_string();

            let query_opts = couchbase_core::analyticsoptions::AnalyticsOptions::builder()
                .statement(statement)
                .client_context_id(client_context_id.as_str())
                .query_context(query_context.as_deref())
                .build();

            let agent = self.agent_provider.get_agent().await;
            Ok(AnalyticsResult::from(agent.analytics(&query_opts).await?))
        }
    }
}

pub(crate) struct Couchbase2AnalyticsClient {}

impl Couchbase2AnalyticsClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub async fn query<'a>(
        &self,
        _statement: &str,
        _opts: Option<&AnalyticsOptions<'a>>,
    ) -> error::Result<AnalyticsResult> {
        unimplemented!()
    }
}