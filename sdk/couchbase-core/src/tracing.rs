use crate::util::get_host_port_from_uri;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::Duration;
use tracing::{instrument, span, trace_span, Level, Span};
use url::Url;

pub(crate) struct TracingUtils;

impl TracingUtils {
    pub(crate) fn record_cluster_labels(span: &Span, cluster_labels: &Option<ClusterLabels>) {
        if let Some(cluster_labels) = cluster_labels {
            if let Some(cluster_uuid) = &cluster_labels.cluster_uuid {
                span.record("db.couchbase.cluster_uuid", cluster_uuid.as_str());
            }
            if let Some(cluster_name) = &cluster_labels.cluster_name {
                span.record("db.couchbase.cluster_name", cluster_name.as_str());
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TracingConfig {
    pub cluster_labels: Option<ClusterLabels>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ClusterLabels {
    pub cluster_uuid: Option<String>,
    pub cluster_name: Option<String>,
}

pub(crate) enum OperationId {
    String(String),
    Number(u64),
}

impl OperationId {
    pub(crate) fn from_u32(n: u32) -> Self {
        Self::Number(n as u64)
    }

    pub(crate) fn from_string(s: String) -> Self {
        Self::String(s)
    }
}

pub(crate) struct EndDispatchFields {
    pub server_duration: Option<Duration>,
    pub operation_id: Option<OperationId>,
}

impl EndDispatchFields {
    pub(crate) fn new(server_duration: Option<Duration>, operation_id: Option<OperationId>) -> Self {
        Self {
            server_duration,
            operation_id,
        }
    }

    pub(crate) fn server_duration(mut self, server_duration: Option<Duration>) -> Self {
        self.server_duration = server_duration;
        self
    }

    pub(crate) fn operation_id(mut self, operation_id: Option<OperationId>) -> Self {
        self.operation_id = operation_id;
        self
    }

    pub(crate) fn end_span(&self, span: Span) {
        if let Some(server_duration) = self.server_duration {
            span.record("db.couchbase.server_duration", server_duration.as_micros());
        }

        if let Some(operation_id) = &self.operation_id {
            match operation_id {
                OperationId::String(s) => span.record("db.couchbase.operation_id", s),
                OperationId::Number(n) => span.record("db.couchbase.operation_id", n),
            };
        }

        drop(span);
    }
}

#[derive(Debug)]
pub(crate) struct BeginDispatchFields {
    pub local_addr: Option<(String, String)>,
    pub peer_addr: (String, String),
    pub client_id: Option<String>,
    pub cluster_labels: Option<ClusterLabels>,
}

impl BeginDispatchFields {
    pub(crate) fn new(
        local_addr: Option<(String, String)>,
        peer_addr: (String, String),
        client_id: Option<String>,
        cluster_labels: Option<ClusterLabels>,
    ) -> Self {
        Self {
            local_addr,
            peer_addr,
            client_id,
            cluster_labels
        }
    }

    pub(crate) fn create_span(&self) -> Span {
        let span = trace_span!(
            "dispatch_to_server",
            "db.system" = "couchbase",
            "net.transport" = "IP.TCP",
            "db.couchbase.cluster_uuid" = tracing::field::Empty,
            "db.couchbase.cluster_name" = tracing::field::Empty,
            "db.couchbase.server_duration" = tracing::field::Empty,
            "db.couchbase.local_id" = self.client_id,
            "net.host.name" = tracing::field::Empty,
            "net.host.port" = tracing::field::Empty,
            "net.peer.name" = self.peer_addr.0,
            "net.peer.port" = self.peer_addr.1,
            "db.couchbase.operation_id" = tracing::field::Empty,
        );

        if let Some(local_addr) = &self.local_addr {
            span.record("net.host.name", &local_addr.0);
            span.record("net.host.port", &local_addr.1);
        }

        TracingUtils::record_cluster_labels(&span, &self.cluster_labels);
        span
    }
}
