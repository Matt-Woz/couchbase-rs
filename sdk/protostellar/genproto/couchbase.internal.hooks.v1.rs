// This file is @generated by prost-build.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateHooksContextRequest {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct CreateHooksContextResponse {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DestroyHooksContextRequest {
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct DestroyHooksContextResponse {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ValueRef {
    #[prost(oneof = "value_ref::Value", tags = "1, 2, 3")]
    pub value: ::core::option::Option<value_ref::Value>,
}
/// Nested message and enum types in `ValueRef`.
pub mod value_ref {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Value {
        #[prost(string, tag = "1")]
        RequestField(::prost::alloc::string::String),
        #[prost(string, tag = "2")]
        CounterValue(::prost::alloc::string::String),
        #[prost(bytes, tag = "3")]
        JsonValue(::prost::alloc::vec::Vec<u8>),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HookCondition {
    #[prost(message, optional, tag = "1")]
    pub left: ::core::option::Option<ValueRef>,
    #[prost(enumeration = "ComparisonOperator", tag = "2")]
    pub op: i32,
    #[prost(message, optional, tag = "3")]
    pub right: ::core::option::Option<ValueRef>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct HookAction {
    #[prost(oneof = "hook_action::Action", tags = "1, 2, 3, 4, 5, 6, 7")]
    pub action: ::core::option::Option<hook_action::Action>,
}
/// Nested message and enum types in `HookAction`.
pub mod hook_action {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct If {
        #[prost(message, repeated, tag = "1")]
        pub cond: ::prost::alloc::vec::Vec<super::HookCondition>,
        #[prost(message, repeated, tag = "2")]
        pub r#match: ::prost::alloc::vec::Vec<super::HookAction>,
        #[prost(message, repeated, tag = "3")]
        pub no_match: ::prost::alloc::vec::Vec<super::HookAction>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Counter {
        #[prost(string, tag = "1")]
        pub counter_id: ::prost::alloc::string::String,
        #[prost(int64, tag = "2")]
        pub delta: i64,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct WaitOnBarrier {
        #[prost(string, tag = "1")]
        pub barrier_id: ::prost::alloc::string::String,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct SignalBarrier {
        #[prost(string, tag = "1")]
        pub barrier_id: ::prost::alloc::string::String,
        #[prost(bool, tag = "2")]
        pub signal_all: bool,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ReturnResponse {
        #[prost(message, optional, tag = "1")]
        pub value: ::core::option::Option<::prost_types::Any>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct ReturnError {
        #[prost(int32, tag = "1")]
        pub code: i32,
        #[prost(string, tag = "2")]
        pub message: ::prost::alloc::string::String,
        #[prost(message, repeated, tag = "3")]
        pub details: ::prost::alloc::vec::Vec<::prost_types::Any>,
    }
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, Copy, PartialEq, ::prost::Message)]
    pub struct Execute {}
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Action {
        #[prost(message, tag = "1")]
        If(If),
        #[prost(message, tag = "2")]
        Counter(Counter),
        #[prost(message, tag = "3")]
        WaitOnBarrier(WaitOnBarrier),
        #[prost(message, tag = "4")]
        SignalBarrier(SignalBarrier),
        #[prost(message, tag = "5")]
        ReturnResponse(ReturnResponse),
        #[prost(message, tag = "6")]
        ReturnError(ReturnError),
        #[prost(message, tag = "7")]
        Execute(Execute),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Hook {
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub description: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub target_method: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "4")]
    pub actions: ::prost::alloc::vec::Vec<HookAction>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AddHooksRequest {
    #[prost(string, tag = "1")]
    pub hooks_context_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "2")]
    pub hooks: ::prost::alloc::vec::Vec<Hook>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct AddHooksResponse {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WatchBarrierRequest {
    #[prost(string, tag = "1")]
    pub hooks_context_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub barrier_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WatchBarrierResponse {
    #[prost(string, tag = "2")]
    pub wait_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SignalBarrierRequest {
    #[prost(string, tag = "1")]
    pub hooks_context_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub barrier_id: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "3")]
    pub wait_id: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct SignalBarrierResponse {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WatchRequestsRequest {
    #[prost(string, tag = "1")]
    pub hooks_context_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WatchRequestsResponse {
    #[prost(message, repeated, tag = "1")]
    pub meta_data: ::prost::alloc::vec::Vec<watch_requests_response::MetaDataEntry>,
    #[prost(string, tag = "2")]
    pub full_method: ::prost::alloc::string::String,
    #[prost(message, optional, tag = "3")]
    pub payload: ::core::option::Option<::prost_types::Any>,
}
/// Nested message and enum types in `WatchRequestsResponse`.
pub mod watch_requests_response {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct MetaDataEntry {
        #[prost(string, tag = "1")]
        pub key: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub value: ::prost::alloc::string::String,
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ComparisonOperator {
    Equal = 0,
    NotEqual = 1,
    GreaterThan = 2,
    GreaterThanOrEqual = 3,
    LessThan = 4,
    LessThanOrEqual = 5,
}
impl ComparisonOperator {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ComparisonOperator::Equal => "COMPARISON_OPERATOR_EQUAL",
            ComparisonOperator::NotEqual => "COMPARISON_OPERATOR_NOT_EQUAL",
            ComparisonOperator::GreaterThan => "COMPARISON_OPERATOR_GREATER_THAN",
            ComparisonOperator::GreaterThanOrEqual => {
                "COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL"
            }
            ComparisonOperator::LessThan => "COMPARISON_OPERATOR_LESS_THAN",
            ComparisonOperator::LessThanOrEqual => {
                "COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL"
            }
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "COMPARISON_OPERATOR_EQUAL" => Some(Self::Equal),
            "COMPARISON_OPERATOR_NOT_EQUAL" => Some(Self::NotEqual),
            "COMPARISON_OPERATOR_GREATER_THAN" => Some(Self::GreaterThan),
            "COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL" => Some(Self::GreaterThanOrEqual),
            "COMPARISON_OPERATOR_LESS_THAN" => Some(Self::LessThan),
            "COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL" => Some(Self::LessThanOrEqual),
            _ => None,
        }
    }
}
/// Generated client implementations.
pub mod hooks_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct HooksServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl HooksServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> HooksServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> HooksServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            HooksServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        pub async fn create_hooks_context(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateHooksContextRequest>,
        ) -> std::result::Result<
            tonic::Response<super::CreateHooksContextResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/couchbase.internal.hooks.v1.HooksService/CreateHooksContext",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "couchbase.internal.hooks.v1.HooksService",
                        "CreateHooksContext",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn destroy_hooks_context(
            &mut self,
            request: impl tonic::IntoRequest<super::DestroyHooksContextRequest>,
        ) -> std::result::Result<
            tonic::Response<super::DestroyHooksContextResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/couchbase.internal.hooks.v1.HooksService/DestroyHooksContext",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "couchbase.internal.hooks.v1.HooksService",
                        "DestroyHooksContext",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn add_hooks(
            &mut self,
            request: impl tonic::IntoRequest<super::AddHooksRequest>,
        ) -> std::result::Result<
            tonic::Response<super::AddHooksResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/couchbase.internal.hooks.v1.HooksService/AddHooks",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "couchbase.internal.hooks.v1.HooksService",
                        "AddHooks",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn watch_barrier(
            &mut self,
            request: impl tonic::IntoRequest<super::WatchBarrierRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::WatchBarrierResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/couchbase.internal.hooks.v1.HooksService/WatchBarrier",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "couchbase.internal.hooks.v1.HooksService",
                        "WatchBarrier",
                    ),
                );
            self.inner.server_streaming(req, path, codec).await
        }
        pub async fn signal_barrier(
            &mut self,
            request: impl tonic::IntoRequest<super::SignalBarrierRequest>,
        ) -> std::result::Result<
            tonic::Response<super::SignalBarrierResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/couchbase.internal.hooks.v1.HooksService/SignalBarrier",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "couchbase.internal.hooks.v1.HooksService",
                        "SignalBarrier",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        pub async fn watch_requests(
            &mut self,
            request: impl tonic::IntoRequest<super::WatchRequestsRequest>,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::WatchRequestsResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/couchbase.internal.hooks.v1.HooksService/WatchRequests",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "couchbase.internal.hooks.v1.HooksService",
                        "WatchRequests",
                    ),
                );
            self.inner.server_streaming(req, path, codec).await
        }
    }
}