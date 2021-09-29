pub mod analytics_indexes;
pub mod buckets;
pub mod collections;
pub mod error;
pub mod options;
pub mod query_indexes;
pub mod results;
pub mod search;
pub mod search_indexes;
pub mod users;
pub mod view_indexes;

use crate::api::analytics_indexes::AnalyticsIndexManager;
use crate::api::buckets::BucketManager;
use crate::api::error::{CouchbaseError, CouchbaseResult, ErrorContext};
use crate::api::options::*;
use crate::api::query_indexes::QueryIndexManager;
use crate::api::results::*;
use crate::api::search_indexes::SearchIndexManager;
use crate::io::request::*;
use crate::io::{Core, MUTATION_MACRO_CAS, MUTATION_MACRO_SEQNO, MUTATION_MACRO_VALUE_CRC32C};
use crate::CouchbaseError::Generic;
use crate::{CollectionManager, SearchQuery, UserManager, ViewIndexManager};
use futures::channel::oneshot;
use serde::{Serialize, Serializer};
use serde_json::{to_vec, Value};
use std::convert::TryFrom;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

/// Connect to a Couchbase cluster and perform cluster-level operations
///
/// This `Cluster` object is also your main and only entry point into the SDK.
#[derive(Debug)]
pub struct Cluster {
    core: Arc<Core>,
}

impl Cluster {
    /// Connect to a couchbase cluster
    ///
    /// # Arguments
    ///
    /// * `connection_string` - the connection string containing the bootstrap hosts
    /// * `username` - the name of the user, used for authentication
    /// * `password` - the password of the user
    ///
    /// # Examples
    ///
    /// Connecting to localhost with the `username` and its `password`.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// ```
    ///
    /// Using three nodes for bootstrapping (recommended for production):
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("couchbase://hosta,hostb,hostc", "username", "password");
    /// ```
    pub fn connect<S: Into<String>>(connection_string: S, username: S, password: S) -> Self {
        Cluster {
            core: Arc::new(Core::new(
                connection_string.into(),
                Some(username.into()),
                Some(password.into()),
            )),
        }
    }

    // This will likely move to become the actual connect function before beta.
    pub fn connect_with_options(
        connection_string: impl Into<String>,
        opts: ClusterOptions,
    ) -> Self {
        let mut connection_string = connection_string.into();
        let to_append = opts.to_conn_string();
        if !to_append.is_empty() {}
        if connection_string.contains("?") {
            connection_string = format!("{}&{}", connection_string, to_append);
        } else {
            connection_string = format!("{}?{}", connection_string, to_append);
        }
        let mut username = opts.username;
        let mut password = opts.password;
        if let Some(auth) = opts.authenticator {
            if let Some(u) = auth.username() {
                username = Some(u.clone());
            }
            if let Some(p) = auth.password() {
                password = Some(p.clone());
            }
            if let Some(path) = auth.certificate_path() {
                connection_string = format!("{}&certpath={}", connection_string, path.clone());
            }
            if let Some(path) = auth.key_path() {
                connection_string = format!("{}&keypath={}", connection_string, path.clone());
            }
        }

        Cluster {
            core: Arc::new(Core::new(connection_string.into(), username, password)),
        }
    }

    /// Open and connect to a couchbase `Bucket`
    ///
    /// # Arguments
    ///
    /// * `name` - the name of the bucket
    ///
    /// # Examples
    ///
    /// Connect and open the `travel-sample` bucket.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let bucket = cluster.bucket("travel-sample");
    /// ```
    pub fn bucket<S: Into<String>>(&self, name: S) -> Bucket {
        let name = name.into();
        self.core.open_bucket(name.clone());
        Bucket::new(self.core.clone(), name)
    }

    /// Executes a N1QL statement
    ///
    /// # Arguments
    ///
    /// * `statement` - the N1QL statement to execute
    /// * `options` - allows to pass in custom options
    ///
    /// # Examples
    ///
    /// Run a N1QL query with default options.
    /// ```no_run
    /// # let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let result = cluster.query("select * from bucket", couchbase::QueryOptions::default());
    /// ```
    ///
    /// This will return an async result, which can be consumed:
    /// ```no_run
    /// # use std::io;
    /// # use futures::stream::StreamExt;
    /// # use futures::executor::block_on;
    /// # fn main() -> io::Result<()> {
    /// # block_on(async {
    /// let cluster = couchbase::Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");
    /// match cluster.query("select 1=1", couchbase::QueryOptions::default()).await {
    ///     Ok(mut result) => {
    ///         let mut rows = result.rows::<serde_json::Value>();
    ///         while let Some(row) = rows.next().await {
    ///             println!("Found Row {:?}", row);
    ///         }
    ///     },
    ///     Err(e) => panic!("Query failed: {:?}", e),
    /// }
    /// # });
    /// # Ok(())
    /// # }
    /// ```
    /// See the [QueryResult](struct.QueryResult.html) for more information on what and how it can be consumed.
    pub async fn query<S: Into<String>>(
        &self,
        statement: S,
        options: QueryOptions,
    ) -> CouchbaseResult<QueryResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Query(QueryRequest {
            statement: statement.into(),
            options,
            sender,
            scope: None,
        }));
        receiver.await.unwrap()
    }

    /// Executes an analytics query
    ///
    /// # Arguments
    ///
    /// * `statement` - the analyticss statement to execute
    /// * `options` - allows to pass in custom options
    ///
    /// # Examples
    ///
    /// Run an analytics query with default options.
    /// ```no_run
    /// # let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let result = cluster.analytics_query("select * from dataset", couchbase::AnalyticsOptions::default());
    /// ```
    ///
    /// This will return an async result, which can be consumed:
    /// ```no_run
    /// # use std::io;
    /// # use futures::stream::StreamExt;
    /// # use futures::executor::block_on;
    /// # fn main() -> io::Result<()> {
    /// # block_on(async {
    /// let cluster = couchbase::Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");
    /// match cluster.analytics_query("select 1=1", couchbase::AnalyticsOptions::default()).await {
    ///     Ok(mut result) => {
    ///         let mut rows = result.rows::<serde_json::Value>();
    ///         while let Some(row) = rows.next().await {
    ///             println!("Found Row {:?}", row);
    ///         }
    ///     },
    ///     Err(e) => panic!("Query failed: {:?}", e),
    /// }
    /// # });
    /// # Ok(())
    /// # }
    /// ```
    /// See the [AnalyticsResult](struct.AnalyticsResult.html) for more information on what and how it can be consumed.
    pub async fn analytics_query<S: Into<String>>(
        &self,
        statement: S,
        options: AnalyticsOptions,
    ) -> CouchbaseResult<AnalyticsResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Analytics(AnalyticsRequest {
            statement: statement.into(),
            options,
            sender,
            scope: None,
        }));
        receiver.await.unwrap()
    }

    /// Executes a search query
    ///
    /// # Arguments
    ///
    /// * `index` - the search index name to use
    /// * `query` - the search query to perform
    /// * `options` - allows to pass in custom options
    ///
    /// # Examples
    ///
    /// Run a search query with default options.
    /// ```no_run
    /// # let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let result = cluster.search_query(
    ///    String::from("test"),
    ///    couchbase::QueryStringQuery::new(String::from("swanky")),
    ///    couchbase::SearchOptions::default(),
    ///);
    /// ```
    ///
    /// This will return an async result, which can be consumed:
    /// ```no_run
    /// # use std::io;
    /// # use futures::stream::StreamExt;
    /// # use futures::executor::block_on;
    /// # fn main() -> io::Result<()> {
    /// # block_on(async {
    /// let cluster = couchbase::Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");
    /// match cluster.search_query(
    ///    String::from("test"),
    ///    couchbase::QueryStringQuery::new(String::from("swanky")),
    ///    couchbase::SearchOptions::default(),
    ///).await {
    ///     Ok(mut result) => {
    ///         for row in result.rows().next().await {
    ///             println!("Found Row {:?}", row);
    ///         }
    ///     },
    ///     Err(e) => panic!("Query failed: {:?}", e),
    /// }
    /// # });
    /// # Ok(())
    /// # }
    /// ```
    /// See the [SearchResult](struct.SearchResult.html) for more information on what and how it can be consumed.
    pub async fn search_query<S: Into<String>, T: SearchQuery>(
        &self,
        index: S,
        query: T,
        options: SearchOptions,
    ) -> CouchbaseResult<SearchResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Search(SearchRequest {
            index: index.into(),
            query: query
                .to_json()
                .map_err(|e| CouchbaseError::EncodingFailure {
                    source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
                    ctx: ErrorContext::default(),
                })?,
            options,
            sender,
        }));
        receiver.await.unwrap()
    }

    /// Returns a new `UserManager`
    ///
    /// # Arguments
    ///
    /// # Examples
    ///
    /// Connect and open the `travel-sample` bucket.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let users = cluster.users();
    /// ```
    pub fn users(&self) -> UserManager {
        UserManager::new(self.core.clone())
    }

    /// Returns a new `BucketManager`
    ///
    /// # Arguments
    ///
    /// # Examples
    ///
    /// Connect and open the `travel-sample` bucket.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let bucket = cluster.buckets();
    /// ```
    pub fn buckets(&self) -> BucketManager {
        BucketManager::new(self.core.clone())
    }

    /// Returns a new `AnalyticsIndexManager`
    ///
    /// # Arguments
    ///
    /// # Examples
    ///
    /// Connect and open the `travel-sample` bucket.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let indexes = cluster.analytics_indexes();
    /// ```
    pub fn analytics_indexes(&self) -> AnalyticsIndexManager {
        AnalyticsIndexManager::new(self.core.clone())
    }

    /// Returns a new `QueryIndexManager`
    ///
    /// # Arguments
    ///
    /// # Examples
    ///
    /// Connect and open the `travel-sample` bucket.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let indexes = cluster.query_indexes();
    /// ```
    pub fn query_indexes(&self) -> QueryIndexManager {
        QueryIndexManager::new(self.core.clone())
    }

    /// Returns a new `SearchIndexManager`
    ///
    /// # Arguments
    ///
    /// # Examples
    ///
    /// Connect and open the `travel-sample` bucket.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let indexes = cluster.search_indexes();
    /// ```
    pub fn search_indexes(&self) -> SearchIndexManager {
        SearchIndexManager::new(self.core.clone())
    }

    /// Returns a reference to the underlying core.
    ///
    /// Note that this API is unsupported and not stable, so you need to opt in via the
    /// `volatile` feature to access it.
    #[cfg(feature = "volatile")]
    pub fn core(&self) -> Arc<Core> {
        self.core.clone()
    }
}

/// Provides bucket-level access to collections and view operations
#[derive(Debug)]
pub struct Bucket {
    name: String,
    core: Arc<Core>,
}

impl Bucket {
    pub(crate) fn new(core: Arc<Core>, name: String) -> Self {
        Self { name, core }
    }

    /// Opens the `default` collection (also used when a cluster with no collection support is used)
    ///
    /// The collection API provides acess to the Key/Value operations. The default collection is also
    /// implicitly using the default scope.
    pub fn default_collection(&self) -> Collection {
        Collection::new(self.core.clone(), "".into(), "".into(), self.name.clone())
    }

    /// The name of the bucket
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Opens a custom collection inside the `default` scope
    ///
    /// # Arguments
    ///
    /// * `name` - the collection name
    pub fn collection<S: Into<String>>(&self, name: S) -> Collection {
        Collection::new(self.core.clone(), name.into(), "".into(), self.name.clone())
    }

    /// Opens a custom scope
    ///
    /// # Arguments
    ///
    /// * `name` - the scope name
    pub fn scope<S: Into<String>>(&self, name: S) -> Scope {
        Scope::new(self.core.clone(), name.into(), self.name.clone())
    }

    /// Executes a ping request
    ///
    /// # Arguments
    ///
    /// * `options` - allows to pass in custom options
    ///
    /// # Examples
    ///
    /// Run a ping with default options.
    /// ```no_run
    /// # let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// # let bucket = cluster.bucket("travel-sample");
    /// # let result = bucket.ping(couchbase::PingOptions::default());
    /// ```
    ///
    /// This will return an async result, which can be consumed:
    /// ```no_run
    /// # use std::io;
    /// # use futures::stream::StreamExt;
    /// # use futures::executor::block_on;
    /// # fn main() -> io::Result<()> {
    /// # block_on(async {
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let bucket = cluster.bucket("travel-sample");
    /// match  bucket.ping(couchbase::PingOptions::default()).await {
    ///     Ok(mut result) => {
    ///         println!("Ping results {:?}", result);
    ///     },
    ///     Err(e) => panic!("Ping failed: {:?}", e),
    /// }
    /// # });
    /// # Ok(())
    /// # }
    /// ```
    /// See the [PingResult](struct.PingResult.html) for more information on what and how it can be consumed.
    pub async fn ping(&self, options: PingOptions) -> CouchbaseResult<PingResult> {
        let (sender, receiver) = oneshot::channel();
        self.core
            .send(Request::Ping(PingRequest { options, sender }));
        receiver.await.unwrap()
    }

    /// Returns a new `CollectionsManager`
    ///
    /// # Arguments
    ///
    /// # Examples
    ///
    /// Connect and open the `travel-sample` bucket.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let bucket = cluster.bucket("travel-sample");
    /// let manager = bucket.collections();
    /// ```
    pub fn collections(&self) -> CollectionManager {
        CollectionManager::new(self.core.clone(), self.name.clone())
    }

    /// Returns a new `QueryIndexManager`
    ///
    /// # Arguments
    ///
    /// # Examples
    ///
    /// Connect and open the `travel-sample` bucket.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let bucket = cluster.bucket("travel-sample");
    /// let manager = bucket.view_indexes();
    /// ```
    pub fn view_indexes(&self) -> ViewIndexManager {
        ViewIndexManager::new(self.core.clone(), self.name.clone())
    }

    /// Executes a view query
    ///
    /// # Arguments
    ///
    /// * `design_document` - the design document name to use
    /// * `view_name` - the view name to use
    /// * `options` - allows to pass in custom options
    ///
    /// # Examples
    ///
    /// Run a view query with default options.
    /// ```no_run
    /// let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let bucket = cluster.bucket("travel-sample");
    /// let result = bucket.view_query(
    ///    "my_design_doc",
    ///    "my_view",
    ///    couchbase::ViewOptions::default(),
    ///);
    /// ```
    ///
    /// This will return an async result, which can be consumed:
    /// ```no_run
    /// # use std::io;
    /// # use futures::stream::StreamExt;
    /// # use futures::executor::block_on;
    /// # fn main() -> io::Result<()> {
    /// # block_on(async {
    /// let cluster = couchbase::Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");
    /// let bucket = cluster.bucket("travel-sample");
    /// match bucket.view_query(
    ///    "my_design_doc",
    ///    "my_view",
    ///    couchbase::ViewOptions::default(),
    /// ).await {
    ///     Ok(mut result) => {
    ///         for row in result.rows().next().await {
    ///             println!("Found Row {:?}", row);
    ///         }
    ///     },
    ///     Err(e) => panic!("Query failed: {:?}", e),
    /// }
    /// # });
    /// # Ok(())
    /// # }
    /// ```
    /// See the [ViewResult](struct.ViewResult.html) for more information on what and how it can be consumed.
    pub async fn view_query(
        &self,
        design_document: impl Into<String>,
        view_name: impl Into<String>,
        options: ViewOptions,
    ) -> CouchbaseResult<ViewResult> {
        let form_data = options.form_data()?;
        let payload = match serde_urlencoded::to_string(form_data) {
            Ok(p) => p,
            Err(e) => {
                return Err(CouchbaseError::EncodingFailure {
                    source: std::io::Error::new(std::io::ErrorKind::Other, e),
                    ctx: ErrorContext::default(),
                });
            }
        };

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::View(ViewRequest {
            design_document: design_document.into(),
            view_name: view_name.into(),
            options: payload.into_bytes(),
            sender,
        }));
        receiver.await.unwrap()
    }
}

/// Scopes provide access to a group of collections
#[derive(Debug)]
pub struct Scope {
    bucket_name: String,
    name: String,
    core: Arc<Core>,
}

impl Scope {
    pub(crate) fn new(core: Arc<Core>, name: String, bucket_name: String) -> Self {
        Self {
            core,
            name,
            bucket_name,
        }
    }

    /// The name of the scope
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Opens a custom collection inside the current scope
    ///
    /// # Arguments
    ///
    /// * `name` - the collection name
    pub fn collection<S: Into<String>>(&self, name: S) -> Collection {
        Collection::new(
            self.core.clone(),
            name.into(),
            self.name.clone(),
            self.bucket_name.clone(),
        )
    }

    /// Executes a N1QL statement
    ///
    /// # Arguments
    ///
    /// * `statement` - the N1QL statement to execute
    /// * `options` - allows to pass in custom options
    ///
    /// # Examples
    ///
    /// Run a N1QL query with default options.
    /// ```no_run
    /// # let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let result = cluster.query("select * from bucket", couchbase::QueryOptions::default());
    /// ```
    ///
    /// This will return an async result, which can be consumed:
    /// ```no_run
    /// # use std::io;
    /// # use futures::stream::StreamExt;
    /// # use futures::executor::block_on;
    /// # fn main() -> io::Result<()> {
    /// # block_on(async {
    /// let cluster = couchbase::Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");
    /// let bucket = cluster.bucket("default");
    /// let scope = bucket.scope("myscope");
    /// match scope.query("select 1=1", couchbase::QueryOptions::default()).await {
    ///     Ok(mut result) => {
    ///         let mut rows = result.rows::<serde_json::Value>();
    ///         while let Some(row) = rows.next().await {
    ///             println!("Found Row {:?}", row);
    ///         }
    ///     },
    ///     Err(e) => panic!("Query failed: {:?}", e),
    /// }
    /// # });
    /// # Ok(())
    /// # }
    /// ```
    /// See the [QueryResult](struct.QueryResult.html) for more information on what and how it can be consumed.
    pub async fn query<S: Into<String>>(
        &self,
        statement: S,
        options: QueryOptions,
    ) -> CouchbaseResult<QueryResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Query(QueryRequest {
            statement: statement.into(),
            options,
            sender,
            scope: Some(self.name.clone()),
        }));
        receiver.await.unwrap()
    }

    /// Executes an analytics query
    ///
    /// # Arguments
    ///
    /// * `statement` - the analyticss statement to execute
    /// * `options` - allows to pass in custom options
    ///
    /// # Examples
    ///
    /// Run an analytics query with default options.
    /// ```no_run
    /// # let cluster = couchbase::Cluster::connect("127.0.0.1", "username", "password");
    /// let result = cluster.analytics_query("select * from dataset", couchbase::AnalyticsOptions::default());
    /// ```
    ///
    /// This will return an async result, which can be consumed:
    /// ```no_run
    /// # use std::io;
    /// # use futures::stream::StreamExt;
    /// # use futures::executor::block_on;
    /// # fn main() -> io::Result<()> {
    /// # block_on(async {
    /// let cluster = couchbase::Cluster::connect("couchbase://127.0.0.1", "Administrator", "password");
    /// match cluster.analytics_query("select 1=1", couchbase::AnalyticsOptions::default()).await {
    ///     Ok(mut result) => {
    ///         let mut rows = result.rows::<serde_json::Value>();
    ///         while let Some(row) = rows.next().await {
    ///             println!("Found Row {:?}", row);
    ///         }
    ///     },
    ///     Err(e) => panic!("Query failed: {:?}", e),
    /// }
    /// # });
    /// # Ok(())
    /// # }
    /// ```
    /// See the [AnalyticsResult](struct.AnalyticsResult.html) for more information on what and how it can be consumed.
    pub async fn analytics_query<S: Into<String>>(
        &self,
        statement: S,
        options: AnalyticsOptions,
    ) -> CouchbaseResult<AnalyticsResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Analytics(AnalyticsRequest {
            statement: statement.into(),
            options,
            sender,
            scope: Some(self.name.clone()),
        }));
        receiver.await.unwrap()
    }
}

/// Primary API to access Key/Value operations
#[derive(Debug)]
pub struct Collection {
    core: Arc<Core>,
    name: String,
    scope_name: String,
    bucket_name: String,
}

impl Collection {
    pub(crate) fn new(
        core: Arc<Core>,
        name: String,
        scope_name: String,
        bucket_name: String,
    ) -> Self {
        Self {
            core,
            name,
            scope_name,
            bucket_name,
        }
    }

    /// The name of the collection
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub async fn get<S: Into<String>>(
        &self,
        id: S,
        options: GetOptions,
    ) -> CouchbaseResult<GetResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Get(GetRequest {
            id: id.into(),
            ty: GetRequestType::Get { options },
            bucket: self.bucket_name.clone(),
            sender,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn get_and_lock<S: Into<String>>(
        &self,
        id: S,
        lock_time: Duration,
        options: GetAndLockOptions,
    ) -> CouchbaseResult<GetResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Get(GetRequest {
            id: id.into(),
            ty: GetRequestType::GetAndLock { options, lock_time },
            bucket: self.bucket_name.clone(),
            sender,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn get_and_touch<S: Into<String>>(
        &self,
        id: S,
        expiry: Duration,
        options: GetAndTouchOptions,
    ) -> CouchbaseResult<GetResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Get(GetRequest {
            id: id.into(),
            ty: GetRequestType::GetAndTouch { options, expiry },
            bucket: self.bucket_name.clone(),
            sender,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn exists<S: Into<String>>(
        &self,
        id: S,
        options: ExistsOptions,
    ) -> CouchbaseResult<ExistsResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Exists(ExistsRequest {
            id: id.into(),
            options,
            bucket: self.bucket_name.clone(),
            sender,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn upsert<S: Into<String>, T>(
        &self,
        id: S,
        content: T,
        options: UpsertOptions,
    ) -> CouchbaseResult<MutationResult>
    where
        T: Serialize,
    {
        self.mutate(id, content, MutateRequestType::Upsert { options })
            .await
    }

    pub async fn insert<S: Into<String>, T>(
        &self,
        id: S,
        content: T,
        options: InsertOptions,
    ) -> CouchbaseResult<MutationResult>
    where
        T: Serialize,
    {
        self.mutate(id, content, MutateRequestType::Insert { options })
            .await
    }

    pub async fn replace<S: Into<String>, T>(
        &self,
        id: S,
        content: T,
        options: ReplaceOptions,
    ) -> CouchbaseResult<MutationResult>
    where
        T: Serialize,
    {
        self.mutate(id, content, MutateRequestType::Replace { options })
            .await
    }

    async fn mutate<S: Into<String>, T>(
        &self,
        id: S,
        content: T,
        ty: MutateRequestType,
    ) -> CouchbaseResult<MutationResult>
    where
        T: Serialize,
    {
        let serialized = match to_vec(&content) {
            Ok(v) => v,
            Err(e) => {
                return Err(CouchbaseError::EncodingFailure {
                    ctx: ErrorContext::default(),
                    source: e.into(),
                })
            }
        };

        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Mutate(MutateRequest {
            id: id.into(),
            content: serialized,
            sender,
            bucket: self.bucket_name.clone(),
            ty,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn remove<S: Into<String>>(
        &self,
        id: S,
        options: RemoveOptions,
    ) -> CouchbaseResult<MutationResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Remove(RemoveRequest {
            id: id.into(),
            sender,
            bucket: self.bucket_name.clone(),
            options,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn lookup_in(
        &self,
        id: impl Into<String>,
        specs: impl IntoIterator<Item = LookupInSpec>,
        options: LookupInOptions,
    ) -> CouchbaseResult<LookupInResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::LookupIn(LookupInRequest {
            id: id.into(),
            specs: specs.into_iter().collect::<Vec<LookupInSpec>>(),
            sender,
            bucket: self.bucket_name.clone(),
            options,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn mutate_in(
        &self,
        id: impl Into<String>,
        specs: impl IntoIterator<Item = MutateInSpec>,
        options: MutateInOptions,
    ) -> CouchbaseResult<MutateInResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::MutateIn(MutateInRequest {
            id: id.into(),
            specs: specs.into_iter().collect::<Vec<MutateInSpec>>(),
            sender,
            bucket: self.bucket_name.clone(),
            options,
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub fn binary(&self) -> BinaryCollection {
        BinaryCollection::new(
            self.core.clone(),
            self.name.clone(),
            self.scope_name.clone(),
            self.bucket_name.clone(),
        )
    }
}

#[derive(Debug)]
pub struct MutationState {
    tokens: Vec<MutationToken>,
}

#[derive(Debug)]
pub struct MutationToken {
    partition_uuid: u64,
    sequence_number: u64,
    partition_id: u16,
    bucket_name: String,
}

impl MutationToken {
    pub fn new(
        partition_uuid: u64,
        sequence_number: u64,
        partition_id: u16,
        bucket_name: String,
    ) -> Self {
        Self {
            partition_uuid,
            sequence_number,
            partition_id,
            bucket_name,
        }
    }

    pub fn partition_uuid(&self) -> u64 {
        self.partition_uuid
    }

    pub fn sequence_number(&self) -> u64 {
        self.sequence_number
    }

    pub fn partition_id(&self) -> u16 {
        self.partition_id
    }

    pub fn bucket_name(&self) -> &String {
        &self.bucket_name
    }
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum MutationMacro {
    CAS,
    SeqNo,
    CRC32c,
}

impl Serialize for MutationMacro {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let alias = match *self {
            MutationMacro::CAS => MUTATION_MACRO_CAS,
            MutationMacro::SeqNo => MUTATION_MACRO_SEQNO,
            MutationMacro::CRC32c => MUTATION_MACRO_VALUE_CRC32C,
        };
        serializer.serialize_str(alias)
    }
}

impl Display for MutationMacro {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let alias = match *self {
            MutationMacro::CAS => MUTATION_MACRO_CAS,
            MutationMacro::SeqNo => MUTATION_MACRO_SEQNO,
            MutationMacro::CRC32c => MUTATION_MACRO_VALUE_CRC32C,
        };

        write!(f, "{}", alias)
    }
}

impl Debug for MutationMacro {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let alias = match *self {
            MutationMacro::CAS => MUTATION_MACRO_CAS,
            MutationMacro::SeqNo => MUTATION_MACRO_SEQNO,
            MutationMacro::CRC32c => MUTATION_MACRO_VALUE_CRC32C,
        };

        write!(f, "{}", alias)
    }
}

#[derive(Debug)]
pub enum MutateInSpec {
    Replace {
        path: String,
        value: Vec<u8>,
        xattr: bool,
    },
    Insert {
        path: String,
        value: Vec<u8>,
        create_path: bool,
        xattr: bool,
    },
    Upsert {
        path: String,
        value: Vec<u8>,
        create_path: bool,
        xattr: bool,
    },
    ArrayAddUnique {
        path: String,
        value: Vec<u8>,
        create_path: bool,
        xattr: bool,
    },
    Remove {
        path: String,
        xattr: bool,
    },
    Counter {
        path: String,
        delta: i64,
        create_path: bool,
        xattr: bool,
    },
    ArrayAppend {
        path: String,
        value: Vec<u8>,
        create_path: bool,
        xattr: bool,
    },
    ArrayPrepend {
        path: String,
        value: Vec<u8>,
        create_path: bool,
        xattr: bool,
    },
    ArrayInsert {
        path: String,
        value: Vec<u8>,
        create_path: bool,
        xattr: bool,
    },
}

impl MutateInSpec {
    pub fn replace<S: Into<String>, T>(
        path: S,
        content: T,
        opts: ReplaceSpecOptions,
    ) -> CouchbaseResult<Self>
    where
        T: Serialize,
    {
        let value = to_vec(&content).map_err(CouchbaseError::encoding_failure_from_serde)?;
        Ok(MutateInSpec::Replace {
            path: path.into(),
            value,
            xattr: opts.xattr,
        })
    }

    pub fn insert<S: Into<String>, T>(
        path: S,
        content: T,
        opts: InsertSpecOptions,
    ) -> CouchbaseResult<Self>
    where
        T: Serialize,
    {
        let value = to_vec(&content).map_err(CouchbaseError::encoding_failure_from_serde)?;
        Ok(MutateInSpec::Insert {
            path: path.into(),
            value,
            create_path: opts.create_path,
            xattr: opts.xattr,
        })
    }

    pub fn upsert<S: Into<String>, T>(
        path: S,
        content: T,
        opts: UpsertSpecOptions,
    ) -> CouchbaseResult<Self>
    where
        T: Serialize,
    {
        let value = to_vec(&content).map_err(CouchbaseError::encoding_failure_from_serde)?;
        Ok(MutateInSpec::Upsert {
            path: path.into(),
            value,
            create_path: opts.create_path,
            xattr: opts.xattr,
        })
    }

    pub fn array_add_unique<S: Into<String>, T>(
        path: S,
        content: T,
        opts: ArrayAddUniqueSpecOptions,
    ) -> CouchbaseResult<Self>
    where
        T: Serialize,
    {
        let value = to_vec(&content).map_err(CouchbaseError::encoding_failure_from_serde)?;
        Ok(MutateInSpec::ArrayAddUnique {
            path: path.into(),
            value,
            create_path: opts.create_path,
            xattr: opts.xattr,
        })
    }

    pub fn array_append<S: Into<String>, T>(
        path: S,
        content: impl IntoIterator<Item = T>,
        opts: ArrayAppendSpecOptions,
    ) -> CouchbaseResult<Self>
    where
        T: Serialize,
    {
        let mut value = vec![];
        content.into_iter().try_for_each(|v| {
            match to_vec(&v) {
                Ok(v) => value.extend(v),
                Err(e) => return Err(CouchbaseError::encoding_failure_from_serde(e)),
            };
            value.push(b',');
            Ok(())
        })?;
        if value.pop().is_none() {
            let mut ctx = ErrorContext::default();
            ctx.insert(
                "content",
                Value::String(String::from("content must contain at least one item")),
            );
            return Err(CouchbaseError::InvalidArgument { ctx });
        }

        Ok(MutateInSpec::ArrayAppend {
            path: path.into(),
            value,
            create_path: opts.create_path,
            xattr: opts.xattr,
        })
    }

    pub fn array_prepend<S: Into<String>, T>(
        path: S,
        content: impl IntoIterator<Item = T>,
        opts: ArrayPrependSpecOptions,
    ) -> CouchbaseResult<Self>
    where
        T: Serialize,
    {
        let mut value = vec![];
        content.into_iter().try_for_each(|v| {
            match to_vec(&v) {
                Ok(v) => value.extend(v),
                Err(e) => return Err(CouchbaseError::encoding_failure_from_serde(e)),
            };
            value.push(b',');
            Ok(())
        })?;
        if value.pop().is_none() {
            let mut ctx = ErrorContext::default();
            ctx.insert(
                "content",
                Value::String(String::from("content must contain at least one item")),
            );
            return Err(CouchbaseError::InvalidArgument { ctx });
        }

        Ok(MutateInSpec::ArrayPrepend {
            path: path.into(),
            value,
            create_path: opts.create_path,
            xattr: opts.xattr,
        })
    }

    pub fn array_insert<S: Into<String>, T>(
        path: S,
        content: impl IntoIterator<Item = T>,
        opts: ArrayInsertSpecOptions,
    ) -> CouchbaseResult<Self>
    where
        T: Serialize,
    {
        let mut value = vec![];
        content.into_iter().try_for_each(|v| {
            match to_vec(&v) {
                Ok(v) => value.extend(v),
                Err(e) => return Err(CouchbaseError::encoding_failure_from_serde(e)),
            };
            value.push(b',');
            Ok(())
        })?;
        if value.pop().is_none() {
            let mut ctx = ErrorContext::default();
            ctx.insert(
                "content",
                Value::String(String::from("content must contain at least one item")),
            );
            return Err(CouchbaseError::InvalidArgument { ctx });
        }

        Ok(MutateInSpec::ArrayInsert {
            path: path.into(),
            value,
            create_path: opts.create_path,
            xattr: opts.xattr,
        })
    }

    pub fn remove<S: Into<String>>(path: S, opts: RemoveSpecOptions) -> CouchbaseResult<Self> {
        Ok(MutateInSpec::Remove {
            path: path.into(),
            xattr: opts.xattr,
        })
    }

    pub fn increment<S: Into<String>>(
        path: S,
        delta: u64,
        opts: IncrementSpecOptions,
    ) -> CouchbaseResult<Self> {
        Ok(MutateInSpec::Counter {
            path: path.into(),
            delta: delta as i64,
            create_path: opts.create_path,
            xattr: opts.xattr,
        })
    }

    pub fn decrement<S: Into<String>>(
        path: S,
        delta: u64,
        opts: DecrementSpecOptions,
    ) -> CouchbaseResult<Self> {
        Ok(MutateInSpec::Counter {
            path: path.into(),
            delta: -(delta as i64),
            create_path: opts.create_path,
            xattr: opts.xattr,
        })
    }
}

#[derive(Debug)]
pub enum LookupInSpec {
    Get { path: String, xattr: bool },
    Exists { path: String, xattr: bool },
    Count { path: String, xattr: bool },
}

impl LookupInSpec {
    pub fn get<S: Into<String>>(path: S, opts: GetSpecOptions) -> Self {
        LookupInSpec::Get {
            path: path.into(),
            xattr: opts.xattr,
        }
    }

    pub fn exists<S: Into<String>>(path: S, opts: ExistsSpecOptions) -> Self {
        LookupInSpec::Exists {
            path: path.into(),
            xattr: opts.xattr,
        }
    }

    pub fn count<S: Into<String>>(path: S, opts: CountSpecOptions) -> Self {
        LookupInSpec::Count {
            path: path.into(),
            xattr: opts.xattr,
        }
    }
}

pub struct BinaryCollection {
    core: Arc<Core>,
    name: String,
    scope_name: String,
    bucket_name: String,
}

impl BinaryCollection {
    pub(crate) fn new(
        core: Arc<Core>,
        name: String,
        scope_name: String,
        bucket_name: String,
    ) -> Self {
        Self {
            core,
            name,
            scope_name,
            bucket_name,
        }
    }

    pub async fn append<S: Into<String>>(
        &self,
        id: S,
        content: Vec<u8>,
        options: AppendOptions,
    ) -> CouchbaseResult<MutationResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Mutate(MutateRequest {
            id: id.into(),
            content,
            sender,
            bucket: self.bucket_name.clone(),
            ty: MutateRequestType::Append { options },
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn prepend<S: Into<String>>(
        &self,
        id: S,
        content: Vec<u8>,
        options: PrependOptions,
    ) -> CouchbaseResult<MutationResult> {
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Mutate(MutateRequest {
            id: id.into(),
            content,
            sender,
            bucket: self.bucket_name.clone(),
            ty: MutateRequestType::Prepend { options },
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn increment<S: Into<String>>(
        &self,
        id: S,
        options: IncrementOptions,
    ) -> CouchbaseResult<CounterResult> {
        let delta = match options.delta {
            Some(d) => i64::try_from(d).map_err(|_e| CouchbaseError::Generic {
                // TODO: we shouldn't swallow the error detail.
                ctx: ErrorContext::default(),
            })?,
            None => 1,
        };
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Counter(CounterRequest {
            id: id.into(),
            sender,
            bucket: self.bucket_name.clone(),
            options: CounterOptions {
                timeout: options.timeout,
                cas: options.cas,
                expiry: options.expiry,
                delta,
            },
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }

    pub async fn decrement<S: Into<String>>(
        &self,
        id: S,
        options: DecrementOptions,
    ) -> CouchbaseResult<CounterResult> {
        let delta = match options.delta {
            Some(d) => {
                -(i64::try_from(d).map_err(|_e| CouchbaseError::Generic {
                    // TODO: we shouldn't swallow the error detail.
                    ctx: ErrorContext::default(),
                })?)
            }
            None => -1,
        };
        let (sender, receiver) = oneshot::channel();
        self.core.send(Request::Counter(CounterRequest {
            id: id.into(),
            sender,
            bucket: self.bucket_name.clone(),
            options: CounterOptions {
                timeout: options.timeout,
                cas: options.cas,
                expiry: options.expiry,
                delta,
            },
            scope: self.scope_name.clone(),
            collection: self.name.clone(),
        }));
        receiver.await.unwrap()
    }
}

#[derive(Debug, Copy, Clone)]
pub enum DurabilityLevel {
    None = 0x00,
    Majority = 0x01,
    MajorityAndPersistOnMaster = 0x02,
    PersistToMajority = 0x03,
}

impl Default for DurabilityLevel {
    fn default() -> Self {
        DurabilityLevel::None
    }
}

impl Display for DurabilityLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let alias = match *self {
            DurabilityLevel::None => "none",
            DurabilityLevel::Majority => "majority",
            DurabilityLevel::MajorityAndPersistOnMaster => "majorityAndPersistActive",
            DurabilityLevel::PersistToMajority => "persistToMajority",
        };

        write!(f, "{}", alias)
    }
}

impl TryFrom<&str> for DurabilityLevel {
    type Error = CouchbaseError;

    fn try_from(alias: &str) -> Result<Self, Self::Error> {
        match alias {
            "none" => Ok(DurabilityLevel::None),
            "majority" => Ok(DurabilityLevel::Majority),
            "majorityAndPersistActive" => Ok(DurabilityLevel::MajorityAndPersistOnMaster),
            "persistToMajority" => Ok(DurabilityLevel::PersistToMajority),
            _ => {
                let mut ctx = ErrorContext::default();
                ctx.insert(alias, "invalid durability mode".into());
                Err(Generic { ctx })
            }
        }
    }
}

// Internal: Do not implement.
// The only supported implementations of Authenticator are PasswordAuthenticator and
// CertificateAuthenticator.
pub trait Authenticator: Debug {
    fn username(&self) -> Option<&String>;
    fn password(&self) -> Option<&String>;
    fn certificate_path(&self) -> Option<&String>;
    fn key_path(&self) -> Option<&String>;
}

#[derive(Debug, Clone)]
pub struct PasswordAuthenticator {
    username: String,
    password: String,
}

impl PasswordAuthenticator {
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

impl Authenticator for PasswordAuthenticator {
    fn username(&self) -> Option<&String> {
        Some(&self.username)
    }

    fn password(&self) -> Option<&String> {
        Some(&self.password)
    }

    fn certificate_path(&self) -> Option<&String> {
        None
    }

    fn key_path(&self) -> Option<&String> {
        None
    }
}

#[derive(Debug, Clone)]
pub struct CertificateAuthenticator {
    cert_path: String,
    key_path: String,
}

impl CertificateAuthenticator {
    pub fn new(cert_path: impl Into<String>, key_path: impl Into<String>) -> Self {
        Self {
            cert_path: cert_path.into(),
            key_path: key_path.into(),
        }
    }
}

impl Authenticator for CertificateAuthenticator {
    fn username(&self) -> Option<&String> {
        None
    }

    fn password(&self) -> Option<&String> {
        None
    }

    fn certificate_path(&self) -> Option<&String> {
        Some(&self.cert_path)
    }

    fn key_path(&self) -> Option<&String> {
        Some(&self.key_path)
    }
}
