// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use derive_new::new;
use tonic::transport::Channel;

use super::Request;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::Result;
use crate::SecurityManager;

/// A trait for connecting to TiKV stores.
#[async_trait]
pub trait KvConnect: Sized + Send + Sync + 'static {
    type KvClient: KvClient + Clone + Send + Sync + 'static;

    async fn connect(&self, address: &str) -> Result<Self::KvClient>;
}

#[derive(new, Clone)]
pub struct TikvConnect {
    security_mgr: Arc<SecurityManager>,
    timeout: Duration,
}

#[async_trait]
impl KvConnect for TikvConnect {
    type KvClient = KvRpcClient;

    async fn connect(&self, address: &str) -> Result<KvRpcClient> {
                // Create a list of clients to support multiple concurrent requests. 
        let mut rpc_clients = Vec::new();
        // 10 conns by default
        for _ in 0..10 {
            // Sleep for some time to avoid overwhelming the server with connection attempts.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let rpc_client = self.security_mgr
                .connect(address, TikvClient::new)
                .await?;
            rpc_clients.push(rpc_client);
        }
        // self.security_mgr
        //     .connect(address, TikvClient::new)
        //     .await
        //     .map(|c| KvRpcClient::new(c, self.timeout))
        log::info!("Created 10 connections to TiKV at {}", address);
        Ok(KvRpcClient {
            rpc_clients,
            timeout: self.timeout,
            store_request_id: Arc::new(AtomicU64::new(0)),
        })
    }
}

#[async_trait]
pub trait KvClient {
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any>>;

    /// Try to get the underlying TikvClient if this is a KvRpcClient
    fn as_tikv_client(&self) -> Option<&TikvClient<Channel>> {
        None
    }

    /// Get the timeout duration for this client
    fn timeout(&self) -> Duration {
        Duration::from_secs(60) // Default timeout
    }
}

/// This client handles requests for a single TiKV node. It converts the data
/// types and abstractions of the client program into the grpc data types.
#[derive(new, Clone)]
pub struct KvRpcClient {
    //rpc_client: TikvClient<Channel>,
    // Create a list of clients to support multiple concurrent requests.
    rpc_clients: Vec<TikvClient<Channel>>,
    timeout: Duration,
    store_request_id: Arc<AtomicU64>,
}

impl KvRpcClient {
    fn next_request_id(&self) -> u64 {
        self.store_request_id.fetch_add(1, Ordering::SeqCst)
    }
}

#[async_trait]
impl KvClient for KvRpcClient {

    async fn dispatch(&self, request: &dyn Request) -> Result<Box<dyn Any>> {
        request.dispatch(&self.rpc_clients[self.next_request_id() as usize % self.rpc_clients.len()], self.timeout).await
    }

    fn as_tikv_client(&self) -> Option<&TikvClient<Channel>> {
        Some(&self.rpc_clients[self.next_request_id() as usize % self.rpc_clients.len()])
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }
}
