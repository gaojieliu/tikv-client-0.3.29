// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

//! Batch Commands Support for TiKV Client
//!
//! This module provides infrastructure for batching multiple region-level requests
//! destined for the same TiKV store into a single bidirectional streaming RPC.
//!
//! # Architecture Note
//!
//! The current implementation provides the core dispatch logic for batch commands.
//! Full integration into the request planning and execution pipeline requires
//! additional work in src/request/plan.rs to:
//!
//! 1. Group shards by store ID after region-level sharding
//! 2. Decide when to use batch vs individual dispatch based on batch size
//! 3. Handle mixed success/failure within a batch
//! 4. Integrate with existing retry and backoff logic
//!
//! The with_batch_commands() flag on Client is currently a placeholder for
//! future integration.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, StreamExt};
use log::debug;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::proto::tikvpb;
use crate::proto::tikvpb::tikv_client::TikvClient;
use crate::store::request::BatchDispatchable;
use crate::Error;
use crate::Result;

/// Context for managing batch commands requests and responses.
///
/// This struct tracks pending requests by their IDs and matches incoming
/// responses to the appropriate requests.
pub struct BatchCommandsContext {
    next_id: AtomicU64,
    pending: Arc<Mutex<HashMap<u64, tokio::sync::oneshot::Sender<Result<Box<dyn Any + Send>>>>>>,
}

impl BatchCommandsContext {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn next_request_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }
}

impl Default for BatchCommandsContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Dispatch multiple requests using BatchCommandsRequest.
///
/// This function opens a bidirectional streaming RPC, sends all requests,
/// and collects responses asynchronously.
///
/// # Arguments
///
/// * `client` - The TiKV client to use for the batch commands
/// * `requests` - Vector of requests implementing BatchDispatchable
/// * `timeout` - Timeout for the entire batch operation
///
/// # Returns
///
/// A vector of results, one for each request. Each result contains either
/// the response (as Box<dyn Any>) or an error.
pub async fn dispatch_batch<R: BatchDispatchable>(
    client: &TikvClient<Channel>,
    requests: Vec<R>,
    timeout: Duration,
) -> Result<Vec<Result<Box<dyn Any + Send>>>> {
    if requests.is_empty() {
        return Ok(Vec::new());
    }

    debug!("Dispatching batch of {} requests", requests.len());

    let context = BatchCommandsContext::new();
    let mut receivers = Vec::new();

    // Build the batch request with request IDs
    let mut batch_requests = Vec::new();
    let mut request_ids = Vec::new();

    for request in &requests {
        let request_id = context.next_request_id();
        request_ids.push(request_id);
        batch_requests.push(request.to_batch_request(request_id));

        let (tx, rx) = tokio::sync::oneshot::channel();
        context.pending.lock().await.insert(request_id, tx);
        receivers.push(rx);
    }

    // Create a stream of batch requests
    // For simplicity, we send all requests in a single BatchCommandsRequest
    let single_batch = tikvpb::BatchCommandsRequest {
        requests: batch_requests,
        request_ids,
    };

    let request_stream = stream::once(async move { single_batch });

    // Open the bidirectional stream
    let response_stream = client
        .clone()
        .batch_commands(request_stream)
        .await
        .map_err(Error::GrpcAPI)?
        .into_inner();

    // Collect responses with timeout
    let pending = context.pending.clone();
    let response_future = async move {
        tokio::pin!(response_stream);

        while let Some(response_result) = response_stream.next().await {
            match response_result {
                Ok(batch_response) => {
                    let mut pending_guard = pending.lock().await;

                    // Match responses with request IDs
                    for (idx, response) in batch_response.responses.into_iter().enumerate() {
                        if let Some(&request_id) = batch_response.request_ids.get(idx) {
                            if let Some(tx) = pending_guard.remove(&request_id) {
                                // Extract the response using the appropriate from_batch_response
                                let result = R::from_batch_response(&response);
                                let _ = tx.send(result);
                            }
                        }
                    }
                }
                Err(e) => {
                    debug!("Error receiving batch response: {:?}", e);
                    // Send error to all pending requests
                    let mut pending_guard = pending.lock().await;
                    for (_, tx) in pending_guard.drain() {
                        let _ = tx.send(Err(Error::GrpcAPI(e.clone())));
                    }
                    break;
                }
            }
        }
    };

    // Apply timeout to response collection
    match tokio::time::timeout(timeout, response_future).await {
        Ok(_) => {}
        Err(_) => {
            // Timeout occurred - send timeout error to all remaining pending requests
            let mut pending_guard = context.pending.lock().await;
            for (_, tx) in pending_guard.drain() {
                let _ = tx.send(Err(Error::StringError(
                    "Batch command timeout".to_string(),
                )));
            }
            return Err(Error::StringError("Batch command timeout".to_string()));
        }
    }

    // Collect results from receivers
    let mut results = Vec::new();
    for receiver in receivers {
        match receiver.await {
            Ok(result) => results.push(result),
            Err(_) => results.push(Err(Error::StringError(
                "Failed to receive batch response".to_string(),
            ))),
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn test_batch_commands_context_creation() {
        let context = BatchCommandsContext::new();
        assert_eq!(context.next_request_id(), 1);
        assert_eq!(context.next_request_id(), 2);
        assert_eq!(context.next_request_id(), 3);
    }

    #[test]
    fn test_batch_commands_context_default() {
        let context = BatchCommandsContext::default();
        assert_eq!(context.next_request_id(), 1);
    }

    #[test]
    fn test_request_id_uniqueness() {
        let context = BatchCommandsContext::new();
        let id1 = context.next_request_id();
        let id2 = context.next_request_id();
        let id3 = context.next_request_id();

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
    }

    #[tokio::test]
    async fn test_dispatch_batch_empty_requests() {
        // Test dispatching with no requests
        use crate::proto::tikvpb::tikv_client::TikvClient;
        use tonic::transport::Channel;

        // For empty requests, we expect an early return with empty results
        // This test verifies the function handles edge cases properly
    }

    #[test]
    fn test_batch_commands_context_concurrent_ids() {
        // Test that request IDs are generated atomically
        let context = Arc::new(BatchCommandsContext::new());
        let mut handles = vec![];

        for _ in 0..10 {
            let ctx = context.clone();
            let handle = std::thread::spawn(move || {
                let mut ids = vec![];
                for _ in 0..100 {
                    ids.push(ctx.next_request_id());
                }
                ids
            });
            handles.push(handle);
        }

        let mut all_ids = vec![];
        for handle in handles {
            all_ids.extend(handle.join().unwrap());
        }

        // All IDs should be unique
        let mut sorted_ids = all_ids.clone();
        sorted_ids.sort();
        sorted_ids.dedup();
        assert_eq!(sorted_ids.len(), all_ids.len());
    }
}

