// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Various mock versions of the various clients and other objects.
//!
//! The goal is to be able to test functionality independently of the rest of
//! the system, in particular without requiring a TiKV or PD server, or RPC layer.

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
use derive_new::new;

use crate::pd::PdClient;
use crate::pd::PdRpcClient;
use crate::pd::RetryClient;
use crate::proto::keyspacepb::KeyspaceMeta;
use crate::proto::kvrpcpb::KvPair;
use crate::proto::metapb::{self};
use crate::proto::metapb::{Peer, Region, RegionEpoch};
use crate::proto::{keyspacepb, kvrpcpb};
use crate::region::RegionWithLeader;
use crate::region::{RegionId, RegionVerId};
use crate::store::KvConnect;
use crate::store::RegionStore;
use crate::store::Request;
use crate::store::{KvClient, Store};
use crate::Error;
use crate::Key;
use crate::Result;
use crate::Timestamp;
use crate::{BoundRange, Config};

/// Create a `PdRpcClient` with it's internals replaced with mocks so that the
/// client can be tested without doing any RPC calls.
pub async fn pd_rpc_client() -> PdRpcClient<MockKvConnect, MockCluster> {
    let config = Config::default();
    PdRpcClient::new(
        config.clone(),
        |_| MockKvConnect,
        |sm| {
            futures::future::ok(RetryClient::new_with_cluster(
                sm,
                config.timeout,
                MockCluster,
            ))
        },
        false,
    )
    .await
    .unwrap()
}

#[allow(clippy::type_complexity)]
#[derive(new, Default, Clone)]
pub struct MockKvClient {
    pub addr: String,
    dispatch: Option<Arc<dyn Fn(&dyn Any) -> Result<Box<dyn Any>> + Send + Sync + 'static>>,
}

impl MockKvClient {
    pub fn with_dispatch_hook<F>(dispatch: F) -> MockKvClient
    where
        F: Fn(&dyn Any) -> Result<Box<dyn Any>> + Send + Sync + 'static,
    {
        MockKvClient {
            addr: String::new(),
            dispatch: Some(Arc::new(dispatch)),
        }
    }
}

pub struct MockKvConnect;

pub struct MockCluster;

#[derive(new)]
pub struct MockPdClient {
    client: MockKvClient,
}

pub struct InjectableMockPdClient {
    regions: HashMap<u64, Arc<MockRegion>>,
    key_to_region: BTreeMap<Key, RegionWithLeader>,
}

impl InjectableMockPdClient {
    pub fn default() -> Self {
        InjectableMockPdClient {
            regions: HashMap::default(),
            key_to_region: Default::default(),
        }
    }

    pub fn region(&mut self, mock_region: Arc<MockRegion>) {
        self.regions.insert(mock_region.id, mock_region.clone());
        let region_with_leader: RegionWithLeader = mock_region.deref().into();
        self.key_to_region
            .insert(mock_region.start_key.clone(), region_with_leader);
    }
}

pub struct InjectableMockKvClient {
    data: BTreeMap<Key, Vec<u8>>,
}

#[async_trait]
impl KvClient for InjectableMockKvClient {
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any>> {
        if let Some(req) = req.as_any().downcast_ref::<kvrpcpb::RawScanRequest>() {
            let start_key: Key = req.start_key.clone().into();
            let end_key: Key = req.end_key.clone().into();
            let range: BoundRange = if !req.reverse {
                (start_key, end_key).into()
            } else {
                (end_key, start_key).into()
            };
            let resp: Vec<_> = self
                .data
                .range(range)
                .map(|(k, v)| KvPair {
                    error: None,
                    key: k.0.clone(),
                    value: v.clone(),
                })
                .collect();
            let resp: Vec<KvPair> = if req.reverse {
                resp.into_iter().rev().collect()
            } else {
                resp
            };
            let resp = kvrpcpb::RawScanResponse {
                region_error: None,
                kvs: resp,
            };
            Ok(Box::new(resp) as Box<dyn Any>)
        } else {
            unreachable!()
        }
    }
}

#[async_trait]
impl PdClient for InjectableMockPdClient {
    type KvClient = InjectableMockKvClient;

    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore> {
        let mock_region = self.regions.get(&region.id()).expect("no region found");
        let kv_client = InjectableMockKvClient {
            data: mock_region.data.clone(),
        };
        Ok(RegionStore::new(region, Arc::new(kv_client)))
    }

    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let k = self
            .key_to_region
            .range(..=key)
            .next_back()
            .map(|(x, y)| (x.clone(), y.clone()));

        Ok(k.expect("no region entry found").1)
    }

    async fn prev_region_for_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let k = self
            .key_to_region
            .range(..key)
            .next_back()
            .map(|(x, y)| (x.clone(), y.clone()));

        Ok(k.expect("no region entry found").1)
    }

    async fn region_for_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        let region = self.regions.get(&id).unwrap().deref().into();
        Ok(region)
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        unimplemented!()
    }

    async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
        unimplemented!()
    }

    async fn load_keyspace(&self, _keyspace: &str) -> Result<KeyspaceMeta> {
        unimplemented!()
    }

    async fn all_stores(&self) -> Result<Vec<Store>> {
        unimplemented!()
    }

    async fn update_leader(&self, _ver_id: RegionVerId, _leader: Peer) -> Result<()> {
        unimplemented!()
    }

    async fn invalidate_region_cache(&self, _ver_id: RegionVerId) {
        unimplemented!()
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct MockRegion {
    id: u64,
    start_key: Key,
    end_key: Key,
    data: BTreeMap<Key, Vec<u8>>,
    key_to_region: BTreeMap<Key, RegionWithLeader>,
}

impl From<&MockRegion> for RegionWithLeader {
    fn from(v: &MockRegion) -> Self {
        let start_key = v.start_key.clone();
        let end_key = v.end_key.clone();

        RegionWithLeader {
            region: Region {
                id: v.id,
                start_key: start_key.into(),
                end_key: end_key.into(),
                region_epoch: None,
                peers: vec![],
                encryption_meta: None,
                is_in_flashback: false,
                flashback_start_ts: 0,
            },
            leader: Some(Peer {
                id: v.id,
                store_id: v.id,
                role: 0,
                is_witness: false,
            }),
        }
    }
}

impl MockRegion {
    pub fn default() -> Self {
        Self {
            id: 0,
            start_key: Default::default(),
            end_key: Default::default(),
            data: Default::default(),
            key_to_region: Default::default(),
        }
    }

    pub fn id(&mut self, id: u64) {
        self.id = id;
    }
    pub fn start_key(&mut self, key: impl Into<Key>) {
        self.start_key = key.into();
    }
    pub fn end_key(&mut self, key: impl Into<Key>) {
        self.end_key = key.into();
    }
    pub fn entry(&mut self, key: impl Into<Key>, value: impl Into<Vec<u8>>) {
        self.data.insert(key.into(), value.into());
    }
}

#[async_trait]
impl KvClient for MockKvClient {
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any>> {
        match &self.dispatch {
            Some(f) => f(req.as_any()),
            None => panic!("no dispatch hook set"),
        }
    }
}

#[async_trait]
impl KvConnect for MockKvConnect {
    type KvClient = MockKvClient;

    async fn connect(&self, address: &str) -> Result<Self::KvClient> {
        Ok(MockKvClient {
            addr: address.to_owned(),
            dispatch: None,
        })
    }
}

impl MockPdClient {
    pub fn default() -> MockPdClient {
        MockPdClient {
            client: MockKvClient::default(),
        }
    }

    pub fn region1() -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = 1;
        region.region.start_key = vec![];
        region.region.end_key = vec![10];
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 0,
        });

        let leader = metapb::Peer {
            store_id: 41,
            ..Default::default()
        };
        region.leader = Some(leader);

        region
    }

    pub fn region2() -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = 2;
        region.region.start_key = vec![10];
        region.region.end_key = vec![250, 250];
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 0,
        });

        let leader = metapb::Peer {
            store_id: 42,
            ..Default::default()
        };
        region.leader = Some(leader);

        region
    }

    pub fn region3() -> RegionWithLeader {
        let mut region = RegionWithLeader::default();
        region.region.id = 3;
        region.region.start_key = vec![250, 250];
        region.region.end_key = vec![];
        region.region.region_epoch = Some(RegionEpoch {
            conf_ver: 0,
            version: 0,
        });

        let leader = metapb::Peer {
            store_id: 43,
            ..Default::default()
        };
        region.leader = Some(leader);

        region
    }
}

#[async_trait]
impl PdClient for MockPdClient {
    type KvClient = MockKvClient;

    async fn map_region_to_store(self: Arc<Self>, region: RegionWithLeader) -> Result<RegionStore> {
        Ok(RegionStore::new(region, Arc::new(self.client.clone())))
    }

    async fn region_for_key(&self, key: &Key) -> Result<RegionWithLeader> {
        let bytes: &[_] = key.into();
        let region = if bytes.is_empty() || bytes < &[10][..] {
            Self::region1()
        } else if bytes >= &[10][..] && bytes < &[250, 250][..] {
            Self::region2()
        } else {
            Self::region3()
        };

        Ok(region)
    }

    async fn prev_region_for_key(&self, _key: &Key) -> Result<RegionWithLeader> {
        todo!()
    }

    async fn region_for_id(&self, id: RegionId) -> Result<RegionWithLeader> {
        match id {
            1 => Ok(Self::region1()),
            2 => Ok(Self::region2()),
            3 => Ok(Self::region3()),
            _ => Err(Error::RegionNotFoundInResponse { region_id: id }),
        }
    }

    async fn all_stores(&self) -> Result<Vec<Store>> {
        Ok(vec![Store::new(Arc::new(self.client.clone()))])
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        Ok(Timestamp::default())
    }

    async fn update_safepoint(self: Arc<Self>, _safepoint: u64) -> Result<bool> {
        unimplemented!()
    }

    async fn update_leader(
        &self,
        _ver_id: crate::region::RegionVerId,
        _leader: metapb::Peer,
    ) -> Result<()> {
        todo!()
    }

    async fn invalidate_region_cache(&self, _ver_id: crate::region::RegionVerId) {}

    async fn load_keyspace(&self, _keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        unimplemented!()
    }
}
