//! HTTP SSZ transport proxy for the authenticated Engine API server.
//!
//! Implements the [EIP-8178] SSZ Engine API routes under `/engine/v1`.
//!
//! [EIP-8178]: https://eips.ethereum.org/EIPS/eip-8178

use crate::engine_ssz_containers::{
    BlobsV1Request, BlobsV1Response, BlobsV2Response, BlobsV3Response, BlobsV4Request,
    BlobsV4Response, BodiesByHashRequest, BodiesResponse, BuiltPayloadAmsterdam, BuiltPayloadOsaka,
    BuiltPayloadParis, BuiltPayloadPrague, BuiltPayloadShanghai, ExecutionPayloadBodyAmsterdam,
    ExecutionPayloadBodyParis, ExecutionPayloadBodyShanghai, ExecutionPayloadEnvelopeAmsterdam,
    ExecutionPayloadEnvelopeCancun, ExecutionPayloadEnvelopeOsaka, ExecutionPayloadEnvelopeParis,
    ExecutionPayloadEnvelopePrague, ExecutionPayloadEnvelopeShanghai, ForkchoiceUpdateAmsterdam,
    ForkchoiceUpdateCancun, ForkchoiceUpdateOsaka, ForkchoiceUpdateParis, ForkchoiceUpdatePrague,
    ForkchoiceUpdateResponse, ForkchoiceUpdateShanghai, Optional,
    PayloadStatus as EngineSszPayloadStatus,
};
use alloy_consensus::{Transaction, TxEnvelope};
use alloy_eips::{
    eip2718::Decodable2718,
    eip7685::{Requests, RequestsOrHash},
};
use alloy_primitives::{Bytes, B128, B256, B64};
use alloy_rpc_types_engine::{
    CancunPayloadFields, ExecutionData, ExecutionPayload, ExecutionPayloadBodyV1,
    ExecutionPayloadFieldV2, ExecutionPayloadSidecar, ExecutionPayloadV1, ExecutionPayloadV2,
    ExecutionPayloadV3, ExecutionPayloadV4, ForkchoiceState, PayloadAttributes, PayloadId,
    PraguePayloadFields,
};
use http_body_util::{BodyExt, LengthLimitError, Limited};
use jsonrpsee::server::{HttpBody, HttpRequest, HttpResponse};
use reth_chainspec::{EthereumHardfork, EthereumHardforks};
use reth_engine_primitives::EngineApiValidator;
use reth_ethereum_engine_primitives::EthEngineTypes;
use reth_provider::{BlockReader, HeaderProvider, StateProviderFactory};
use reth_rpc::EngineApi;
use reth_rpc_engine_api::EngineApiError;
use reth_transaction_pool::TransactionPool;
use ssz::Decode;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::RwLock;
use tower::{BoxError, Layer, Service};

const OCTET_STREAM: &str = "application/octet-stream";
const APPLICATION_JSON: &str = "application/json";
const TEXT_PLAIN: &str = "text/plain";
const CONTENT_TYPE: &str = "content-type";
const CACHE_CONTROL: &str = "cache-control";
const ETH_EXECUTION_VERSION: &str = "eth-execution-version";

const STATUS_OK: u16 = 200;
const STATUS_BAD_REQUEST: u16 = 400;
const STATUS_NOT_FOUND: u16 = 404;
const STATUS_CONFLICT: u16 = 409;
const STATUS_METHOD_NOT_ALLOWED: u16 = 405;
const STATUS_PAYLOAD_TOO_LARGE: u16 = 413;
const STATUS_INTERNAL_SERVER_ERROR: u16 = 500;
const STATUS_SERVICE_UNAVAILABLE: u16 = 503;
const STATUS_UNSUPPORTED_MEDIA_TYPE: u16 = 415;
const STATUS_UNPROCESSABLE_ENTITY: u16 = 422;

const MAX_BLOB_LIMIT: usize = crate::engine_ssz_containers::MAX_BLOBS_REQUEST;
const MAX_BLOB_REQUEST_BYTES: usize = 4 + 16 + MAX_BLOB_LIMIT * 32;
const MAX_BODIES_REQUEST: usize = crate::engine_ssz_containers::MAX_BODIES_REQUEST;
const MAX_BODIES_REQUEST_BYTES: usize = 4 + MAX_BODIES_REQUEST * 32;
const MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
const PROBLEM_JSON: &str = "application/problem+json";

type EthEngineApi<Provider, Pool, Validator, ChainSpec> =
    EngineApi<Provider, EthEngineTypes, Pool, Validator, ChainSpec>;
type SharedEthEngineApi<Provider, Pool, Validator, ChainSpec> =
    Arc<RwLock<Option<EthEngineApi<Provider, Pool, Validator, ChainSpec>>>>;

/// Shared handle used by [`EngineSszProxyLayer`].
pub struct EngineSszProxyHandle<ChainSpec, Provider = (), Pool = (), Validator = ()> {
    engine_api: SharedEthEngineApi<Provider, Pool, Validator, ChainSpec>,
}

impl<C, Provider, Pool, Validator> Clone for EngineSszProxyHandle<C, Provider, Pool, Validator> {
    fn clone(&self) -> Self {
        Self { engine_api: self.engine_api.clone() }
    }
}

impl<C, Provider, Pool, Validator> std::fmt::Debug
    for EngineSszProxyHandle<C, Provider, Pool, Validator>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineSszProxyHandle").finish_non_exhaustive()
    }
}

impl<ChainSpec, Provider, Pool, Validator>
    EngineSszProxyHandle<ChainSpec, Provider, Pool, Validator>
{
    fn new() -> Self {
        Self { engine_api: Default::default() }
    }

    fn with_engine_api(engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>) -> Self {
        Self { engine_api: Arc::new(RwLock::new(Some(engine_api))) }
    }

    /// Sets the Engine API implementation used by the proxy.
    pub async fn set_engine_api(
        &self,
        engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    ) {
        *self.engine_api.write().await = Some(engine_api);
    }

    /// Sets the Engine API implementation during synchronous launch wiring.
    pub fn set_engine_api_sync(
        &self,
        engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    ) {
        *self
            .engine_api
            .try_write()
            .expect("engine api handle should not be locked during launch") = Some(engine_api);
    }
}

impl<ChainSpec, Provider, Pool, Validator>
    EngineSszProxyHandle<ChainSpec, Provider, Pool, Validator>
{
    /// Returns the Engine API implementation used by the proxy.
    pub async fn engine_api(&self) -> Option<EthEngineApi<Provider, Pool, Validator, ChainSpec>> {
        self.engine_api.read().await.clone()
    }
}

/// A tower layer that intercepts SSZ Engine API routes under `/engine/v1`.
#[derive(Clone, Debug)]
pub struct EngineSszProxyLayer<ChainSpec, Provider = (), Pool = (), Validator = ()> {
    handle: EngineSszProxyHandle<ChainSpec, Provider, Pool, Validator>,
}

impl<ChainSpec, Provider, Pool, Validator>
    EngineSszProxyLayer<ChainSpec, Provider, Pool, Validator>
{
    /// Creates a new proxy layer and a handle for setting the engine after node launch.
    pub fn new() -> (Self, EngineSszProxyHandle<ChainSpec, Provider, Pool, Validator>) {
        let handle = EngineSszProxyHandle::new();
        (Self { handle: handle.clone() }, handle)
    }

    /// Creates a new proxy layer with an Engine API implementation.
    pub fn with_engine_api(
        engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    ) -> (Self, EngineSszProxyHandle<ChainSpec, Provider, Pool, Validator>) {
        let handle = EngineSszProxyHandle::with_engine_api(engine_api);
        (Self { handle: handle.clone() }, handle)
    }
}

impl<S, ChainSpec, Provider, Pool, Validator> Layer<S>
    for EngineSszProxyLayer<ChainSpec, Provider, Pool, Validator>
{
    type Service = EngineSszProxyService<S, ChainSpec, Provider, Pool, Validator>;

    fn layer(&self, inner: S) -> Self::Service {
        EngineSszProxyService { inner, handle: self.handle.clone() }
    }
}

/// The service produced by [`EngineSszProxyLayer`].
#[derive(Clone, Debug)]
pub struct EngineSszProxyService<S, ChainSpec, Provider = (), Pool = (), Validator = ()> {
    inner: S,
    handle: EngineSszProxyHandle<ChainSpec, Provider, Pool, Validator>,
}

impl<S, ChainSpec, Provider, Pool, Validator> Service<HttpRequest>
    for EngineSszProxyService<S, ChainSpec, Provider, Pool, Validator>
where
    S: Service<HttpRequest, Response = HttpResponse, Error = BoxError> + Send + Clone,
    S::Future: Send + 'static,
    Provider: HeaderProvider + BlockReader + StateProviderFactory + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    type Response = HttpResponse;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: HttpRequest) -> Self::Future {
        if !request.uri().path().starts_with("/engine/") {
            let fut = self.inner.call(request);
            return Box::pin(fut)
        }

        let handle = self.handle.clone();
        Box::pin(async move { Ok(handle_engine_ssz_request(handle, request).await) })
    }
}

async fn handle_engine_ssz_request<ChainSpec, Provider, Pool, Validator>(
    handle: EngineSszProxyHandle<ChainSpec, Provider, Pool, Validator>,
    request: HttpRequest,
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    let legacy = request.uri().path().starts_with("/engine/v2/");
    let error = |status, kind, message| {
        if legacy {
            text_response(status, message)
        } else {
            problem_response(status, kind, None)
        }
    };
    let Some(endpoint) = parse_engine_path(request.uri().path()) else {
        return error(STATUS_NOT_FOUND, "method-not-found", "unknown engine ssz endpoint")
    };
    if request.method() != endpoint.method() {
        return error(STATUS_METHOD_NOT_ALLOWED, "method-not-allowed", "method not allowed")
    }
    match endpoint {
        EngineSszEndpoint::Capabilities => {
            let api = handle.engine_api().await;
            if api.is_none() && !legacy {
                return error(
                    STATUS_SERVICE_UNAVAILABLE,
                    "service-unavailable",
                    "engine api unavailable",
                )
            }
            handle_capabilities(api.is_some_and(|api| api.has_payload_body_bal_store()))
        }
        EngineSszEndpoint::Identity => {
            let Some(api) = handle.engine_api().await else {
                return error(
                    STATUS_SERVICE_UNAVAILABLE,
                    "service-unavailable",
                    "engine api unavailable",
                )
            };
            handle_identity(api)
        }
        EngineSszEndpoint::NewPayload(path_fork) | EngineSszEndpoint::Forkchoice(path_fork) => {
            let Some(fork) = request_fork(&request, path_fork) else {
                return error(STATUS_BAD_REQUEST, "unsupported-fork", "unsupported fork")
            };
            let body = match read_transport_body(request, MAX_PAYLOAD_BYTES, legacy).await {
                Ok(body) => body,
                Err(response) => return response,
            };
            let Some(api) = handle.engine_api().await else {
                return error(
                    STATUS_SERVICE_UNAVAILABLE,
                    "service-unavailable",
                    "engine api unavailable",
                )
            };
            match (endpoint, legacy) {
                (EngineSszEndpoint::NewPayload(_), true) => {
                    handle_legacy_new_payload(api, fork.payloads_version(), &body).await
                }
                (EngineSszEndpoint::NewPayload(_), false) => {
                    handle_new_payload(api, fork, &body).await
                }
                (_, true) => {
                    handle_legacy_forkchoice_updated(api, fork.forkchoice_version(), &body).await
                }
                (_, false) => handle_forkchoice_updated(api, fork, &body).await,
            }
        }
        EngineSszEndpoint::GetPayload(payload_id) => {
            let Ok(payload_id) = payload_id else {
                return error(STATUS_BAD_REQUEST, "invalid-request", "invalid payload id")
            };
            let Some(fork) = request_fork(&request, None) else {
                return error(STATUS_BAD_REQUEST, "unsupported-fork", "unsupported fork")
            };
            let Some(api) = handle.engine_api().await else {
                return error(
                    STATUS_SERVICE_UNAVAILABLE,
                    "service-unavailable",
                    "engine api unavailable",
                )
            };
            handle_get_payload(api, fork, payload_id).await
        }
        EngineSszEndpoint::PayloadBodiesByHash => {
            let Some(fork) = request_fork(&request, None) else {
                return error(STATUS_BAD_REQUEST, "unsupported-fork", "unsupported fork")
            };
            let body = match read_bodies_hash_body(request).await {
                Ok(body) => body,
                Err(response) => return response,
            };
            let request = match BodiesByHashRequest::from_ssz_bytes(&body) {
                Ok(request) if request.block_hashes.len() <= MAX_BODIES_REQUEST => {
                    PayloadBodiesRequest::Hash(request.block_hashes)
                }
                Ok(_) => {
                    return problem_response(STATUS_PAYLOAD_TOO_LARGE, "request-too-large", None)
                }
                Err(_) => return problem_response(STATUS_BAD_REQUEST, "ssz-decode-error", None),
            };
            let Some(api) = handle.engine_api().await else {
                return error(
                    STATUS_SERVICE_UNAVAILABLE,
                    "service-unavailable",
                    "engine api unavailable",
                )
            };
            handle_get_payload_bodies(api, fork, request).await
        }
        EngineSszEndpoint::PayloadBodiesByRange => {
            let Some(fork) = request_fork(&request, None) else {
                return error(STATUS_BAD_REQUEST, "unsupported-fork", "unsupported fork")
            };
            let (start, count) =
                match parse_bodies_range_query(request.uri().query().unwrap_or_default()) {
                    Ok(range) => range,
                    Err(response) => return response,
                };
            let Some(api) = handle.engine_api().await else {
                return error(
                    STATUS_SERVICE_UNAVAILABLE,
                    "service-unavailable",
                    "engine api unavailable",
                )
            };
            handle_get_payload_bodies(api, fork, PayloadBodiesRequest::Range { start, count }).await
        }
        EngineSszEndpoint::Blobs(version) => {
            let body = match read_transport_body(request, MAX_BLOB_REQUEST_BYTES, legacy).await {
                Ok(body) => body,
                Err(response) => return response,
            };
            let Some(api) = handle.engine_api().await else {
                return error(
                    STATUS_SERVICE_UNAVAILABLE,
                    "service-unavailable",
                    "engine api unavailable",
                )
            };
            if legacy {
                handle_legacy_get_blobs(api, version, &body).await
            } else {
                handle_get_blobs(api, version, &body).await
            }
        }
    }
}

// Legacy fork-scoped aliases remain usable without a header. A header must agree with the
// path, and multiple header values are ambiguous even when an intermediary combines them.
fn request_fork(request: &HttpRequest, path_fork: Option<EngineSszFork>) -> Option<EngineSszFork> {
    let mut versions = request.headers().get_all(ETH_EXECUTION_VERSION).iter();
    let Some(version) = versions.next() else { return path_fork };
    if versions.next().is_some() {
        return None
    }
    let fork = version.to_str().ok()?.parse().ok()?;
    if path_fork.is_some_and(|path| path != fork) {
        return None
    }
    Some(fork)
}

fn parse_engine_path(path: &str) -> Option<EngineSszEndpoint> {
    let segments: Vec<_> = path.trim_start_matches('/').split('/').collect();
    match segments.as_slice() {
        ["engine", "v1" | "v2", "capabilities"] => Some(EngineSszEndpoint::Capabilities),
        ["engine", "v1" | "v2", "identity"] => Some(EngineSszEndpoint::Identity),
        ["engine", "v1", "payloads"] => Some(EngineSszEndpoint::NewPayload(None)),
        ["engine", "v1", "payloads", payload_id] => {
            Some(EngineSszEndpoint::GetPayload(payload_id.parse::<B64>().map(PayloadId::from)))
        }
        ["engine", "v1", "bodies", "hash"] => Some(EngineSszEndpoint::PayloadBodiesByHash),
        ["engine", "v1", "bodies"] => Some(EngineSszEndpoint::PayloadBodiesByRange),
        ["engine", "v1", "forkchoice"] => Some(EngineSszEndpoint::Forkchoice(None)),
        ["engine", "v2", fork, "payloads"] => {
            Some(EngineSszEndpoint::NewPayload(Some(fork.parse().ok()?)))
        }
        ["engine", "v2", fork, "forkchoice"] => {
            Some(EngineSszEndpoint::Forkchoice(Some(fork.parse().ok()?)))
        }
        ["engine", "v1" | "v2", "blobs", version] => {
            Some(EngineSszEndpoint::Blobs(parse_method_version(version)?))
        }
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EngineSszEndpoint {
    Capabilities,
    Identity,
    NewPayload(Option<EngineSszFork>),
    GetPayload(Result<PayloadId, <B64 as std::str::FromStr>::Err>),
    Forkchoice(Option<EngineSszFork>),
    PayloadBodiesByHash,
    PayloadBodiesByRange,
    Blobs(u8),
}

impl EngineSszEndpoint {
    const fn method(&self) -> &'static str {
        match self {
            Self::Capabilities |
            Self::Identity |
            Self::GetPayload(_) |
            Self::PayloadBodiesByRange => "GET",
            Self::NewPayload(_) |
            Self::Forkchoice(_) |
            Self::PayloadBodiesByHash |
            Self::Blobs(_) => "POST",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EngineSszFork {
    Paris,
    Shanghai,
    Cancun,
    Prague,
    Osaka,
    Amsterdam,
}

impl EngineSszFork {
    const fn payloads_version(self) -> u8 {
        match self {
            Self::Paris => 1,
            Self::Shanghai => 2,
            Self::Cancun => 3,
            Self::Prague | Self::Osaka => 4,
            Self::Amsterdam => 5,
        }
    }

    const fn forkchoice_version(self) -> u8 {
        match self {
            Self::Paris => 1,
            Self::Shanghai => 2,
            Self::Cancun | Self::Prague | Self::Osaka => 3,
            Self::Amsterdam => 4,
        }
    }
}

impl std::str::FromStr for EngineSszFork {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "paris" => Ok(Self::Paris),
            "shanghai" => Ok(Self::Shanghai),
            "cancun" => Ok(Self::Cancun),
            "prague" => Ok(Self::Prague),
            "osaka" => Ok(Self::Osaka),
            "amsterdam" => Ok(Self::Amsterdam),
            _ => Err(()),
        }
    }
}

fn parse_method_version(version: &str) -> Option<u8> {
    version.strip_prefix('v')?.parse().ok().filter(|version| (1..=4).contains(version))
}

fn handle_capabilities(amsterdam_bodies: bool) -> HttpResponse {
    let mut body_forks = vec!["paris", "shanghai", "cancun", "prague", "osaka"];
    if amsterdam_bodies {
        body_forks.push("amsterdam");
    }
    json_response(serde_json::json!({
        "supported_forks": ["paris", "shanghai", "cancun", "prague", "osaka", "amsterdam"],
        "fork_scoped_endpoints": ["payloads", "forkchoice", "bodies"],
        "endpoint_forks": { "bodies": body_forks },
        "independently_versioned": {
            "blobs": ["v1", "v2", "v3", "v4"],
        },
        "unscoped_endpoints": ["capabilities", "identity"],
        "limits": {
            "bodies.max_count": MAX_BODIES_REQUEST,
            "blobs.max_versioned_hashes": MAX_BLOB_LIMIT,
            "payload.max_bytes": MAX_PAYLOAD_BYTES,
        },
    }))
}

fn handle_identity<Provider, Pool, Validator, ChainSpec>(
    engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    json_response(vec![engine_api.client_version().clone()])
}

async fn handle_get_payload<Provider, Pool, Validator, ChainSpec>(
    engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    fork: EngineSszFork,
    payload_id: PayloadId,
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    match fork {
        EngineSszFork::Paris => match engine_api.get_payload_v2_metered(payload_id).await {
            Ok(payload) => {
                let block_value = payload.block_value;
                match payload.execution_payload {
                    ExecutionPayloadFieldV2::V1(payload) => {
                        get_payload_response(BuiltPayloadParis { payload, block_value })
                    }
                    ExecutionPayloadFieldV2::V2(_) => {
                        problem_response(STATUS_BAD_REQUEST, "unsupported-fork", None)
                    }
                }
            }
            Err(err) => engine_error_response(err),
        },
        EngineSszFork::Shanghai => match engine_api.get_payload_v2_metered(payload_id).await {
            Ok(payload) => match BuiltPayloadShanghai::try_from(payload) {
                Ok(payload) => get_payload_response(payload),
                Err(err) => problem_response(
                    STATUS_UNPROCESSABLE_ENTITY,
                    "invalid-body",
                    Some(err.to_string()),
                ),
            },
            Err(err) => engine_error_response(err),
        },
        EngineSszFork::Cancun => engine_api
            .get_payload_v3_metered(payload_id)
            .await
            .map_or_else(engine_error_response, get_payload_response),
        EngineSszFork::Prague => engine_api
            .get_payload_v4_metered(payload_id)
            .await
            .map(BuiltPayloadPrague::from)
            .map_or_else(engine_error_response, get_payload_response),
        EngineSszFork::Osaka => engine_api
            .get_payload_v5_metered(payload_id)
            .await
            .map(BuiltPayloadOsaka::from)
            .map_or_else(engine_error_response, get_payload_response),
        EngineSszFork::Amsterdam => engine_api
            .get_payload_v6_metered(payload_id)
            .await
            .map(BuiltPayloadAmsterdam::from)
            .map_or_else(engine_error_response, get_payload_response),
    }
}

async fn handle_new_payload<Provider, Pool, Validator, ChainSpec>(
    engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    fork: EngineSszFork,
    body: &[u8],
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    let payload = match decode_new_payload_request(fork, body) {
        Ok(payload) => payload,
        Err(err) => return err.into_response(),
    };

    let response = match fork.payloads_version() {
        1 => engine_api.new_payload_v1(payload).await,
        2 => engine_api.new_payload_v2(payload).await,
        3 => engine_api.new_payload_v3(payload).await,
        4 => engine_api.new_payload_v4(payload).await,
        5 => engine_api.new_payload_v5(payload).await,
        _ => return problem_response(STATUS_BAD_REQUEST, "unsupported-fork", None),
    };

    match response {
        Ok(mut status) => {
            if let alloy_rpc_types_engine::PayloadStatusEnum::Invalid { validation_error } =
                &mut status.status
            {
                validation_error.truncate(
                    validation_error
                        .floor_char_boundary(crate::engine_ssz_containers::MAX_ERROR_BYTES),
                );
            }
            match EngineSszPayloadStatus::try_from(status) {
                Ok(status) => ssz_response(status),
                Err(err) => problem_response(
                    STATUS_INTERNAL_SERVER_ERROR,
                    "internal",
                    Some(err.to_string()),
                ),
            }
        }
        Err(err) => engine_error_response(err),
    }
}

async fn handle_forkchoice_updated<Provider, Pool, Validator, ChainSpec>(
    engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    fork: EngineSszFork,
    body: &[u8],
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    let (state, attrs, custody_columns) = match decode_forkchoice_request(fork, body) {
        Ok(request) => request,
        Err(_) => return problem_response(STATUS_BAD_REQUEST, "ssz-decode-error", None),
    };

    if attrs.as_ref().is_some_and(|attrs| {
        !body_matches_fork(engine_api.chain_spec().as_ref(), fork, attrs.timestamp)
    }) {
        return problem_response(STATUS_BAD_REQUEST, "unsupported-fork", None)
    }

    let response = match fork.forkchoice_version() {
        1 => engine_api.fork_choice_updated_v1_metered(state, attrs).await,
        2 => engine_api.fork_choice_updated_v2_metered(state, attrs).await,
        3 => engine_api.fork_choice_updated_v3_metered(state, attrs).await,
        4 => engine_api.fork_choice_updated_v4_metered(state, attrs, custody_columns).await,
        _ => return problem_response(STATUS_BAD_REQUEST, "unsupported-fork", None),
    };

    match response {
        Ok(updated) => match ForkchoiceUpdateResponse::try_from(updated) {
            Ok(updated) => ssz_response(updated),
            Err(err) => {
                problem_response(STATUS_INTERNAL_SERVER_ERROR, "internal", Some(err.to_string()))
            }
        },
        Err(err) => engine_error_response(err),
    }
}

enum PayloadBodiesRequest {
    Hash(Vec<B256>),
    Range { start: u64, count: u64 },
}

async fn handle_get_payload_bodies<Provider, Pool, Validator, ChainSpec>(
    engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    fork: EngineSszFork,
    request: PayloadBodiesRequest,
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    let include_bal = fork == EngineSszFork::Amsterdam;
    if include_bal && !engine_api.has_payload_body_bal_store() {
        return problem_response(
            STATUS_BAD_REQUEST,
            "unsupported-fork",
            Some("Amsterdam bodies require an explicitly configured BAL store".into()),
        )
    }
    let response = match request {
        PayloadBodiesRequest::Hash(hashes) => {
            engine_api.get_payload_bodies_by_hash_with_timestamps_metered(hashes, include_bal).await
        }
        PayloadBodiesRequest::Range { start, count } => {
            engine_api
                .get_payload_bodies_by_range_with_timestamps_metered(start, count, include_bal)
                .await
        }
    };
    let chain_spec = engine_api.chain_spec().as_ref();
    match fork {
        EngineSszFork::Amsterdam => payload_bodies_http_response(
            response,
            |body| ExecutionPayloadBodyAmsterdam::try_from(body).ok(),
            fork,
            chain_spec,
        ),
        EngineSszFork::Paris => payload_bodies_http_response(
            response,
            |body| ExecutionPayloadBodyParis::try_from(ExecutionPayloadBodyV1::from(body)).ok(),
            fork,
            chain_spec,
        ),
        _ => payload_bodies_http_response(
            response,
            |body| ExecutionPayloadBodyShanghai::try_from(ExecutionPayloadBodyV1::from(body)).ok(),
            fork,
            chain_spec,
        ),
    }
}

fn parse_bodies_range_query(query: &str) -> Result<(u64, u64), HttpResponse> {
    let invalid = || problem_response(STATUS_BAD_REQUEST, "invalid-request", None);
    let mut start = None;
    let mut count = None;
    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').ok_or_else(invalid)?;
        let field = match key {
            "from" if start.is_none() => &mut start,
            "count" if count.is_none() => &mut count,
            _ => return Err(invalid()),
        };
        *field = Some(value.parse::<u64>().map_err(|_| invalid())?);
    }
    let range = (start.ok_or_else(invalid)?, count.ok_or_else(invalid)?);
    if range.1 > MAX_BODIES_REQUEST as u64 {
        return Err(problem_response(STATUS_PAYLOAD_TOO_LARGE, "request-too-large", None))
    }
    Ok(range)
}

/// Amsterdam bodies with a missing or pruned BAL are unavailable, just like pruned blocks.
fn payload_bodies_response<LegacyBody, ForkBody>(
    response: Result<Vec<Option<(u64, LegacyBody)>>, EngineApiError>,
    convert: impl Fn(LegacyBody) -> Option<ForkBody>,
    fork: EngineSszFork,
    chain_spec: &impl EthereumHardforks,
) -> Result<BodiesResponse<ForkBody>, EngineApiError>
where
    ForkBody: Default + ssz::Encode + ssz::Decode,
{
    let bodies = response?;
    let bodies = bodies
        .into_iter()
        .map(|body| {
            body.filter(|(timestamp, _)| body_matches_fork(chain_spec, fork, *timestamp))
                .map(|(_, body)| body)
        })
        .collect();
    Ok(BodiesResponse::from_optional_bodies(bodies, convert))
}

fn payload_bodies_http_response<LegacyBody, ForkBody>(
    response: Result<Vec<Option<(u64, LegacyBody)>>, EngineApiError>,
    convert: impl Fn(LegacyBody) -> Option<ForkBody>,
    fork: EngineSszFork,
    chain_spec: &impl EthereumHardforks,
) -> HttpResponse
where
    ForkBody: Default + ssz::Encode + ssz::Decode,
{
    match payload_bodies_response(response, convert, fork, chain_spec) {
        Ok(response) => ssz_response(response),
        Err(err) => payload_bodies_error_response(err),
    }
}

fn body_matches_fork<ChainSpec: EthereumHardforks>(
    chain_spec: &ChainSpec,
    fork: EngineSszFork,
    timestamp: u64,
) -> bool {
    let active = |fork| chain_spec.is_ethereum_fork_active_at_timestamp(fork, timestamp);
    match fork {
        EngineSszFork::Paris => !active(EthereumHardfork::Shanghai),
        EngineSszFork::Shanghai => {
            active(EthereumHardfork::Shanghai) && !active(EthereumHardfork::Cancun)
        }
        EngineSszFork::Cancun => {
            active(EthereumHardfork::Cancun) && !active(EthereumHardfork::Prague)
        }
        EngineSszFork::Prague => {
            active(EthereumHardfork::Prague) && !active(EthereumHardfork::Osaka)
        }
        EngineSszFork::Osaka => {
            active(EthereumHardfork::Osaka) && !active(EthereumHardfork::Amsterdam)
        }
        EngineSszFork::Amsterdam => active(EthereumHardfork::Amsterdam),
    }
}

fn payload_bodies_error_response(err: EngineApiError) -> HttpResponse {
    engine_error_response(err)
}

async fn read_bodies_hash_body(request: HttpRequest) -> Result<Vec<u8>, HttpResponse> {
    read_ssz_body(request, MAX_BODIES_REQUEST_BYTES).await.map(|body| body.to_vec())
}

/// Handles SSZ `engine_getBlobsV*` requests with the node's blob store.
async fn handle_legacy_get_blobs<ChainSpec, Provider, Pool, Validator>(
    engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    version: u8,
    body: &[u8],
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    match version {
        1 => {
            let hashes = match decode_blob_hashes_request(body) {
                Ok(hashes) => hashes,
                Err(err) => return text_response(STATUS_BAD_REQUEST, err),
            };
            match engine_api.get_blobs_v1_metered(hashes) {
                Ok(response) => ssz_response(response),
                Err(err) => legacy_blob_error_response(err),
            }
        }
        2 => {
            let hashes = match decode_blob_hashes_request(body) {
                Ok(hashes) => hashes,
                Err(err) => return text_response(STATUS_BAD_REQUEST, err),
            };
            match engine_api.get_blobs_v2_metered(hashes) {
                Ok(Some(response)) => ssz_response(response),
                Ok(None) => no_content_response(),
                Err(err) => legacy_blob_error_response(err),
            }
        }
        3 => {
            let hashes = match decode_blob_hashes_request(body) {
                Ok(hashes) => hashes,
                Err(err) => return text_response(STATUS_BAD_REQUEST, err),
            };
            match engine_api.get_blobs_v3_metered(hashes) {
                Ok(Some(response)) => ssz_response(response),
                Ok(None) => no_content_response(),
                Err(err) => legacy_blob_error_response(err),
            }
        }
        4 => {
            let (hashes, indices_bitarray) = match decode_blob_cells_request(body) {
                Ok(request) => request,
                Err(err) => return text_response(STATUS_BAD_REQUEST, err),
            };
            match engine_api.get_blobs_v4_metered(hashes, indices_bitarray) {
                Ok(Some(response)) => ssz_response(response),
                Ok(None) => no_content_response(),
                Err(err) => legacy_blob_error_response(err),
            }
        }
        _ => text_response(STATUS_NOT_FOUND, "unsupported blobs endpoint version"),
    }
}

fn legacy_blob_error_response(err: EngineApiError) -> HttpResponse {
    let status = match &err {
        EngineApiError::BlobRequestTooLarge { .. } => STATUS_PAYLOAD_TOO_LARGE,
        EngineApiError::EngineObjectValidationError(
            reth_payload_primitives::EngineObjectValidationError::UnsupportedFork,
        ) => STATUS_BAD_REQUEST,
        _ => STATUS_INTERNAL_SERVER_ERROR,
    };
    text_response(status, err.to_string())
}

async fn handle_get_blobs<ChainSpec, Provider, Pool, Validator>(
    engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    version: u8,
    body: &[u8],
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    if version == 4 {
        let request = match BlobsV4Request::from_ssz_bytes(body) {
            Ok(request) => request,
            Err(_) => return problem_response(STATUS_BAD_REQUEST, "ssz-decode-error", None),
        };
        if request.versioned_hashes.len() > MAX_BLOB_LIMIT {
            return problem_response(STATUS_PAYLOAD_TOO_LARGE, "request-too-large", None)
        }
        return blob_response::<BlobsV4Response, _>(
            engine_api.get_blobs_v4_metered(request.versioned_hashes, request.indices_bitarray),
        )
    }
    let request = match BlobsV1Request::from_ssz_bytes(body) {
        Ok(request) => request,
        Err(_) => return problem_response(STATUS_BAD_REQUEST, "ssz-decode-error", None),
    };
    if request.versioned_hashes.len() > MAX_BLOB_LIMIT {
        return problem_response(STATUS_PAYLOAD_TOO_LARGE, "request-too-large", None)
    }
    let hashes = request.versioned_hashes;
    match version {
        1 => blob_response::<BlobsV1Response, _>(engine_api.get_blobs_v1_metered(hashes).map(Some)),
        2 => blob_response::<BlobsV2Response, _>(engine_api.get_blobs_v2_metered(hashes)),
        3 => blob_response::<BlobsV3Response, _>(engine_api.get_blobs_v3_metered(hashes)),
        _ => problem_response(STATUS_NOT_FOUND, "method-not-found", None),
    }
}

/// Decodes the common getBlobs request container with only versioned hashes.
fn decode_blob_hashes_request(body: &[u8]) -> Result<Vec<B256>, &'static str> {
    Vec::<B256>::from_ssz_bytes(body).map_err(|_| "invalid ssz")
}

/// Decodes the Amsterdam getBlobs request container with hashes and a cell index mask.
fn decode_blob_cells_request(body: &[u8]) -> Result<(Vec<B256>, B128), &'static str> {
    <(Vec<B256>, B128) as ssz::Decode>::from_ssz_bytes(body).map_err(|_| "invalid ssz")
}

fn decode_new_payload_request(
    fork: EngineSszFork,
    body: &[u8],
) -> Result<ExecutionData, PayloadDecodeError> {
    let (payload, parent_root, requests): (ExecutionPayload, _, Option<Requests>) = match fork {
        EngineSszFork::Paris => {
            let envelope = ExecutionPayloadEnvelopeParis::from_ssz_bytes(body)?;
            (envelope.payload.into(), None, None)
        }
        EngineSszFork::Shanghai => {
            let envelope = ExecutionPayloadEnvelopeShanghai::from_ssz_bytes(body)?;
            (envelope.payload.into(), None, None)
        }
        EngineSszFork::Cancun => {
            let envelope = ExecutionPayloadEnvelopeCancun::from_ssz_bytes(body)?;
            (envelope.payload.into(), Some(envelope.parent_beacon_block_root), None)
        }
        EngineSszFork::Prague => {
            let envelope = ExecutionPayloadEnvelopePrague::from_ssz_bytes(body)?;
            (
                envelope.payload.into(),
                Some(envelope.parent_beacon_block_root),
                Some(envelope.execution_requests),
            )
        }
        EngineSszFork::Osaka => {
            let envelope = ExecutionPayloadEnvelopeOsaka::from_ssz_bytes(body)?;
            (
                envelope.payload.into(),
                Some(envelope.parent_beacon_block_root),
                Some(envelope.execution_requests),
            )
        }
        EngineSszFork::Amsterdam => {
            let envelope = ExecutionPayloadEnvelopeAmsterdam::from_ssz_bytes(body)?;
            (
                ExecutionPayload::V4(envelope.payload),
                Some(envelope.parent_beacon_block_root),
                Some(envelope.execution_requests),
            )
        }
    };

    // Alloy's codecs reproduce the wire layout but do not enforce the SSZ list bounds.
    // Check those independently of the smaller, advertised total HTTP request limit.
    let inner = payload.as_v1();
    check_ssz_bound(inner.extra_data.len(), 32, "extra_data")?;
    check_ssz_bound(inner.transactions.len(), 1 << 20, "transactions")?;
    for transaction in &inner.transactions {
        check_ssz_bound(transaction.len(), 1 << 30, "transaction")?;
    }
    if let Some(withdrawals) = payload.withdrawals() {
        check_ssz_bound(withdrawals.len(), 16, "withdrawals")?;
    }
    if let Some(bal) = payload.block_access_list() {
        check_ssz_bound(bal.len(), 1 << 30, "block_access_list")?;
    }
    if let Some(requests) = &requests {
        check_ssz_bound(requests.len(), 256, "execution_requests")?;
        for request in requests.iter() {
            check_ssz_bound(request.len(), 1 << 30, "execution_request")?;
        }
    }

    let versioned_hashes = calculate_versioned_hashes(&payload)?;
    let sidecar = match parent_root {
        Some(parent_beacon_block_root) => {
            let cancun = CancunPayloadFields { parent_beacon_block_root, versioned_hashes };
            match requests {
                Some(requests) => {
                    ExecutionPayloadSidecar::v4(cancun, PraguePayloadFields::new(requests))
                }
                None => ExecutionPayloadSidecar::v3(cancun),
            }
        }
        None => ExecutionPayloadSidecar::none(),
    };
    Ok(ExecutionData::new(payload, sidecar))
}

fn legacy_calculate_versioned_hashes(transactions: &[Bytes]) -> Result<Vec<B256>, &'static str> {
    let mut versioned_hashes = Vec::new();
    for transaction in transactions {
        let transaction =
            TxEnvelope::decode_2718_exact(transaction.as_ref()).map_err(|_| "invalid tx")?;
        if let Some(hashes) = transaction.blob_versioned_hashes() {
            versioned_hashes.extend_from_slice(hashes);
        }
    }

    Ok(versioned_hashes)
}

fn decode_forkchoice_request(
    fork: EngineSszFork,
    body: &[u8],
) -> Result<(ForkchoiceState, Option<PayloadAttributes>, Option<B128>), ssz::DecodeError> {
    let (state, attrs, custody) = match fork {
        EngineSszFork::Paris => {
            let ForkchoiceUpdateParis { forkchoice_state, payload_attributes } =
                ForkchoiceUpdateParis::from_ssz_bytes(body)?;
            (forkchoice_state, optional_attrs(payload_attributes), None)
        }
        EngineSszFork::Shanghai => {
            let ForkchoiceUpdateShanghai { forkchoice_state, payload_attributes } =
                ForkchoiceUpdateShanghai::from_ssz_bytes(body)?;
            (forkchoice_state, optional_attrs(payload_attributes), None)
        }
        EngineSszFork::Cancun => {
            let ForkchoiceUpdateCancun { forkchoice_state, payload_attributes } =
                ForkchoiceUpdateCancun::from_ssz_bytes(body)?;
            (forkchoice_state, optional_attrs(payload_attributes), None)
        }
        EngineSszFork::Prague => {
            let ForkchoiceUpdatePrague { forkchoice_state, payload_attributes } =
                ForkchoiceUpdatePrague::from_ssz_bytes(body)?;
            (forkchoice_state, optional_attrs(payload_attributes), None)
        }
        EngineSszFork::Osaka => {
            let ForkchoiceUpdateOsaka { forkchoice_state, payload_attributes } =
                ForkchoiceUpdateOsaka::from_ssz_bytes(body)?;
            (forkchoice_state, optional_attrs(payload_attributes), None)
        }
        EngineSszFork::Amsterdam => {
            let ForkchoiceUpdateAmsterdam { forkchoice_state, payload_attributes, custody_columns } =
                ForkchoiceUpdateAmsterdam::from_ssz_bytes(body)?;
            (forkchoice_state, optional_attrs(payload_attributes), custody_columns.into_option())
        }
    };
    if let Some(withdrawals) =
        attrs.as_ref().and_then(|attrs: &PayloadAttributes| attrs.withdrawals.as_ref())
    {
        check_ssz_bound(withdrawals.len(), 16, "withdrawals")?;
    }
    Ok((state, attrs, custody))
}

fn calculate_versioned_hashes(payload: &ExecutionPayload) -> Result<Vec<B256>, PayloadDecodeError> {
    let mut versioned_hashes = Vec::new();
    for transaction in payload.decoded_transactions::<TxEnvelope>() {
        let transaction = transaction.map_err(PayloadDecodeError::InvalidTransaction)?;
        if let Some(hashes) = transaction.blob_versioned_hashes() {
            versioned_hashes.extend_from_slice(hashes);
        }
    }
    Ok(versioned_hashes)
}
/// Structural SSZ errors and invalid values have distinct REST error codes.
#[derive(Debug)]
enum PayloadDecodeError {
    Ssz(ssz::DecodeError),
    InvalidTransaction(alloy_eips::eip2718::Eip2718Error),
}

impl From<ssz::DecodeError> for PayloadDecodeError {
    fn from(error: ssz::DecodeError) -> Self {
        Self::Ssz(error)
    }
}

impl PayloadDecodeError {
    fn into_response(self) -> HttpResponse {
        match self {
            Self::Ssz(error) => {
                problem_response(STATUS_BAD_REQUEST, "ssz-decode-error", Some(format!("{error:?}")))
            }
            Self::InvalidTransaction(error) => problem_response(
                STATUS_UNPROCESSABLE_ENTITY,
                "invalid-body",
                Some(error.to_string()),
            ),
        }
    }
}

fn check_ssz_bound(actual: usize, max: usize, field: &str) -> Result<(), ssz::DecodeError> {
    if actual > max {
        return Err(ssz::DecodeError::BytesInvalid(format!(
            "{field} exceeds SSZ bound: {actual} > {max}"
        )))
    }
    Ok(())
}

// Legacy aliases keep accepting their existing content types and keep plain-text errors,
// while also bounding peer-supplied bodies instead of buffering them without a limit.
async fn read_transport_body(
    request: HttpRequest,
    max_bytes: usize,
    legacy: bool,
) -> Result<Bytes, HttpResponse> {
    if !legacy {
        return read_ssz_body(request, max_bytes).await
    }
    match Limited::new(request.into_body(), max_bytes).collect().await {
        Ok(body) => Ok(body.to_bytes().into()),
        Err(error) if error.downcast_ref::<LengthLimitError>().is_some() => {
            Err(text_response(STATUS_PAYLOAD_TOO_LARGE, "request too large"))
        }
        Err(_) => Err(text_response(STATUS_BAD_REQUEST, "failed to read request body")),
    }
}

async fn read_ssz_body(request: HttpRequest, max_bytes: usize) -> Result<Bytes, HttpResponse> {
    let content_type = request.headers().get(CONTENT_TYPE).and_then(|value| value.to_str().ok());
    if content_type != Some(OCTET_STREAM) {
        return Err(problem_response(STATUS_UNSUPPORTED_MEDIA_TYPE, "unsupported-media-type", None))
    }

    if let Some(content_length) = request.headers().get("content-length") {
        let Some(content_length) =
            content_length.to_str().ok().and_then(|value| value.parse::<usize>().ok())
        else {
            return Err(problem_response(STATUS_BAD_REQUEST, "invalid-request", None))
        };
        if content_length > max_bytes {
            return Err(problem_response(STATUS_PAYLOAD_TOO_LARGE, "request-too-large", None))
        }
    }

    match Limited::new(request.into_body(), max_bytes).collect().await {
        Ok(body) => Ok(body.to_bytes().into()),
        Err(err) if err.downcast_ref::<LengthLimitError>().is_some() => {
            Err(problem_response(STATUS_PAYLOAD_TOO_LARGE, "request-too-large", None))
        }
        Err(_) => Err(problem_response(STATUS_BAD_REQUEST, "invalid-request", None)),
    }
}

fn blob_response<Ssz, Legacy>(response: Result<Option<Legacy>, EngineApiError>) -> HttpResponse
where
    Ssz: TryFrom<Legacy> + ssz::Encode,
    Ssz::Error: std::fmt::Display,
{
    match response {
        Ok(Some(response)) => match Ssz::try_from(response) {
            Ok(response) => ssz_response(response),
            Err(err) => {
                problem_response(STATUS_INTERNAL_SERVER_ERROR, "internal", Some(err.to_string()))
            }
        },
        Ok(None) => no_content_response(),
        Err(err) => engine_error_response(err),
    }
}

fn engine_error_response(err: EngineApiError) -> HttpResponse {
    let detail = err.to_string();
    let error: jsonrpsee::types::ErrorObjectOwned = err.into();
    let (status, problem_type) = match error.code() {
        -32700 => (STATUS_BAD_REQUEST, "parse-error"),
        -32600 => (STATUS_BAD_REQUEST, "invalid-request"),
        -32601 => (STATUS_NOT_FOUND, "method-not-found"),
        -32602 => (STATUS_UNPROCESSABLE_ENTITY, "invalid-body"),
        -38001 => (STATUS_NOT_FOUND, "unknown-payload"),
        -38002 => (STATUS_CONFLICT, "invalid-forkchoice"),
        -38003 => (STATUS_UNPROCESSABLE_ENTITY, "invalid-attributes"),
        -38004 => (STATUS_PAYLOAD_TOO_LARGE, "request-too-large"),
        -38005 => (STATUS_BAD_REQUEST, "unsupported-fork"),
        -38006 => (STATUS_CONFLICT, "reorg-too-deep"),
        _ => (STATUS_INTERNAL_SERVER_ERROR, "internal"),
    };
    problem_response(status, problem_type, Some(detail))
}

fn optional_attrs<T>(attrs: Optional<T>) -> Option<PayloadAttributes>
where
    T: Into<PayloadAttributes>,
{
    attrs.into_option().map(Into::into)
}

fn ssz_response<T: ssz::Encode>(value: T) -> HttpResponse {
    HttpResponse::builder()
        .status(STATUS_OK)
        .header(CONTENT_TYPE, OCTET_STREAM)
        .body(HttpBody::from(value.as_ssz_bytes()))
        .expect("valid response")
}

fn get_payload_response<T: ssz::Encode>(value: T) -> HttpResponse {
    let mut response = ssz_response(value);
    response.headers_mut().insert(CACHE_CONTROL, "no-store".parse().expect("valid cache control"));
    response
}

fn json_response<T: serde::Serialize>(value: T) -> HttpResponse {
    let Ok(body) = serde_json::to_string(&value) else {
        return text_response(STATUS_INTERNAL_SERVER_ERROR, "failed to encode json")
    };

    HttpResponse::builder()
        .status(STATUS_OK)
        .header(CONTENT_TYPE, APPLICATION_JSON)
        .body(HttpBody::from(body))
        .expect("valid response")
}

fn no_content_response() -> HttpResponse {
    HttpResponse::builder().status(204).body(HttpBody::empty()).expect("valid response")
}

fn text_response(status: u16, body: impl Into<String>) -> HttpResponse {
    HttpResponse::builder()
        .status(status)
        .header(CONTENT_TYPE, TEXT_PLAIN)
        .body(HttpBody::from(body.into()))
        .expect("valid response")
}

fn problem_response(
    status: u16,
    problem_type: &'static str,
    detail: Option<String>,
) -> HttpResponse {
    let problem_type = format!("/engine-api/errors/{problem_type}");
    let body = match detail {
        Some(detail) => serde_json::json!({ "type": problem_type, "detail": detail }),
        None => serde_json::json!({ "type": problem_type }),
    };

    HttpResponse::builder()
        .status(status)
        .header(CONTENT_TYPE, PROBLEM_JSON)
        .body(HttpBody::from(body.to_string()))
        .expect("valid response")
}

// Compatibility codecs for callers of the original fork-scoped /engine/v2 routes.
async fn handle_legacy_new_payload<Provider, Pool, Validator, ChainSpec>(
    engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    version: u8,
    body: &[u8],
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    let payload = match legacy_decode_new_payload_request(version, body) {
        Ok(payload) => payload,
        Err(err) => return text_response(STATUS_BAD_REQUEST, err),
    };

    let response = match version {
        1 => engine_api.new_payload_v1(payload).await,
        2 => engine_api.new_payload_v2(payload).await,
        3 => engine_api.new_payload_v3(payload).await,
        4 => engine_api.new_payload_v4(payload).await,
        5 => engine_api.new_payload_v5(payload).await,
        _ => return text_response(STATUS_BAD_REQUEST, "unsupported payload endpoint version"),
    };

    match response {
        Ok(status) => ssz_response(status),
        Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

async fn handle_legacy_forkchoice_updated<Provider, Pool, Validator, ChainSpec>(
    engine_api: EthEngineApi<Provider, Pool, Validator, ChainSpec>,
    version: u8,
    body: &[u8],
) -> HttpResponse
where
    Provider: HeaderProvider + BlockReader + StateProviderFactory + 'static,
    Pool: TransactionPool + 'static,
    Validator: EngineApiValidator<EthEngineTypes>,
    ChainSpec: EthereumHardforks + Send + Sync + 'static,
{
    let (state, attrs, custody_columns) = match legacy_decode_forkchoice_request(version, body) {
        Ok(request) => request,
        Err(err) => return text_response(STATUS_BAD_REQUEST, err),
    };

    let response = match version {
        1 => engine_api.fork_choice_updated_v1_metered(state, attrs).await,
        2 => engine_api.fork_choice_updated_v2_metered(state, attrs).await,
        3 => engine_api.fork_choice_updated_v3_metered(state, attrs).await,
        4 => engine_api.fork_choice_updated_v4_metered(state, attrs, custody_columns).await,
        _ => return text_response(STATUS_BAD_REQUEST, "unsupported forkchoice endpoint version"),
    };

    match response {
        Ok(updated) => ssz_response(updated),
        Err(err) => text_response(STATUS_INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

fn legacy_decode_new_payload_request(
    version: u8,
    body: &[u8],
) -> Result<ExecutionData, &'static str> {
    match version {
        1 => {
            let execution_payload =
                legacy_decode_one::<ExecutionPayloadV1>(body).map_err(|_| "invalid ssz")?;
            Ok(ExecutionData::new(execution_payload.into(), ExecutionPayloadSidecar::none()))
        }
        2 => {
            let execution_payload =
                legacy_decode_one::<ExecutionPayloadV2>(body).map_err(|_| "invalid ssz")?;
            Ok(ExecutionData::new(execution_payload.into(), ExecutionPayloadSidecar::none()))
        }
        3 => {
            let (execution_payload, parent_beacon_block_root) =
                <(ExecutionPayloadV3, B256)>::from_ssz_bytes(body).map_err(|_| "invalid ssz")?;
            let versioned_hashes = legacy_calculate_versioned_hashes(
                &execution_payload.payload_inner.payload_inner.transactions,
            )?;
            let sidecar = ExecutionPayloadSidecar::v3(CancunPayloadFields {
                parent_beacon_block_root,
                versioned_hashes,
            });
            Ok(ExecutionData::new(execution_payload.into(), sidecar))
        }
        4 => {
            let (execution_payload, parent_beacon_block_root, execution_requests) =
                <(ExecutionPayloadV3, B256, Vec<Bytes>)>::from_ssz_bytes(body)
                    .map_err(|_| "invalid ssz")?;
            let versioned_hashes = legacy_calculate_versioned_hashes(
                &execution_payload.payload_inner.payload_inner.transactions,
            )?;
            let sidecar = ExecutionPayloadSidecar::v4(
                CancunPayloadFields { parent_beacon_block_root, versioned_hashes },
                PraguePayloadFields::new(RequestsOrHash::Requests(Requests::new(
                    execution_requests,
                ))),
            );
            Ok(ExecutionData::new(execution_payload.into(), sidecar))
        }
        5 => {
            let (execution_payload, parent_beacon_block_root, execution_requests) =
                <(ExecutionPayloadV4, B256, Vec<Bytes>)>::from_ssz_bytes(body)
                    .map_err(|_| "invalid ssz")?;
            let versioned_hashes = legacy_calculate_versioned_hashes(
                &execution_payload.payload_inner.payload_inner.payload_inner.transactions,
            )?;
            let sidecar = ExecutionPayloadSidecar::v4(
                CancunPayloadFields { parent_beacon_block_root, versioned_hashes },
                PraguePayloadFields::new(RequestsOrHash::Requests(Requests::new(
                    execution_requests,
                ))),
            );
            Ok(ExecutionData::new(ExecutionPayload::V4(execution_payload), sidecar))
        }
        _ => Err("unsupported payload endpoint version"),
    }
}

fn legacy_decode_forkchoice_request(
    version: u8,
    body: &[u8],
) -> Result<(ForkchoiceState, Option<PayloadAttributes>, Option<B128>), &'static str> {
    match version {
        1..=3 => {
            let (forkchoice_state, payload_attributes) =
                <(ForkchoiceState, Vec<PayloadAttributes>)>::from_ssz_bytes(body)
                    .map_err(|_| "invalid ssz")?;
            Ok((forkchoice_state, legacy_payload_attrs(version, payload_attributes)?, None))
        }
        4 => {
            let (forkchoice_state, payload_attributes, custody_columns) =
                <(ForkchoiceState, Vec<PayloadAttributes>, Vec<B128>)>::from_ssz_bytes(body)
                    .map_err(|_| "invalid ssz")?;
            Ok((
                forkchoice_state,
                legacy_payload_attrs(version, payload_attributes)?,
                legacy_custody_columns_opt(custody_columns)?,
            ))
        }
        _ => Err("unsupported forkchoice endpoint version"),
    }
}

fn legacy_decode_one<T: ssz::Decode>(body: &[u8]) -> Result<T, ssz::DecodeError> {
    let mut builder = ssz::SszDecoderBuilder::new(body);
    builder.register_type::<T>()?;
    let mut decoder = builder.build()?;
    decoder.decode_next()
}

fn legacy_payload_attrs(
    version: u8,
    attrs: Vec<PayloadAttributes>,
) -> Result<Option<PayloadAttributes>, &'static str> {
    if attrs.len() > 1 {
        return Err("payload_attributes must contain at most one value")
    }

    attrs
        .into_iter()
        .next()
        .map(|attrs| legacy_validate_payload_attrs_version(version, attrs))
        .transpose()
}

fn legacy_custody_columns_opt(custody_columns: Vec<B128>) -> Result<Option<B128>, &'static str> {
    if custody_columns.len() > 1 {
        return Err("invalid params")
    }

    Ok(custody_columns.into_iter().next())
}

fn legacy_validate_payload_attrs_version(
    version: u8,
    attrs: PayloadAttributes,
) -> Result<PayloadAttributes, &'static str> {
    let matches_version = match version {
        1 => {
            attrs.withdrawals.is_none() &&
                attrs.parent_beacon_block_root.is_none() &&
                attrs.slot_number.is_none()
        }
        2 => {
            attrs.withdrawals.is_some() &&
                attrs.parent_beacon_block_root.is_none() &&
                attrs.slot_number.is_none()
        }
        3 => {
            attrs.withdrawals.is_some() &&
                attrs.parent_beacon_block_root.is_some() &&
                attrs.slot_number.is_none()
        }
        4 => {
            attrs.withdrawals.is_some() &&
                attrs.parent_beacon_block_root.is_some() &&
                attrs.slot_number.is_some()
        }
        _ => false,
    };

    if matches_version {
        Ok(attrs)
    } else {
        Err("payload_attributes version does not match endpoint")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ssz::Encode;

    #[test]
    fn payload_schema_bounds() {
        use alloy_rpc_types_engine::{ExecutionPayloadV1, ExecutionPayloadV2, ExecutionPayloadV3};
        let mut payload = ExecutionPayloadV3 {
            payload_inner: ExecutionPayloadV2 {
                payload_inner: ExecutionPayloadV1::from_block_unchecked(
                    B256::ZERO,
                    &reth_ethereum_primitives::Block::default(),
                ),
                withdrawals: vec![],
            },
            blob_gas_used: 0,
            excess_blob_gas: 0,
        };
        payload.payload_inner.payload_inner.extra_data = vec![0; 32].into();
        payload.payload_inner.withdrawals = vec![Default::default(); 16];
        let mut envelope = ExecutionPayloadEnvelopePrague {
            payload,
            parent_beacon_block_root: B256::ZERO,
            execution_requests: Requests::new(vec![Bytes::new(); 256]),
        };
        assert!(decode_new_payload_request(EngineSszFork::Prague, &envelope.as_ssz_bytes()).is_ok());
        envelope.execution_requests = Requests::new(vec![Bytes::new(); 257]);
        assert!(matches!(
            decode_new_payload_request(EngineSszFork::Prague, &envelope.as_ssz_bytes()),
            Err(PayloadDecodeError::Ssz(_))
        ));
        envelope.execution_requests = Requests::default();
        envelope.payload.payload_inner.withdrawals.push(Default::default());
        assert!(matches!(
            decode_new_payload_request(EngineSszFork::Prague, &envelope.as_ssz_bytes()),
            Err(PayloadDecodeError::Ssz(_))
        ));
        envelope.payload.payload_inner.withdrawals.clear();
        envelope.payload.payload_inner.payload_inner.extra_data = vec![0; 33].into();
        assert!(matches!(
            decode_new_payload_request(EngineSszFork::Prague, &envelope.as_ssz_bytes()),
            Err(PayloadDecodeError::Ssz(_))
        ));
        let shanghai = ExecutionPayloadEnvelopeShanghai {
            payload: ExecutionPayloadV2 {
                payload_inner: ExecutionPayloadV1::from_block_unchecked(
                    B256::ZERO,
                    &reth_ethereum_primitives::Block::default(),
                ),
                withdrawals: vec![Default::default(); 17],
            },
        };
        assert!(matches!(
            decode_new_payload_request(EngineSszFork::Shanghai, &shanghai.as_ssz_bytes()),
            Err(PayloadDecodeError::Ssz(_))
        ));
        let paris = ExecutionPayloadEnvelopeParis {
            payload: ExecutionPayloadV1 {
                transactions: vec![Bytes::new(); (1 << 20) + 1],
                ..ExecutionPayloadV1::from_block_unchecked(
                    B256::ZERO,
                    &reth_ethereum_primitives::Block::default(),
                )
            },
        };
        assert!(matches!(
            decode_new_payload_request(EngineSszFork::Paris, &paris.as_ssz_bytes()),
            Err(PayloadDecodeError::Ssz(_))
        ));
    }

    #[test]
    fn forkchoice_withdrawal_bounds() {
        use crate::engine_ssz_containers::{
            PayloadAttributesAmsterdam, PayloadAttributesCancun, PayloadAttributesShanghai,
        };
        for count in [16, 17] {
            let withdrawals = vec![Default::default(); count];
            let state = ForkchoiceState::default();
            let shanghai = ForkchoiceUpdateShanghai {
                forkchoice_state: state,
                payload_attributes: Optional::some(PayloadAttributesShanghai {
                    withdrawals: withdrawals.clone(),
                    ..Default::default()
                }),
            };
            let cancun = ForkchoiceUpdateCancun {
                forkchoice_state: state,
                payload_attributes: Optional::some(PayloadAttributesCancun {
                    withdrawals: withdrawals.clone(),
                    ..Default::default()
                }),
            };
            let amsterdam = ForkchoiceUpdateAmsterdam {
                forkchoice_state: state,
                payload_attributes: Optional::some(PayloadAttributesAmsterdam {
                    withdrawals,
                    ..Default::default()
                }),
                custody_columns: Optional::none(),
            };
            for (fork, bytes) in [
                (EngineSszFork::Shanghai, shanghai.as_ssz_bytes()),
                (EngineSszFork::Cancun, cancun.as_ssz_bytes()),
                (EngineSszFork::Prague, cancun.as_ssz_bytes()),
                (EngineSszFork::Osaka, cancun.as_ssz_bytes()),
                (EngineSszFork::Amsterdam, amsterdam.as_ssz_bytes()),
            ] {
                assert_eq!(
                    decode_forkchoice_request(fork, &bytes).is_ok(),
                    count == 16,
                    "{fork:?}"
                );
            }
        }
    }

    #[test]
    fn paris_forkchoice_uses_fixed_size_optional_attributes() {
        use crate::engine_ssz_containers::PayloadAttributesParis;
        let request = ForkchoiceUpdateParis {
            forkchoice_state: ForkchoiceState::default(),
            payload_attributes: Optional::some(PayloadAttributesParis {
                timestamp: 1_700_000_000,
                ..Default::default()
            }),
        };
        let bytes = request.as_ssz_bytes();
        assert_eq!(bytes.len(), 160);
        assert_eq!(&bytes[96..100], &100u32.to_le_bytes());
        assert_eq!(&bytes[100..108], &1_700_000_000u64.to_le_bytes());
        assert_eq!(
            decode_forkchoice_request(EngineSszFork::Paris, &bytes).unwrap().1.unwrap().timestamp,
            1_700_000_000
        );
    }

    #[tokio::test]
    async fn post_body_limits_apply_without_content_length() {
        for size in [16, 17] {
            let request = HttpRequest::builder()
                .header(CONTENT_TYPE, OCTET_STREAM)
                .body(HttpBody::from(vec![0; size]))
                .unwrap();
            let response = read_ssz_body(request, 16).await;
            if size == 16 {
                assert_eq!(response.unwrap().len(), size);
            } else {
                assert_eq!(response.unwrap_err().status(), STATUS_PAYLOAD_TOO_LARGE);
            }
        }
    }

    #[tokio::test]
    async fn engine_errors_preserve_validation_semantics() {
        use alloy_rpc_types_engine::ForkchoiceUpdateError;
        use reth_engine_primitives::BeaconForkChoiceUpdateError;
        for (error, status, kind) in [
            (EngineApiError::UnknownPayload, 404, "unknown-payload"),
            (EngineApiError::BlobRequestTooLarge { len: 129 }, 413, "request-too-large"),
            (
                EngineApiError::ForkChoiceUpdate(
                    BeaconForkChoiceUpdateError::ForkchoiceUpdateError(
                        ForkchoiceUpdateError::InvalidState,
                    ),
                ),
                409,
                "invalid-forkchoice",
            ),
            (
                EngineApiError::ForkChoiceUpdate(
                    BeaconForkChoiceUpdateError::ForkchoiceUpdateError(
                        ForkchoiceUpdateError::UpdatedInvalidPayloadAttributes,
                    ),
                ),
                422,
                "invalid-attributes",
            ),
        ] {
            let response = engine_error_response(error);
            assert_eq!(response.status(), status);
            assert_eq!(response.headers()[CONTENT_TYPE], PROBLEM_JSON);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let problem: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(problem["type"], format!("/engine-api/errors/{kind}"));
        }
    }

    #[test]
    fn bodies_range_query_validation() {
        assert_eq!(parse_bodies_range_query("from=1&count=32").unwrap(), (1, 32));
        assert_eq!(parse_bodies_range_query("count=2&from=3").unwrap(), (3, 2));
        for query in
            ["", "from=1", "from=x&count=1", "from=1&count=1&count=2", "from=1&count=1&other=0"]
        {
            assert_eq!(parse_bodies_range_query(query).unwrap_err().status(), 400);
        }
        assert_eq!(parse_bodies_range_query("from=1&count=33").unwrap_err().status(), 413);
    }

    #[test]
    fn payload_bodies_are_filtered_at_fork_boundaries() {
        use reth_chainspec::{ChainSpecBuilder, ForkCondition};
        let chain_spec = ChainSpecBuilder::default()
            .chain(1.into())
            .genesis(Default::default())
            .with_fork(EthereumHardfork::Shanghai, ForkCondition::Timestamp(10))
            .with_fork(EthereumHardfork::Cancun, ForkCondition::Timestamp(20))
            .with_fork(EthereumHardfork::Prague, ForkCondition::Timestamp(30))
            .with_fork(EthereumHardfork::Osaka, ForkCondition::Timestamp(40))
            .with_fork(EthereumHardfork::Amsterdam, ForkCondition::Timestamp(50))
            .build();
        for (fork, start, end) in [
            (EngineSszFork::Shanghai, 10, 20),
            (EngineSszFork::Cancun, 20, 30),
            (EngineSszFork::Prague, 30, 40),
            (EngineSszFork::Osaka, 40, 50),
        ] {
            let body = ExecutionPayloadBodyV1 {
                transactions: vec![Bytes::from_static(&[1, 2, 3])],
                withdrawals: Some(vec![]),
            };
            let response = payload_bodies_response(
                Ok(vec![
                    Some((start - 1, body.clone())),
                    Some((start, body.clone())),
                    Some((end - 1, body.clone())),
                    Some((end, body)),
                    None,
                ]),
                |body| ExecutionPayloadBodyShanghai::try_from(body).ok(),
                fork,
                &chain_spec,
            )
            .unwrap();
            assert_eq!(
                response.entries.iter().map(|entry| entry.available).collect::<Vec<_>>(),
                [false, true, true, false, false]
            );
            for index in [0, 3, 4] {
                assert_eq!(response.entries[index].body, ExecutionPayloadBodyShanghai::default());
            }
        }
        assert!(body_matches_fork(&chain_spec, EngineSszFork::Paris, 9));
        assert!(!body_matches_fork(&chain_spec, EngineSszFork::Paris, 10));
        assert!(!body_matches_fork(&chain_spec, EngineSszFork::Amsterdam, 49));
        assert!(body_matches_fork(&chain_spec, EngineSszFork::Amsterdam, 50));
    }

    #[tokio::test]
    async fn payload_bodies_hash_request_limits_and_media_type() {
        for content_length in [false, true] {
            for count in [32, 33] {
                let bytes =
                    BodiesByHashRequest { block_hashes: vec![B256::ZERO; count] }.as_ssz_bytes();
                let mut request = HttpRequest::builder().header(CONTENT_TYPE, OCTET_STREAM);
                if content_length {
                    request = request.header("content-length", bytes.len());
                }
                let result =
                    read_bodies_hash_body(request.body(HttpBody::from(bytes)).unwrap()).await;
                if count == 32 {
                    assert_eq!(
                        BodiesByHashRequest::from_ssz_bytes(&result.unwrap())
                            .unwrap()
                            .block_hashes
                            .len(),
                        32
                    );
                } else {
                    let response = result.unwrap_err();
                    assert_eq!(response.status(), STATUS_PAYLOAD_TOO_LARGE);
                    assert_eq!(response.headers()[CONTENT_TYPE], PROBLEM_JSON);
                }
            }
        }
        let response =
            read_bodies_hash_body(HttpRequest::new(HttpBody::empty())).await.unwrap_err();
        assert_eq!(response.status(), STATUS_UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test]
    async fn payload_bodies_errors_and_capabilities_match_rest_spec() {
        let response = payload_bodies_error_response(EngineApiError::InvalidBodiesRange {
            start: 0,
            count: 1,
        });
        assert_eq!(response.status(), 422);
        assert_eq!(response.headers()[CONTENT_TYPE], PROBLEM_JSON);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let problem: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(problem["type"], "/engine-api/errors/invalid-body");
        let body = handle_capabilities(false).into_body().collect().await.unwrap().to_bytes();
        let capabilities: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(capabilities["limits"]["bodies.max_count"], 32);
        assert!(!capabilities["endpoint_forks"]["bodies"]
            .as_array()
            .unwrap()
            .iter()
            .any(|fork| fork == "amsterdam"));
        let body = handle_capabilities(true).into_body().collect().await.unwrap().to_bytes();
        let capabilities: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(capabilities["endpoint_forks"]["bodies"]
            .as_array()
            .unwrap()
            .iter()
            .any(|fork| fork == "amsterdam"));
    }

    #[test]
    fn parses_payload_bodies_endpoints() {
        assert_eq!(
            parse_engine_path("/engine/v1/bodies/hash").unwrap(),
            EngineSszEndpoint::PayloadBodiesByHash
        );
        assert_eq!(
            parse_engine_path("/engine/v1/bodies").unwrap(),
            EngineSszEndpoint::PayloadBodiesByRange
        );
    }

    #[test]
    fn parses_get_payload_endpoints_and_malformed_ids() {
        assert_eq!(
            parse_engine_path("/engine/v1/payloads/0x0000000000000001"),
            Some(EngineSszEndpoint::GetPayload(Ok(PayloadId::new([0, 0, 0, 0, 0, 0, 0, 1]))))
        );
        assert!(matches!(
            parse_engine_path("/engine/v1/payloads/0x01"),
            Some(EngineSszEndpoint::GetPayload(Err(_)))
        ));
    }

    #[test]
    fn legacy_decodes_forkchoice_v4_with_custody_columns() {
        let forkchoice_state = ForkchoiceState {
            head_block_hash: B256::ZERO,
            safe_block_hash: B256::ZERO,
            finalized_block_hash: B256::ZERO,
        };
        let encoded =
            (forkchoice_state, Vec::<PayloadAttributes>::new(), vec![B128::with_last_byte(1)])
                .as_ssz_bytes();

        let (decoded_state, decoded_attrs, custody_columns) =
            legacy_decode_forkchoice_request(4, &encoded).unwrap();
        assert_eq!(decoded_state, forkchoice_state);
        assert!(decoded_attrs.is_none());
        assert_eq!(custody_columns, Some(B128::with_last_byte(1)));
    }

    #[test]
    fn legacy_rejects_forkchoice_v4_with_multiple_custody_columns() {
        let forkchoice_state = ForkchoiceState {
            head_block_hash: B256::ZERO,
            safe_block_hash: B256::ZERO,
            finalized_block_hash: B256::ZERO,
        };
        let encoded = (
            forkchoice_state,
            Vec::<PayloadAttributes>::new(),
            vec![B128::ZERO, B128::with_last_byte(1)],
        )
            .as_ssz_bytes();

        let err = legacy_decode_forkchoice_request(4, &encoded).unwrap_err();
        assert_eq!(err, "invalid params");
    }

    type TestHandle = EngineSszProxyHandle<
        reth_chainspec::ChainSpec,
        Arc<reth_provider::test_utils::MockEthProvider>,
        reth_transaction_pool::noop::NoopTransactionPool,
        crate::EthereumEngineValidator,
    >;

    type TestApi = EthEngineApi<
        Arc<reth_provider::test_utils::MockEthProvider>,
        reth_transaction_pool::noop::NoopTransactionPool,
        crate::EthereumEngineValidator,
        reth_chainspec::ChainSpec,
    >;

    fn test_api() -> TestApi {
        test_api_with_provider(Arc::new(Default::default()), reth_chainspec::MAINNET.clone()).0
    }

    fn test_api_with_provider(
        provider: Arc<reth_provider::test_utils::MockEthProvider>,
        chain: Arc<reth_chainspec::ChainSpec>,
    ) -> (
        TestApi,
        tokio::sync::mpsc::UnboundedReceiver<
            reth_engine_primitives::BeaconEngineMessage<EthEngineTypes>,
        >,
    ) {
        let (to_engine, engine_rx) = tokio::sync::mpsc::unbounded_channel();
        let (_payload_service, payload_store) =
            reth_payload_builder::test_utils::test_payload_service();
        let api = EngineApi::new(
            provider,
            chain.clone(),
            reth_engine_primitives::ConsensusEngineHandle::new(to_engine),
            payload_store.into(),
            reth_transaction_pool::noop::NoopTransactionPool::default(),
            reth_tasks::Runtime::test(),
            alloy_rpc_types_engine::ClientVersionV1 {
                code: alloy_rpc_types_engine::ClientCode::RH,
                name: "SSZ shared EngineApi".into(),
                version: "test".into(),
                commit: "00000000".into(),
            },
            reth_rpc_engine_api::EngineCapabilities::default(),
            crate::EthereumEngineValidator::new(chain),
            false,
            reth_network_api::noop::NoopNetwork::default(),
        );
        (api, engine_rx)
    }

    fn post_request(path: &str, version: Option<&str>) -> HttpRequest {
        let mut request =
            HttpRequest::builder().method("POST").uri(path).header(CONTENT_TYPE, OCTET_STREAM);
        if let Some(version) = version {
            request = request.header(ETH_EXECUTION_VERSION, version);
        }
        request.body(HttpBody::from(Vec::<u8>::new())).unwrap()
    }

    #[tokio::test]
    async fn http_rejects_missing_invalid_or_conflicting_execution_version() {
        let handle = TestHandle::with_engine_api(test_api());
        for (path, version) in [
            ("/engine/v1/payloads", None),
            ("/engine/v1/forkchoice", Some("future")),
            ("/engine/v2/prague/payloads", Some("cancun")),
            ("/engine/v2/prague/forkchoice", Some("prague,cancun")),
        ] {
            let response =
                handle_engine_ssz_request(handle.clone(), post_request(path, version)).await;
            assert_eq!(response.status(), STATUS_BAD_REQUEST, "{path} {version:?}");
        }
        let mut request = post_request("/engine/v1/payloads", Some("prague"));
        request.headers_mut().append(ETH_EXECUTION_VERSION, "cancun".parse().unwrap());
        assert_eq!(handle_engine_ssz_request(handle, request).await.status(), STATUS_BAD_REQUEST);
    }

    #[tokio::test]
    async fn http_legacy_aliases_share_actual_engine_api_and_decode_validation() {
        let api = test_api();
        let expected = serde_json::to_value(vec![api.client_version().clone()]).unwrap();
        let handle = TestHandle::new();
        let shared = handle.clone();
        handle.set_engine_api_sync(api);
        for path in ["/engine/v1/identity", "/engine/v2/identity"] {
            let request =
                HttpRequest::builder().uri(path).body(HttpBody::from(Vec::<u8>::new())).unwrap();
            let response = handle_engine_ssz_request(shared.clone(), request).await;
            assert_eq!(response.status(), STATUS_OK);
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(), expected);
        }
        for path in ["/engine/v2/prague/payloads", "/engine/v2/prague/forkchoice"] {
            let response =
                handle_engine_ssz_request(shared.clone(), post_request(path, None)).await;
            assert_eq!(response.status(), STATUS_BAD_REQUEST);
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes().as_ref(),
                b"invalid ssz"
            );
        }
    }

    #[tokio::test]
    async fn http_standard_and_legacy_payload_responses_preserve_their_wire_schemas() {
        use alloy_rpc_types_engine::{ExecutionPayloadV1, PayloadStatus, PayloadStatusEnum};
        use reth_engine_primitives::BeaconEngineMessage;
        let (api, mut messages) =
            test_api_with_provider(Arc::new(Default::default()), reth_chainspec::MAINNET.clone());
        let handle = TestHandle::with_engine_api(api);
        let payload =
            ExecutionPayloadV1::from_block_slow(&reth_ethereum_primitives::Block::default());
        let body = ExecutionPayloadEnvelopeParis { payload }.as_ssz_bytes();
        let status = PayloadStatus::from_status(PayloadStatusEnum::Syncing);
        let mut responses = Vec::new();
        for (path, version) in
            [("/engine/v1/payloads", Some("paris")), ("/engine/v2/paris/payloads", None)]
        {
            let mut request = post_request(path, version);
            *request.body_mut() = HttpBody::from(body.clone());
            let reply = async {
                let Some(BeaconEngineMessage::NewPayload { tx, .. }) = messages.recv().await else {
                    panic!("expected actual EngineApi newPayload callback")
                };
                tx.send(Ok(status.clone())).unwrap();
            };
            let (response, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                tokio::join!(handle_engine_ssz_request(handle.clone(), request), reply)
            })
            .await
            .expect("HTTP dispatch must reach actual EngineApi callback");
            assert_eq!(response.status(), STATUS_OK);
            responses.push(response.into_body().collect().await.unwrap().to_bytes());
        }
        assert_eq!(
            responses[0].as_ref(),
            EngineSszPayloadStatus::try_from(status.clone()).unwrap().as_ssz_bytes()
        );
        assert_eq!(responses[1].as_ref(), status.as_ssz_bytes());
        assert_ne!(responses[0], responses[1]);
    }

    #[tokio::test]
    async fn http_get_payload_and_body_transport_errors_are_distinct() {
        let handle = TestHandle::with_engine_api(test_api());
        for (path, version, status) in [
            ("/engine/v1/payloads/0x01", "paris", STATUS_BAD_REQUEST),
            ("/engine/v1/payloads/0x0000000000000001", "paris", STATUS_NOT_FOUND),
            ("/engine/v1/bodies?from=0&count=1", "paris", 422),
            ("/engine/v1/bodies?from=1&count=33", "paris", STATUS_PAYLOAD_TOO_LARGE),
            ("/engine/v1/bodies?from=1&count=1", "amsterdam", STATUS_BAD_REQUEST),
        ] {
            let request = HttpRequest::builder()
                .uri(path)
                .header(ETH_EXECUTION_VERSION, version)
                .body(HttpBody::empty())
                .unwrap();
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                handle_engine_ssz_request(handle.clone(), request),
            )
            .await
            .unwrap();
            assert_eq!(response.status(), status, "{path}");
        }
        let response = get_payload_response(BuiltPayloadParis {
            payload: alloy_rpc_types_engine::ExecutionPayloadV1::from_block_slow(
                &reth_ethereum_primitives::Block::default(),
            ),
            block_value: Default::default(),
        });
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    }

    #[tokio::test]
    async fn http_amsterdam_bodies_require_and_read_actual_injected_bal_store() {
        use alloy_eips::{eip7928::bal::RawBal, NumHash};
        use reth_storage_api::BalStoreHandle;
        let chain =
            Arc::new(reth_chainspec::ChainSpecBuilder::mainnet().amsterdam_activated().build());
        let provider = Arc::new(reth_provider::test_utils::MockEthProvider::default());
        let mut block = reth_ethereum_primitives::Block::default();
        block.header.number = 1;
        block.header.timestamp = 1;
        block.body.withdrawals = Some(Default::default());
        let raw = RawBal::new(Bytes::from_static(&[0xc0]));
        block.header.block_access_list_hash = Some(raw.hash());
        let hash = block.header.hash_slow();
        provider.add_block(hash, block);
        let store = BalStoreHandle::new(reth_provider::InMemoryBalStore::default());
        store.insert(NumHash::new(1, hash), raw.clone()).unwrap();
        let (api, _) = test_api_with_provider(provider, chain);
        assert!(!api.has_payload_body_bal_store());
        let api = api.with_payload_body_bal_store(store);
        assert!(api.clone().has_payload_body_bal_store());
        let handle = TestHandle::with_engine_api(api);
        let request = HttpRequest::builder()
            .method("POST")
            .uri("/engine/v1/bodies/hash")
            .header(ETH_EXECUTION_VERSION, "amsterdam")
            .header(CONTENT_TYPE, OCTET_STREAM)
            .body(HttpBody::from(
                BodiesByHashRequest { block_hashes: vec![hash, B256::ZERO] }.as_ssz_bytes(),
            ))
            .unwrap();
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            handle_engine_ssz_request(handle, request),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), STATUS_OK);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let bodies =
            crate::engine_ssz_containers::BodiesResponseAmsterdam::from_ssz_bytes(&bytes).unwrap();
        assert_eq!(bodies.entries.len(), 2);
        assert!(bodies.entries[0].available);
        assert_eq!(bodies.entries[0].body.block_access_list, *raw.as_raw());
        assert!(!bodies.entries[1].available);
    }

    #[tokio::test]
    async fn http_invalid_status_keeps_verdict_and_utf8_boundary_without_changing_legacy_errors() {
        use alloy_rpc_types_engine::{PayloadStatus, PayloadStatusEnum};
        use reth_engine_primitives::BeaconEngineMessage;
        let (api, mut engine_rx) =
            test_api_with_provider(Arc::new(Default::default()), reth_chainspec::MAINNET.clone());
        let handle = TestHandle::with_engine_api(api);
        let payload = ExecutionPayloadEnvelopeParis {
            payload: ExecutionPayloadV1::from_block_unchecked(
                B256::ZERO,
                &reth_ethereum_primitives::Block::default(),
            ),
        }
        .as_ssz_bytes();
        let validation_error = "€".repeat(crate::engine_ssz_containers::MAX_ERROR_BYTES);
        for (path, version, legacy) in [
            ("/engine/v1/payloads", Some("paris"), false),
            ("/engine/v2/paris/payloads", None, true),
        ] {
            let mut request = post_request(path, version);
            *request.body_mut() = HttpBody::from(payload.clone());
            let status = PayloadStatus::from_status(PayloadStatusEnum::Invalid {
                validation_error: validation_error.clone(),
            });
            let respond = async {
                let Some(BeaconEngineMessage::NewPayload { tx, .. }) = engine_rx.recv().await
                else {
                    panic!("expected actual engine newPayload callback")
                };
                tx.send(Ok(status.clone())).unwrap();
            };
            let (response, ()) = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                tokio::join!(handle_engine_ssz_request(handle.clone(), request), respond)
            })
            .await
            .unwrap();
            assert_eq!(response.status(), STATUS_OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            if legacy {
                assert_eq!(body.as_ref(), status.as_ssz_bytes());
            } else {
                let decoded = EngineSszPayloadStatus::from_ssz_bytes(&body).unwrap();
                assert_eq!(
                    PayloadStatus::from(decoded),
                    PayloadStatus::from_status(PayloadStatusEnum::Invalid {
                        validation_error: "€"
                            .repeat(crate::engine_ssz_containers::MAX_ERROR_BYTES / 3),
                    })
                );
            }
        }
    }

    #[tokio::test]
    async fn http_native_blob_containers_and_legacy_flat_lists_both_reach_actual_engine_api() {
        let chain = Arc::new(
            reth_chainspec::ChainSpecBuilder::mainnet()
                .with_fork(EthereumHardfork::Osaka, reth_chainspec::ForkCondition::Never)
                .build(),
        );
        let (api, _) = test_api_with_provider(Arc::new(Default::default()), chain);
        let handle = TestHandle::with_engine_api(api);
        for legacy in [false, true] {
            let body = if legacy {
                vec![B256::ZERO].as_ssz_bytes()
            } else {
                BlobsV1Request { versioned_hashes: vec![B256::ZERO] }.as_ssz_bytes()
            };
            let path = if legacy { "/engine/v2/blobs/v1" } else { "/engine/v1/blobs/v1" };
            let request = HttpRequest::builder()
                .method("POST")
                .uri(path)
                .header(CONTENT_TYPE, if legacy { "text/plain" } else { OCTET_STREAM })
                .body(HttpBody::from(body))
                .unwrap();
            let response = handle_engine_ssz_request(handle.clone(), request).await;
            assert_eq!(response.status(), STATUS_OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            if legacy {
                let decoded =
                    Vec::<Option<alloy_eips::eip4844::BlobAndProofV1>>::from_ssz_bytes(&body)
                        .unwrap();
                assert_eq!(decoded, vec![None]);
            } else {
                let decoded = BlobsV1Response::from_ssz_bytes(&body).unwrap();
                assert_eq!(decoded.entries.len(), 1);
                assert!(!decoded.entries[0].available);
            }
        }
    }

    #[tokio::test]
    async fn legacy_blob_routes_preserve_client_error_statuses() {
        let chain = Arc::new(
            reth_chainspec::ChainSpecBuilder::mainnet()
                .with_fork(EthereumHardfork::Osaka, reth_chainspec::ForkCondition::Never)
                .build(),
        );
        let (api, _) = test_api_with_provider(Arc::new(Default::default()), chain);
        let handle = TestHandle::with_engine_api(api);
        for (version, count, status) in
            [(2, 1, STATUS_BAD_REQUEST), (1, MAX_BLOB_LIMIT + 1, STATUS_PAYLOAD_TOO_LARGE)]
        {
            let request = HttpRequest::builder()
                .method("POST")
                .uri(format!("/engine/v2/blobs/v{version}"))
                .body(HttpBody::from(vec![B256::ZERO; count].as_ssz_bytes()))
                .unwrap();
            let response = handle_engine_ssz_request(handle.clone(), request).await;
            assert_eq!(response.status(), status);
            assert_eq!(response.headers()[CONTENT_TYPE], TEXT_PLAIN);
        }
    }

    #[tokio::test]
    async fn native_media_and_invalid_transactions_have_distinct_rest_errors() {
        let handle = TestHandle::with_engine_api(test_api());
        let mut wrong_type = post_request("/engine/v1/payloads", Some("paris"));
        wrong_type.headers_mut().insert(CONTENT_TYPE, "text/plain".parse().unwrap());
        let response = handle_engine_ssz_request(handle.clone(), wrong_type).await;
        assert_eq!(response.status(), STATUS_UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(response.headers()[CONTENT_TYPE], PROBLEM_JSON);
        let mut payload = ExecutionPayloadV1::from_block_unchecked(
            B256::ZERO,
            &reth_ethereum_primitives::Block::default(),
        );
        payload.transactions = vec![Bytes::from_static(&[0xff])];
        let mut request = post_request("/engine/v1/payloads", Some("paris"));
        *request.body_mut() =
            HttpBody::from(ExecutionPayloadEnvelopeParis { payload }.as_ssz_bytes());
        let response = handle_engine_ssz_request(handle, request).await;
        assert_eq!(response.status(), STATUS_UNPROCESSABLE_ENTITY);
        assert_eq!(response.headers()[CONTENT_TYPE], PROBLEM_JSON);
        for size in [16, 17] {
            let request = HttpRequest::builder()
                .header(CONTENT_TYPE, "text/plain")
                .body(HttpBody::from(vec![0; size]))
                .unwrap();
            let result = read_transport_body(request, 16, true).await;
            if size == 16 {
                assert_eq!(result.unwrap().len(), 16);
            } else {
                let response = result.unwrap_err();
                assert_eq!(response.status(), STATUS_PAYLOAD_TOO_LARGE);
                assert_eq!(response.headers()[CONTENT_TYPE], TEXT_PLAIN);
            }
        }
    }

    #[test]
    fn parses_capabilities_endpoint() {
        let endpoint = parse_engine_path("/engine/v1/capabilities").unwrap();
        assert_eq!(endpoint, EngineSszEndpoint::Capabilities);
    }

    #[test]
    fn parses_identity_endpoint() {
        let endpoint = parse_engine_path("/engine/v1/identity").unwrap();
        assert_eq!(endpoint, EngineSszEndpoint::Identity);
    }

    #[test]
    fn parses_fork_scoped_payload_endpoint() {
        let endpoint = parse_engine_path("/engine/v1/payloads").unwrap();
        assert_eq!(endpoint, EngineSszEndpoint::NewPayload(None));
    }

    #[test]
    fn parses_fork_scoped_forkchoice_endpoint() {
        let endpoint = parse_engine_path("/engine/v1/forkchoice").unwrap();
        assert_eq!(endpoint, EngineSszEndpoint::Forkchoice(None));
    }

    #[test]
    fn rejects_legacy_version_scoped_endpoint() {
        assert!(parse_engine_path("/engine/v4/payloads").is_none());
    }

    #[test]
    fn decodes_top_level_blob_hashes_request() {
        let hashes = vec![B256::ZERO, B256::with_last_byte(1)];
        let decoded = decode_blob_hashes_request(&hashes.as_ssz_bytes()).unwrap();
        assert_eq!(decoded, hashes);
    }

    #[test]
    fn decodes_forkchoice_v4_with_custody_columns() {
        let forkchoice_state = ForkchoiceState::same_hash(B256::ZERO);
        let encoded = ForkchoiceUpdateAmsterdam {
            forkchoice_state,
            payload_attributes: Optional::none(),
            custody_columns: Optional::some(B128::with_last_byte(1)),
        }
        .as_ssz_bytes();

        let (decoded_state, decoded_attrs, custody_columns) =
            decode_forkchoice_request(EngineSszFork::Amsterdam, &encoded).unwrap();
        assert_eq!(decoded_state, forkchoice_state);
        assert!(decoded_attrs.is_none());
        assert_eq!(custody_columns, Some(B128::with_last_byte(1)));
    }

    #[test]
    fn decodes_forkchoice_cancun_payload_attributes() {
        let forkchoice_state = ForkchoiceState::same_hash(B256::ZERO);
        let attrs = crate::engine_ssz_containers::PayloadAttributesCancun {
            timestamp: 1,
            prev_randao: B256::with_last_byte(2),
            suggested_fee_recipient: Default::default(),
            withdrawals: Vec::new(),
            parent_beacon_block_root: B256::with_last_byte(3),
        };
        let encoded =
            ForkchoiceUpdateCancun { forkchoice_state, payload_attributes: Optional::some(attrs) }
                .as_ssz_bytes();

        let (decoded_state, decoded_attrs, custody_columns) =
            decode_forkchoice_request(EngineSszFork::Cancun, &encoded).unwrap();
        assert_eq!(decoded_state, forkchoice_state);
        let decoded_attrs = decoded_attrs.unwrap();
        assert_eq!(decoded_attrs.timestamp, 1);
        assert!(decoded_attrs.withdrawals.as_ref().unwrap().is_empty());
        assert_eq!(decoded_attrs.parent_beacon_block_root, Some(B256::with_last_byte(3)));
        assert!(custody_columns.is_none());
    }
}
