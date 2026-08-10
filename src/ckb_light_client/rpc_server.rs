use std::{future::Future, net::SocketAddr, pin::Pin};

use jsonrpc_core::{
    futures_util::future::Either,
    middleware::{NoopCallFuture, NoopFuture},
    MetaIoHandler, Middleware, Output, Request, Response,
};
use jsonrpc_http_server::{Server, ServerBuilder};

use super::rpc_router::RpcRouter;

const MAX_REQUEST_BODY_SIZE: usize = 1024 * 1024;

#[derive(Clone, Copy)]
struct RejectBatchRequests;

impl Middleware<()> for RejectBatchRequests {
    type Future = NoopFuture;
    type CallFuture = NoopCallFuture;

    fn on_request<F, X>(&self, request: Request, metadata: (), next: F) -> Either<Self::Future, X>
    where
        F: Fn(Request, ()) -> X + Send + Sync,
        X: Future<Output = Option<Response>> + Send + 'static,
    {
        if matches!(request, Request::Batch(_)) {
            let response = Response::Single(Output::invalid_request(
                jsonrpc_core::Id::Null,
                Some(jsonrpc_core::Version::V2),
            ));
            Either::Left(Box::pin(std::future::ready(Some(response)))
                as Pin<Box<dyn Future<Output = Option<Response>> + Send>>)
        } else {
            Either::Right(next(request, metadata))
        }
    }
}

pub(crate) fn start(address: SocketAddr, router: RpcRouter) -> Result<Server, String> {
    let mut handler = MetaIoHandler::with_middleware(RejectBatchRequests);
    for method in RpcRouter::methods() {
        let router = router.clone();
        handler.add_sync_method(method, move |params| router.handle(method, params));
    }

    ServerBuilder::new(handler)
        .threads(2)
        .max_request_body_size(MAX_REQUEST_BODY_SIZE)
        .start_http(&address)
        .map_err(|err| format!("failed to start embedded CKB RPC gateway on {address}: {err}"))
}

#[cfg(test)]
mod tests {
    use jsonrpc_core::MetaIoHandler;

    use super::RejectBatchRequests;

    #[test]
    fn batch_requests_are_rejected_as_one_invalid_request() {
        let handler = MetaIoHandler::<(), _>::with_middleware(RejectBatchRequests);
        let response = handler
            .handle_request_sync(
                r#"[{"jsonrpc":"2.0","method":"one","id":1},{"jsonrpc":"2.0","method":"two","id":2}]"#,
                (),
            )
            .expect("invalid request response");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&response).unwrap(),
            serde_json::json!({
                "jsonrpc": "2.0",
                "error": {"code": -32600, "message": "Invalid request"},
                "id": null
            })
        );
    }
}
