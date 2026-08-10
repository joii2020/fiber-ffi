pub(crate) mod config;
mod rpc_router;
mod rpc_server;
pub(crate) mod runtime;

pub(crate) use runtime::LocalCkbNodeHandle;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CkbDataMode {
    #[cfg(not(feature = "disable-ckb-rpc"))]
    Hybrid { upstream_rpc_url: String },
    #[cfg(feature = "disable-ckb-rpc")]
    LightClientOnly,
}

impl CkbDataMode {
    #[cfg(not(feature = "disable-ckb-rpc"))]
    pub(crate) fn upstream_rpc_url(&self) -> Option<&str> {
        match self {
            Self::Hybrid { upstream_rpc_url } => Some(upstream_rpc_url),
        }
    }

    #[cfg(feature = "disable-ckb-rpc")]
    pub(crate) fn upstream_rpc_url(&self) -> Option<&str> {
        None
    }
}
