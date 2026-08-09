pub(crate) mod config;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CkbDataMode {
    #[cfg(not(feature = "disable-ckb-rpc"))]
    Hybrid { upstream_rpc_url: String },
    #[cfg(feature = "disable-ckb-rpc")]
    LightClientOnly,
}
