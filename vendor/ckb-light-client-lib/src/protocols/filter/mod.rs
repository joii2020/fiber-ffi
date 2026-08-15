use std::time::Duration;

mod block_filter;
mod components;
mod peer_selection;

const BAD_MESSAGE_BAN_TIME: Duration = Duration::from_secs(5 * 60);

pub use block_filter::FilterProtocol;
pub use peer_selection::FilterPeerSelectionConfig;

#[cfg(test)]
pub(crate) use block_filter::GET_BLOCK_FILTERS_TOKEN;
