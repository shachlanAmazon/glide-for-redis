mod fake_multiplexer;
/// Contains information that determines how the request and response headers are shaped.
pub mod headers;
mod rotating_buffer;
mod socket_like;
mod socket_like_listener;
mod socket_listener_impl;
pub use socket_like::*;
pub use socket_listener_impl::*;
