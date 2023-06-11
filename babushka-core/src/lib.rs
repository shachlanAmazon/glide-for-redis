pub mod client;
mod retry_strategies;
pub mod rotating_buffer;
mod socket_listener;
pub use socket_listener::*;

// Include the `items` module, which is generated from items.proto.
// It is important to maintain the same structure as in the proto.
pub mod response {
    include!(concat!(env!("OUT_DIR"), "/response.rs"));
}
pub mod redis_request {
    include!(concat!(env!("OUT_DIR"), "/redis_request.rs"));
}

pub mod connection_request {
    include!(concat!(env!("OUT_DIR"), "/connection_request.rs"));
}
