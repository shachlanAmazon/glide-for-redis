#[allow(warnings, unused)]
mod connection_request {
    include!(concat!(
        env!("OUT_DIR"),
        "/flatbuffers/connection-request_generated.rs"
    ));
}
#[allow(warnings, unused)]
mod redis_request {
    include!(concat!(
        env!("OUT_DIR"),
        "/flatbuffers/redis-request_generated.rs"
    ));
}
#[allow(warnings, unused)]
mod response {
    include!(concat!(
        env!("OUT_DIR"),
        "/flatbuffers/response_generated.rs"
    ));
}
pub mod client;
mod retry_strategies;
pub mod rotating_buffer;
mod socket_listener;
pub use socket_listener::*;
