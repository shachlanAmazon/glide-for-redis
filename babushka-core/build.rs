use std::io::Result;

use prost_build::Config;

fn main() -> Result<()> {
    Config::new().bytes(["."]).compile_protos(
        &[
            "src/protobuf/redis_request.proto",
            "src/protobuf/response.proto",
            "src/protobuf/connection_request.proto",
        ],
        &["src/"],
    )?;
    Ok(())
}
