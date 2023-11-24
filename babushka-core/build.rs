use std::path::Path;

fn main() {
    flatc_rust::run(flatc_rust::Args {
        inputs: &[
            Path::new("src/flatbuffers-schema/connection-request.fbs"),
            Path::new("src/flatbuffers-schema/redis-request.fbs"),
            Path::new("src/flatbuffers-schema/response.fbs"),
        ],
        out_dir: Path::new(&format!(
            "{}/flatbuffers",
            std::env::var("OUT_DIR").unwrap()
        )),
        ..Default::default()
    })
    .expect("flatc");
}
