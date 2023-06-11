use std::io::Write;

use babushka::{
    redis_request::{command, redis_request},
    redis_request::{Command, RedisRequest, RequestType},
    rotating_buffer::RotatingBuffer,
};
use bytes::BufMut;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use integer_encoding::VarInt;
use prost::Message;
use rand::{distributions::Alphanumeric, Rng};

fn benchmark(
    c: &mut Criterion,
    test_group: &str,
    mut benchmark_fn: impl FnMut(&Vec<Vec<u8>>),
    test_name: &str,
    test_data: Vec<Vec<u8>>,
) {
    let mut group = c.benchmark_group(test_group);
    group.sample_size(5000);

    group.bench_function(test_name, move |b| {
        b.iter(|| benchmark_fn(black_box(&test_data)));
    });
}

fn benchmark_short_test_data(
    c: &mut Criterion,
    test_group: &str,
    benchmark_fn: impl FnMut(&Vec<Vec<u8>>),
) {
    benchmark(
        c,
        test_group,
        benchmark_fn,
        "short_Test_data",
        short_test_data(),
    );
}
fn benchmark_medium_test_data(
    c: &mut Criterion,
    test_group: &str,
    benchmark_fn: impl FnMut(&Vec<Vec<u8>>),
) {
    benchmark(
        c,
        test_group,
        benchmark_fn,
        "medium_test_data",
        medium_test_data(),
    );
}
fn benchmark_long_pointer_test_data(
    c: &mut Criterion,
    test_group: &str,
    benchmark_fn: impl FnMut(&Vec<Vec<u8>>),
) {
    benchmark(
        c,
        test_group,
        benchmark_fn,
        "long_pointer_test_data",
        long_pointer_test_data(),
    );
}
fn benchmark_long_test_data(
    c: &mut Criterion,
    test_group: &str,
    benchmark_fn: impl FnMut(&Vec<Vec<u8>>),
) {
    benchmark(
        c,
        test_group,
        benchmark_fn,
        "long_test_data",
        long_test_data(),
    );
}
fn benchmark_multiple_requests_test_data(
    c: &mut Criterion,
    test_group: &str,
    benchmark_fn: impl FnMut(&Vec<Vec<u8>>),
) {
    benchmark(
        c,
        test_group,
        benchmark_fn,
        "multiple_requests_test_data",
        multiple_requests_test_data(),
    );
}
fn benchmark_split_data(
    c: &mut Criterion,
    test_group: &str,
    benchmark_fn: impl FnMut(&Vec<Vec<u8>>),
) {
    benchmark(c, test_group, benchmark_fn, "split_data", split_data());
}

fn generate_random_string(length: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

fn write_length(buffer: &mut Vec<u8>, length: u32) {
    let required_space = u32::required_space(length);
    let new_len = buffer.len() + required_space;
    buffer.resize(new_len, 0_u8);
    u32::encode_var(length, &mut buffer[new_len - required_space..]);
}

fn message_buffer(request: RedisRequest) -> Vec<u8> {
    let message_length = request.encoded_len() as usize;
    let mut buffer = Vec::with_capacity(message_length);
    write_length(&mut buffer, message_length as u32);
    request.encode(&mut buffer).unwrap();
    buffer
}

fn short_test_data() -> Vec<Vec<u8>> {
    vec![message_buffer(short_request())]
}

fn medium_test_data() -> Vec<Vec<u8>> {
    vec![message_buffer(medium_request())]
}

fn long_test_data() -> Vec<Vec<u8>> {
    vec![message_buffer(long_request(false))]
}

fn long_pointer_test_data() -> Vec<Vec<u8>> {
    vec![message_buffer(long_request(true))]
}

fn multiple_requests_test_data() -> Vec<Vec<u8>> {
    let mut vec = Vec::new();
    vec.append(&mut message_buffer(long_request(false)));
    vec.append(&mut message_buffer(medium_request()));
    vec.append(&mut message_buffer(long_request(true)));
    vec![vec]
}

fn split_data() -> Vec<Vec<u8>> {
    let mut vec = Vec::new();
    vec.append(&mut message_buffer(long_request(false)));
    vec.append(&mut message_buffer(medium_request()));
    vec.append(&mut message_buffer(long_request(true)));
    vec.append(&mut message_buffer(long_request(true)));
    vec.append(&mut message_buffer(medium_request()));
    vec.append(&mut message_buffer(short_request()));
    vec.append(&mut message_buffer(long_request(false)));
    let mut vec1 = vec.split_off(200);
    let vec2 = vec1.split_off(2000);
    vec![vec, vec1, vec2]
}

fn create_request(args: Vec<String>, args_pointer: bool) -> RedisRequest {
    let mut request = RedisRequest::default();
    request.callback_idx = 1;
    let mut command = Command::default();
    command.request_type = RequestType::CustomCommand.into();
    if args_pointer {
        command.args = Some(command::Args::ArgsVecPointer(Box::leak(Box::new(args))
            as *mut Vec<String>
            as u64));
    } else {
        let mut args_array = command::ArgsArray::default();
        args_array.args = args;
        command.args = Some(command::Args::ArgsArray(args_array));
    }
    request.command = Some(redis_request::Command::SingleCommand(command));
    request
}

fn short_request() -> RedisRequest {
    create_request(vec!["GET".into(), "goo".into(), "bar".into()], false)
}

fn medium_request() -> RedisRequest {
    create_request(
        vec![
            "GET".into(),
            generate_random_string(126),
            generate_random_string(512),
        ],
        false,
    )
}

fn long_request(args_pointer: bool) -> RedisRequest {
    create_request(
        vec![
            "GET".into(),
            generate_random_string(1021),
            generate_random_string(2042),
            generate_random_string(1023),
            generate_random_string(4097),
        ],
        args_pointer,
    )
}

macro_rules! run_bench {
    ($test_name: ident, $c:ident, $rotating_buffer:ident) => {
        $test_name($c, "rotating_buffer", |test_data: &Vec<Vec<u8>>| {
            for data in test_data {
                $rotating_buffer.current_buffer().put(&data[..]);
                $rotating_buffer.get_requests::<RedisRequest>().unwrap();
            }
            $rotating_buffer.current_buffer().clear()
        });
    };
}

fn rotating_buffer_bench(c: &mut Criterion) {
    let mut rotating_buffer = RotatingBuffer::new(1024);

    run_bench!(benchmark_short_test_data, c, rotating_buffer);
    run_bench!(benchmark_medium_test_data, c, rotating_buffer);
    run_bench!(benchmark_long_test_data, c, rotating_buffer);
    run_bench!(benchmark_long_pointer_test_data, c, rotating_buffer);
    run_bench!(benchmark_multiple_requests_test_data, c, rotating_buffer);
    run_bench!(benchmark_split_data, c, rotating_buffer);
}

criterion_group!(rotating_buffer, rotating_buffer_bench,);

criterion_main!(rotating_buffer);
