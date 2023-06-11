use babushka::{
    client::{BabushkaClient, Client},
    connection_request::{AddressInfo, ConnectionRequest, TlsMode},
};
use criterion::{criterion_group, criterion_main, Criterion};
use futures::future::join_all;
use redis::{
    AsyncCommands, ConnectionAddr, ConnectionInfo, RedisConnectionInfo, RedisResult, Value,
};
use std::env;
use tokio::runtime::{Builder, Runtime};

async fn run_get(mut connection: impl BabushkaClient) -> RedisResult<Value> {
    connection.get("foo").await
}

fn benchmark_single_get(
    c: &mut Criterion,
    connection_id: &str,
    test_group: &str,
    connection: impl BabushkaClient,
    runtime: &Runtime,
) {
    let mut group = c.benchmark_group(test_group);
    group.significance_level(0.1).sample_size(500);
    group.bench_function(format!("{connection_id}-single get"), move |b| {
        b.to_async(runtime).iter(|| run_get(connection.clone()));
    });
}

fn benchmark_concurrent_gets(
    c: &mut Criterion,
    connection_id: &str,
    test_group: &str,
    connection: impl BabushkaClient,
    runtime: &Runtime,
) {
    const ITERATIONS: usize = 100;
    let mut group = c.benchmark_group(test_group);
    group.significance_level(0.1).sample_size(150);
    group.bench_function(format!("{connection_id}-concurrent gets"), move |b| {
        b.to_async(runtime).iter(|| {
            let mut actions = Vec::with_capacity(ITERATIONS);
            for _ in 0..ITERATIONS {
                actions.push(run_get(connection.clone()));
            }
            join_all(actions)
        });
    });
}

fn benchmark<Fun, Con>(
    c: &mut Criterion,
    address: ConnectionAddr,
    connection_id: &str,
    group: &str,
    connection_creation: Fun,
) where
    Con: BabushkaClient,
    Fun: FnOnce(ConnectionAddr, &Runtime) -> Con,
{
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    let connection = connection_creation(address, &runtime);
    benchmark_single_get(c, connection_id, group, connection.clone(), &runtime);
    benchmark_concurrent_gets(c, connection_id, group, connection, &runtime);
}

fn get_connection_info(address: ConnectionAddr) -> redis::ConnectionInfo {
    ConnectionInfo {
        addr: address,
        redis: RedisConnectionInfo::default(),
    }
}

fn multiplexer_benchmark(c: &mut Criterion, address: ConnectionAddr, group: &str) {
    benchmark(c, address, "multiplexer", group, |address, runtime| {
        let client = redis::Client::open(get_connection_info(address)).unwrap();
        runtime.block_on(async { client.get_multiplexed_tokio_connection().await.unwrap() })
    });
}

fn connection_manager_benchmark(c: &mut Criterion, address: ConnectionAddr, group: &str) {
    benchmark(
        c,
        address,
        "connection-manager",
        group,
        |address, runtime| {
            let client = redis::Client::open(get_connection_info(address)).unwrap();
            runtime.block_on(async { client.get_tokio_connection_manager().await.unwrap() })
        },
    );
}

fn create_connection_request(address: ConnectionAddr) -> ConnectionRequest {
    let mut request = ConnectionRequest::default();
    match address {
        ConnectionAddr::Tcp(host, port) => {
            request.tls_mode = TlsMode::NoTls.into();
            let mut address_info = AddressInfo::default();
            address_info.host = host;
            address_info.port = port as u32;
            request.addresses.push(address_info);
        }
        ConnectionAddr::TcpTls {
            host,
            port,
            insecure,
        } => {
            request.tls_mode = if insecure {
                TlsMode::InsecureTls.into()
            } else {
                TlsMode::SecureTls.into()
            };
            let mut address_info = AddressInfo::default();
            address_info.host = host;
            address_info.port = port as u32;
            request.addresses.push(address_info);
        }
        _ => unreachable!(),
    };
    request
}

fn client_cmd_benchmark(c: &mut Criterion, address: ConnectionAddr, group: &str) {
    benchmark(c, address, "ClientCMD", group, |address, runtime| {
        runtime
            .block_on(Client::new(create_connection_request(address)))
            .unwrap()
    });
}

fn local_benchmark<F: FnOnce(&mut Criterion, ConnectionAddr, &str)>(c: &mut Criterion, f: F) {
    f(
        c,
        ConnectionAddr::Tcp("localhost".to_string(), 6379),
        "local",
    );
}

fn get_tls_address() -> Result<ConnectionAddr, impl std::error::Error> {
    env::var("HOST").map(|host| ConnectionAddr::TcpTls {
        host,
        port: 6379,
        insecure: false,
    })
}

fn remote_benchmark<F: FnOnce(&mut Criterion, ConnectionAddr, &str)>(c: &mut Criterion, f: F) {
    let Ok(address) = get_tls_address() else {
        eprintln!("*** HOST must be set as an env parameter for remote server benchmark ***");
        return;
    };
    f(c, address, "remote");
}

fn multiplexer_benchmarks(c: &mut Criterion) {
    remote_benchmark(c, multiplexer_benchmark);
    local_benchmark(c, multiplexer_benchmark)
}

fn connection_manager_benchmarks(c: &mut Criterion) {
    remote_benchmark(c, connection_manager_benchmark);
    local_benchmark(c, connection_manager_benchmark)
}

fn client_cmd_benchmarks(c: &mut Criterion) {
    remote_benchmark(c, client_cmd_benchmark);
    local_benchmark(c, client_cmd_benchmark)
}

criterion_group!(
    benches,
    connection_manager_benchmarks,
    multiplexer_benchmarks,
    client_cmd_benchmarks
);

criterion_main!(benches);
