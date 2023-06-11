#![allow(dead_code)]
use babushka::{
    client::ClientCMD,
    connection_request::{self, AddressInfo, AuthenticationInfo},
};
use futures::Future;
use rand::{distributions::Alphanumeric, Rng};
use redis::{aio::ConnectionLike, ConnectionAddr, RedisConnectionInfo, RedisResult, Value};
use socket2::{Domain, Socket, Type};
use std::{env, fs, io, net::SocketAddr, net::TcpListener, path::PathBuf, process, time::Duration};
use tempfile::TempDir;

pub mod cluster;

pub(crate) const SHORT_CMD_TEST_TIMEOUT: Duration = Duration::from_millis(10_000);
pub(crate) const LONG_CMD_TEST_TIMEOUT: Duration = Duration::from_millis(20_000);

// Code copied from redis-rs

#[derive(PartialEq, Eq)]
pub enum ServerType {
    Tcp { tls: bool },
    Unix,
}

pub struct RedisServer {
    pub process: process::Child,
    tempdir: Option<tempfile::TempDir>,
    addr: redis::ConnectionAddr,
}

pub enum Module {
    Json,
}

pub fn get_available_port() -> u16 {
    // this is technically a race but we can't do better with
    // the tools that redis gives us :(
    let addr = &"127.0.0.1:0".parse::<SocketAddr>().unwrap().into();
    let socket = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
    socket.set_reuse_address(true).unwrap();
    socket.bind(addr).unwrap();
    socket.listen(1).unwrap();
    let listener = TcpListener::from(socket);
    listener.local_addr().unwrap().port()
}

impl RedisServer {
    pub fn new(server_type: ServerType) -> RedisServer {
        RedisServer::with_modules(server_type, &[])
    }

    pub fn with_modules(server_type: ServerType, modules: &[Module]) -> RedisServer {
        let addr = match server_type {
            ServerType::Tcp { tls } => {
                let redis_port = get_available_port();
                if tls {
                    redis::ConnectionAddr::TcpTls {
                        host: "127.0.0.1".to_string(),
                        port: redis_port,
                        insecure: true,
                    }
                } else {
                    redis::ConnectionAddr::Tcp("127.0.0.1".to_string(), redis_port)
                }
            }
            ServerType::Unix => {
                let (a, b) = rand::random::<(u64, u64)>();
                let path = format!("/tmp/redis-rs-test-{a}-{b}.sock");
                redis::ConnectionAddr::Unix(PathBuf::from(&path))
            }
        };
        RedisServer::new_with_addr_and_modules(addr, modules)
    }

    pub fn new_with_addr_and_modules(
        addr: redis::ConnectionAddr,
        modules: &[Module],
    ) -> RedisServer {
        RedisServer::new_with_addr_tls_modules_and_spawner(addr, None, modules, |cmd| {
            cmd.spawn()
                .unwrap_or_else(|err| panic!("Failed to run {cmd:?}: {err}"))
        })
    }

    pub fn new_with_addr_tls_modules_and_spawner<
        F: FnOnce(&mut process::Command) -> process::Child,
    >(
        addr: redis::ConnectionAddr,
        tls_paths: Option<TlsFilePaths>,
        modules: &[Module],
        spawner: F,
    ) -> RedisServer {
        let mut redis_cmd = process::Command::new("redis-server");

        for module in modules {
            match module {
                Module::Json => {
                    redis_cmd
                        .arg("--loadmodule")
                        .arg(env::var("REDIS_RS_REDIS_JSON_PATH").expect(
                        "Unable to find path to RedisJSON at REDIS_RS_REDIS_JSON_PATH, is it set?",
                    ));
                }
            };
        }

        redis_cmd
            .stdout(process::Stdio::null())
            .stderr(process::Stdio::null())
            .arg("--enable-debug-command")
            .arg("local");
        let tempdir = tempfile::Builder::new()
            .prefix("redis")
            .tempdir()
            .expect("failed to create tempdir");
        match addr {
            redis::ConnectionAddr::Tcp(ref bind, server_port) => {
                redis_cmd
                    .arg("--port")
                    .arg(server_port.to_string())
                    .arg("--bind")
                    .arg(bind);

                RedisServer {
                    process: spawner(&mut redis_cmd),
                    tempdir: None,
                    addr,
                }
            }
            redis::ConnectionAddr::TcpTls { ref host, port, .. } => {
                let tls_paths = tls_paths.unwrap_or_else(|| build_keys_and_certs_for_tls(&tempdir));

                // prepare redis with TLS
                redis_cmd
                    .arg("--tls-port")
                    .arg(&port.to_string())
                    .arg("--port")
                    .arg("0")
                    .arg("--tls-cert-file")
                    .arg(&tls_paths.redis_crt)
                    .arg("--tls-key-file")
                    .arg(&tls_paths.redis_key)
                    .arg("--tls-ca-cert-file")
                    .arg(&tls_paths.ca_crt)
                    .arg("--tls-auth-clients") // Make it so client doesn't have to send cert
                    .arg("no")
                    .arg("--bind")
                    .arg(host);

                let addr = redis::ConnectionAddr::TcpTls {
                    host: host.clone(),
                    port,
                    insecure: true,
                };

                RedisServer {
                    process: spawner(&mut redis_cmd),
                    tempdir: Some(tempdir),
                    addr,
                }
            }
            redis::ConnectionAddr::Unix(ref path) => {
                redis_cmd
                    .arg("--port")
                    .arg("0")
                    .arg("--unixsocket")
                    .arg(path);
                RedisServer {
                    process: spawner(&mut redis_cmd),
                    tempdir: Some(tempdir),
                    addr,
                }
            }
        }
    }

    pub fn get_client_addr(&self) -> redis::ConnectionAddr {
        self.addr.clone()
    }

    pub fn connection_info(&self) -> redis::ConnectionInfo {
        redis::ConnectionInfo {
            addr: self.get_client_addr(),
            redis: Default::default(),
        }
    }

    pub fn stop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
        if let redis::ConnectionAddr::Unix(ref path) = self.get_client_addr() {
            fs::remove_file(path).ok();
        }
    }
}

impl Drop for RedisServer {
    fn drop(&mut self) {
        self.stop()
    }
}

pub fn encode_value<W>(value: &Value, writer: &mut W) -> io::Result<()>
where
    W: io::Write,
{
    #![allow(clippy::write_with_newline)]
    match *value {
        Value::Nil => write!(writer, "$-1\r\n"),
        Value::Int(val) => write!(writer, ":{val}\r\n"),
        Value::Data(ref val) => {
            write!(writer, "${}\r\n", val.len())?;
            writer.write_all(val)?;
            writer.write_all(b"\r\n")
        }
        Value::Bulk(ref values) => {
            write!(writer, "*{}\r\n", values.len())?;
            for val in values.iter() {
                encode_value(val, writer)?;
            }
            Ok(())
        }
        Value::Okay => write!(writer, "+OK\r\n"),
        Value::Status(ref s) => write!(writer, "+{s}\r\n"),
    }
}

#[derive(Clone)]
pub struct TlsFilePaths {
    redis_crt: PathBuf,
    redis_key: PathBuf,
    ca_crt: PathBuf,
}

pub fn build_keys_and_certs_for_tls(tempdir: &TempDir) -> TlsFilePaths {
    // Based on shell script in redis's server tests
    // https://github.com/redis/redis/blob/8c291b97b95f2e011977b522acf77ead23e26f55/utils/gen-test-certs.sh
    let ca_crt = tempdir.path().join("ca.crt");
    let ca_key = tempdir.path().join("ca.key");
    let ca_serial = tempdir.path().join("ca.txt");
    let redis_crt = tempdir.path().join("redis.crt");
    let redis_key = tempdir.path().join("redis.key");

    fn make_key<S: AsRef<std::ffi::OsStr>>(name: S, size: usize) {
        process::Command::new("openssl")
            .arg("genrsa")
            .arg("-out")
            .arg(name)
            .arg(&format!("{size}"))
            .stdout(process::Stdio::null())
            .stderr(process::Stdio::null())
            .spawn()
            .expect("failed to spawn openssl")
            .wait()
            .expect("failed to create key");
    }

    // Build CA Key
    make_key(&ca_key, 4096);

    // Build redis key
    make_key(&redis_key, 2048);

    // Build CA Cert
    process::Command::new("openssl")
        .arg("req")
        .arg("-x509")
        .arg("-new")
        .arg("-nodes")
        .arg("-sha256")
        .arg("-key")
        .arg(&ca_key)
        .arg("-days")
        .arg("3650")
        .arg("-subj")
        .arg("/O=Redis Test/CN=Certificate Authority")
        .arg("-out")
        .arg(&ca_crt)
        .stdout(process::Stdio::null())
        .stderr(process::Stdio::null())
        .spawn()
        .expect("failed to spawn openssl")
        .wait()
        .expect("failed to create CA cert");

    // Read redis key
    let mut key_cmd = process::Command::new("openssl")
        .arg("req")
        .arg("-new")
        .arg("-sha256")
        .arg("-subj")
        .arg("/O=Redis Test/CN=Generic-cert")
        .arg("-key")
        .arg(&redis_key)
        .stdout(process::Stdio::piped())
        .stderr(process::Stdio::null())
        .spawn()
        .expect("failed to spawn openssl");

    // build redis cert
    process::Command::new("openssl")
        .arg("x509")
        .arg("-req")
        .arg("-sha256")
        .arg("-CA")
        .arg(&ca_crt)
        .arg("-CAkey")
        .arg(&ca_key)
        .arg("-CAserial")
        .arg(&ca_serial)
        .arg("-CAcreateserial")
        .arg("-days")
        .arg("365")
        .arg("-out")
        .arg(&redis_crt)
        .stdin(key_cmd.stdout.take().expect("should have stdout"))
        .stdout(process::Stdio::null())
        .stderr(process::Stdio::null())
        .spawn()
        .expect("failed to spawn openssl")
        .wait()
        .expect("failed to create redis cert");

    key_cmd.wait().expect("failed to create redis key");

    TlsFilePaths {
        redis_crt,
        redis_key,
        ca_crt,
    }
}

pub async fn wait_for_server_to_become_ready(server_address: &ConnectionAddr) {
    let millisecond = Duration::from_millis(1);
    let mut retries = 0;
    let client = redis::Client::open(redis::ConnectionInfo {
        addr: server_address.clone(),
        redis: RedisConnectionInfo::default(),
    })
    .unwrap();
    loop {
        match client.get_multiplexed_async_connection().await {
            Err(err) => {
                if err.is_connection_refusal() {
                    tokio::time::sleep(millisecond).await;
                    retries += 1;
                    if retries > 100000 {
                        panic!("Tried to connect too many times, last error: {err}");
                    }
                } else {
                    panic!("Could not connect: {err}");
                }
            }
            Ok(mut con) => {
                while con.send_packed_command(&redis::cmd("PING")).await.is_err() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                let _: RedisResult<()> = redis::cmd("FLUSHDB").query_async(&mut con).await;
                break;
            }
        }
    }
}

pub fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

pub fn block_on_all<F>(f: F) -> F::Output
where
    F: Future,
{
    current_thread_runtime().block_on(f)
}

pub fn get_address_info(address: &ConnectionAddr) -> AddressInfo {
    match address {
        ConnectionAddr::Tcp(host, port) => AddressInfo {
            host: host.clone(),
            port: *port as u32,
        },

        ConnectionAddr::TcpTls {
            host,
            port,
            insecure: _,
        } => AddressInfo {
            host: host.clone(),
            port: *port as u32,
        },
        ConnectionAddr::Unix(_) => unreachable!("Unix connection not tested"),
    }
}

pub fn generate_random_string(length: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

pub async fn send_get(client: &mut impl ConnectionLike, key: &str) -> RedisResult<Value> {
    let mut get_command = redis::Cmd::new();
    get_command.arg("GET").arg(key);
    client.req_packed_command(&get_command).await
}

pub async fn send_set_and_get(mut client: impl ConnectionLike, key: String) {
    const VALUE_LENGTH: usize = 10;
    let value = generate_random_string(VALUE_LENGTH);

    let mut set_command = redis::Cmd::new();
    set_command.arg("SET").arg(key.as_str()).arg(value.clone());
    let set_result = client.req_packed_command(&set_command).await.unwrap();
    let mut get_command = redis::Cmd::new();
    get_command.arg("GET").arg(key);
    let get_result = client.req_packed_command(&get_command).await.unwrap();

    assert_eq!(set_result, Value::Okay);
    assert_eq!(get_result, Value::Data(value.into_bytes()));
}

pub struct TestBasics {
    pub server: RedisServer,
    pub client: ClientCMD,
}

fn set_connection_info_to_connection_request(
    connection_info: RedisConnectionInfo,
    connection_request: &mut connection_request::ConnectionRequest,
) {
    if connection_info.password.is_some() {
        connection_request.authentication_info = Some(AuthenticationInfo {
            password: connection_info.password.unwrap(),
            username: connection_info.username.unwrap_or_default(),
            ..Default::default()
        });
    }
}

pub async fn setup_acl(addr: &ConnectionAddr, connection_info: &RedisConnectionInfo) {
    let client = redis::Client::open(redis::ConnectionInfo {
        addr: addr.clone(),
        redis: RedisConnectionInfo::default(),
    })
    .unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let password = connection_info.password.clone().unwrap();
    let username = connection_info
        .username
        .clone()
        .unwrap_or_else(|| "default".to_string());
    let mut cmd = redis::cmd("ACL");
    cmd.arg("SETUSER")
        .arg(username)
        .arg("on")
        .arg("allkeys")
        .arg("+@all")
        .arg(format!(">{password}"));
    connection.req_packed_command(&cmd).await.unwrap();
}

#[derive(Eq, PartialEq, Default)]
pub enum ClusterMode {
    #[default]
    Disabled,
    Enabled,
}

pub fn create_connection_request(
    addresses: &[ConnectionAddr],
    configuration: &TestConfiguration,
) -> connection_request::ConnectionRequest {
    let addresses_info = addresses.iter().map(get_address_info).collect();
    let mut connection_request = connection_request::ConnectionRequest::default();
    connection_request.addresses = addresses_info;
    connection_request.tls_mode = if configuration.use_tls {
        connection_request::TlsMode::InsecureTls
    } else {
        connection_request::TlsMode::NoTls
    }
    .into();
    connection_request.cluster_mode_enabled = ClusterMode::Enabled == configuration.cluster_mode;
    if let Some(response_timeout) = configuration.response_timeout {
        connection_request.response_timeout = response_timeout;
    }
    if let Some(connection_timeout) = configuration.connection_timeout {
        connection_request.client_creation_timeout = connection_timeout;
    }
    set_connection_info_to_connection_request(
        configuration.connection_info.clone().unwrap_or_default(),
        &mut connection_request,
    );
    connection_request
}

#[derive(Default)]
pub struct TestConfiguration {
    pub use_tls: bool,
    pub connection_retry_strategy: Option<connection_request::ConnectionRetryStrategy>,
    pub connection_info: Option<RedisConnectionInfo>,
    pub cluster_mode: ClusterMode,
    pub response_timeout: Option<u32>,
    pub connection_timeout: Option<u32>,
}

pub async fn setup_test_basics_internal(configuration: &TestConfiguration) -> TestBasics {
    let server = RedisServer::new(ServerType::Tcp {
        tls: configuration.use_tls,
    });
    if let Some(redis_connection_info) = &configuration.connection_info {
        setup_acl(&server.get_client_addr(), redis_connection_info).await;
    }
    let mut connection_request =
        create_connection_request(&[server.get_client_addr()], configuration);
    connection_request.connection_retry_strategy = configuration.connection_retry_strategy.clone();
    connection_request.cluster_mode_enabled = false;
    let client = ClientCMD::create_client(connection_request).await.unwrap();
    TestBasics { server, client }
}

pub async fn setup_test_basics(use_tls: bool) -> TestBasics {
    setup_test_basics_internal(&TestConfiguration {
        use_tls,
        ..Default::default()
    })
    .await
}

#[cfg(test)]
#[ctor::ctor]
fn init() {
    logger_core::init_console(logger_core::Level::Debug);
}
