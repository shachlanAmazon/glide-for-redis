use super::{create_connection_request, ClusterMode, TestConfiguration};
use babushka::client::Client;
use babushka::connection_request::AddressInfo;
use futures::future::{join_all, BoxFuture};
use futures::FutureExt;
use redis::{ConnectionAddr, RedisConnectionInfo};
use std::process::Command;
use std::time::Duration;
use which::which;

// Code copied from redis-rs

pub(crate) const SHORT_CLUSTER_TEST_TIMEOUT: Duration = Duration::from_millis(50_000);
pub(crate) const LONG_CLUSTER_TEST_TIMEOUT: Duration = Duration::from_millis(60_000);

enum ClusterType {
    Tcp,
    TcpTls,
}

impl ClusterType {
    fn build_addr(use_tls: bool, host: &str, port: u16) -> redis::ConnectionAddr {
        if use_tls {
            redis::ConnectionAddr::TcpTls {
                host: host.to_string(),
                port,
                insecure: true,
            }
        } else {
            redis::ConnectionAddr::Tcp(host.to_string(), port)
        }
    }
}

pub struct RedisCluster {
    cluster_folder: String,
    addresses: Vec<AddressInfo>,
    use_tls: bool,
    password: Option<String>,
}

impl Drop for RedisCluster {
    fn drop(&mut self) {
        Self::execute_cluster_script(
            vec!["stop", "--cluster-folder", &self.cluster_folder],
            self.use_tls,
            self.password.clone(),
        );
    }
}

impl RedisCluster {
    pub fn new(
        use_tls: bool,
        conn_info: &Option<RedisConnectionInfo>,
        shards: Option<u16>,
        replicas: Option<u16>,
    ) -> RedisCluster {
        let mut script_args = vec!["start"];
        let shards_num: String;
        let replicas_num: String;
        if let Some(shards) = shards {
            shards_num = shards.to_string();
            script_args.push("-n");
            script_args.push(&shards_num);
        }
        if let Some(replicas) = replicas {
            replicas_num = replicas.to_string();
            script_args.push("-r");
            script_args.push(&replicas_num);
        }
        let output: String = Self::execute_cluster_script(script_args, use_tls, None);
        let (cluster_folder, addresses) = Self::parse_start_script_output(&output);
        let mut password: Option<String> = None;
        if let Some(info) = conn_info {
            password = info.password.clone();
        };
        RedisCluster {
            cluster_folder,
            addresses,
            use_tls,
            password,
        }
    }

    fn parse_start_script_output(output: &str) -> (String, Vec<AddressInfo>) {
        let cluster_folder = output.split("CLUSTER_FOLDER=").collect::<Vec<&str>>();
        assert!(
            !cluster_folder.is_empty() && cluster_folder.len() >= 2,
            "{:?}",
            cluster_folder
        );
        let cluster_folder = cluster_folder.get(1).unwrap().lines();
        let cluster_folder = cluster_folder.collect::<Vec<&str>>();
        let cluster_folder = cluster_folder.first().unwrap().to_string();

        let output_parts = output.split("CLUSTER_NODES=").collect::<Vec<&str>>();
        assert!(!output_parts.is_empty() && output_parts.len() >= 2);
        let nodes = output_parts.get(1).unwrap().split(',');
        let mut address_vec: Vec<AddressInfo> = Vec::new();
        for node in nodes {
            let node_parts = node.split(':').collect::<Vec<&str>>();

            let mut address_info = AddressInfo::default();
            address_info.host = node_parts.first().unwrap().to_string();

            address_info.port = node_parts.get(1).unwrap().parse::<u32>().unwrap();
            address_vec.push(address_info);
        }
        (cluster_folder, address_vec)
    }

    fn execute_cluster_script(args: Vec<&str>, use_tls: bool, password: Option<String>) -> String {
        let python_binary = which("python3").unwrap();
        let mut script_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        script_path.push("../utils/cluster_manager.py");
        assert!(script_path.exists());
        let cmd = format!(
            "{} {} {} {} {}",
            python_binary.display(),
            script_path.display(),
            if use_tls { "--tls" } else { "" },
            if let Some(pass) = password {
                format!("--auth {pass}")
            } else {
                "".to_string()
            },
            args.join(" ")
        );
        let output = if cfg!(target_os = "windows") {
            Command::new("cmd")
                .args(["/C", &cmd])
                .output()
                .expect("failed to execute process")
        } else {
            Command::new("sh")
                .arg("-c")
                .arg(&cmd)
                .output()
                .expect("failed to execute process")
        };
        let parsed_output = output.stdout;
        std::str::from_utf8(&parsed_output)
            .unwrap()
            .trim()
            .to_string()
    }

    pub fn get_server_addresses(&self) -> Vec<ConnectionAddr> {
        self.addresses
            .iter()
            .map(|address| {
                ClusterType::build_addr(self.use_tls, &address.host, address.port as u16)
            })
            .collect()
    }
}

pub struct ClusterTestBasics {
    pub cluster: RedisCluster,
    pub client: Client,
}

async fn setup_acl_for_cluster(
    addresses: &[ConnectionAddr],
    connection_info: &RedisConnectionInfo,
) {
    let ops: Vec<BoxFuture<()>> = addresses
        .iter()
        .map(|addr| (async move { super::setup_acl(addr, connection_info).await }).boxed())
        .collect();
    join_all(ops).await;
}

pub async fn setup_test_basics_internal(mut configuration: TestConfiguration) -> ClusterTestBasics {
    let cluster = RedisCluster::new(
        configuration.use_tls,
        &configuration.connection_info,
        None,
        None,
    );
    if let Some(redis_connection_info) = &configuration.connection_info {
        setup_acl_for_cluster(&cluster.get_server_addresses(), redis_connection_info).await;
    }
    configuration.cluster_mode = ClusterMode::Enabled;
    configuration.response_timeout = Some(10000);
    let connection_request =
        create_connection_request(&cluster.get_server_addresses(), &configuration);

    let client = Client::new(connection_request).await.unwrap();
    ClusterTestBasics { cluster, client }
}

pub async fn setup_test_basics(use_tls: bool) -> ClusterTestBasics {
    setup_test_basics_internal(TestConfiguration {
        use_tls,
        ..Default::default()
    })
    .await
}
