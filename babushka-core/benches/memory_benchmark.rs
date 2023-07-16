use babushka::{
    client::Client,
    connection_request::{AddressInfo, ConnectionRequest, TlsMode},
};
use iai_callgrind::{black_box, main};
use redis::{aio::ConnectionLike, Value};
use tokio::runtime::Builder;

fn create_connection_request() -> ConnectionRequest {
    let host = "localhost";
    let mut request = ConnectionRequest::new();
    request.tls_mode = TlsMode::NoTls.into();
    let mut address_info = AddressInfo::new();
    address_info.host = host.into();
    address_info.port = 6379;
    request.addresses.push(address_info);
    request
}

fn runner<Fut>(f: impl FnOnce(Client) -> Fut)
where
    Fut: futures::Future<Output = ()>,
{
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let client = Client::new(create_connection_request()).await.unwrap();
        f(client).await;
    });
}

// Don't forget the `#[inline(never)]`
#[inline(never)]
fn just_setup() {
    runner(|_| async {});
}

// Don't forget the `#[inline(never)]`
#[inline(never)]
fn send_message() {
    runner(|mut client| async move {
        client
            .req_packed_command(black_box(redis::pack_command_to_bytes(
                vec!["PING"].iter(),
                None,
            )))
            .await
            .unwrap();
    });
}

// Don't forget the `#[inline(never)]`
#[inline(never)]
fn send_and_receive_messages() {
    let mut num_to_string = itoa::Buffer::new();
    runner(|mut client| async move {
        client
            .req_packed_command(black_box(redis::pack_command_to_bytes(
                vec!["SET", "foo", "bar"].iter(),
                Some(&mut num_to_string),
            )))
            .await
            .unwrap();
        client
            .req_packed_command(black_box(redis::pack_command_to_bytes(
                vec!["SET", "baz", "foo"].iter(),
                Some(&mut num_to_string),
            )))
            .await
            .unwrap();
        let result = client
            .req_packed_command(black_box(redis::pack_command_to_bytes(
                vec!["MGET", "baz", "foo"].iter(),
                Some(&mut num_to_string),
            )))
            .await
            .unwrap();
        assert!(
            result
                == Value::Bulk(vec![
                    Value::Data(b"foo".to_vec()),
                    Value::Data(b"bar".to_vec())
                ])
        )
    });
}

main!(just_setup, send_message, send_and_receive_messages);
