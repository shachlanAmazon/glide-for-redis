use crate::fake_multiplexer::FakeMultiplexer;

use super::{headers::*, rotating_buffer::RotatingBuffer};
use bytes::BufMut;
use num_traits::ToPrimitive;
use redis::RedisResult;
use redis::{Client, RedisError};
use std::cell::Cell;
use std::ops::Range;
use std::rc::Rc;
use std::str;
use std::{io, thread};
use tokio::runtime::Builder;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::sync::Notify;
use tokio::task;
use ClosingReason2::*;
use PipeListeningResult::*;

struct SocketListener {
    read_sender: Sender<oneshot::Sender<Vec<u8>>>,
    rotating_buffer: RotatingBuffer,
    values_written_notifier: Rc<Notify>,
}

enum PipeListeningResult {
    Closed(ClosingReason2),
    ReceivedValues(Vec<WholeRequest>),
}

impl From<ClosingReason2> for PipeListeningResult {
    fn from(result: ClosingReason2) -> Self {
        Closed(result)
    }
}

impl SocketListener {
    fn new(
        read_sender: Sender<oneshot::Sender<Vec<u8>>>,
        values_written_notifier: Rc<Notify>,
    ) -> Self {
        let rotating_buffer = RotatingBuffer::new(2, 65_536);
        SocketListener {
            read_sender,
            rotating_buffer,
            values_written_notifier,
        }
    }

    async fn request_read(&mut self) -> Result<Vec<u8>, ()> {
        let (sender, receiver) = oneshot::channel();
        self.read_sender.send(sender).await.map_err(|_| ())?;
        receiver.await.map_err(|_| ())
    }

    async fn next_values(&mut self) -> PipeListeningResult {
        loop {
            let Ok(received_values) = self.request_read().await else {
                return ReadSocketClosed.into();
            };

            self.rotating_buffer
                .current_buffer()
                .extend_from_slice(received_values.as_slice());

            return match self.rotating_buffer.get_requests() {
                Ok(requests) => ReceivedValues(requests),
                Err(err) => UnhandledError(err.into()).into(),
            };
        }
    }
}

fn write_response_header(
    accumulated_outputs: &Rc<Cell<Vec<u8>>>,
    callback_index: u32,
    response_type: ResponseType,
    length: usize,
) -> Result<(), io::Error> {
    let mut vec = accumulated_outputs.take();
    vec.put_u32_le(length as u32);
    vec.put_u32_le(callback_index);
    vec.put_u32_le(response_type.to_u32().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Response type {:?} wasn't found", response_type),
        )
    })?);

    assert!(!vec.is_empty());
    accumulated_outputs.set(vec);
    Ok(())
}

fn write_null_response_header(
    accumulated_outputs: &Rc<Cell<Vec<u8>>>,
    callback_index: u32,
) -> Result<(), io::Error> {
    write_response_header(
        accumulated_outputs,
        callback_index,
        ResponseType::Null,
        HEADER_END,
    )
}

fn write_slice_to_output(accumulated_outputs: &Cell<Vec<u8>>, bytes_to_write: &[u8]) {
    let mut vec = accumulated_outputs.take();
    vec.extend_from_slice(bytes_to_write);
    accumulated_outputs.set(vec);
}

async fn send_set_request(
    buffer: SharedBuffer,
    key_range: Range<usize>,
    value_range: Range<usize>,
    callback_index: u32,
    mut connection: FakeMultiplexer,
    accumulated_outputs: Rc<Cell<Vec<u8>>>,
    values_written_notifier: Rc<Notify>,
) -> RedisResult<()> {
    connection
        .set(&buffer[key_range], &buffer[value_range])
        .await?;
    write_null_response_header(&accumulated_outputs, callback_index)?;
    values_written_notifier.notify_one();
    Ok(())
}

async fn send_get_request(
    vec: SharedBuffer,
    key_range: Range<usize>,
    callback_index: u32,
    mut connection: FakeMultiplexer,
    accumulated_outputs: Rc<Cell<Vec<u8>>>,
    values_written_notifier: Rc<Notify>,
) -> RedisResult<()> {
    let result: Option<Vec<u8>> = connection.get(&vec[key_range]).await?;
    match result {
        Some(result_bytes) => {
            let length = HEADER_END + result_bytes.len();
            write_response_header(
                &accumulated_outputs,
                callback_index,
                ResponseType::String,
                length,
            )?;
            write_slice_to_output(&accumulated_outputs, &result_bytes);
        }
        None => {
            write_null_response_header(&accumulated_outputs, callback_index)?;
        }
    };

    values_written_notifier.notify_one();
    Ok(())
}

fn handle_request(
    request: WholeRequest,
    connection: FakeMultiplexer,
    accumulated_outputs: Rc<Cell<Vec<u8>>>,
    values_written_notifier: Rc<Notify>,
) {
    task::spawn_local(async move {
        let result = match request.request_type {
            RequestRanges::Get { key: key_range } => {
                send_get_request(
                    request.buffer,
                    key_range,
                    request.callback_index,
                    connection.clone(),
                    accumulated_outputs.clone(),
                    values_written_notifier.clone(),
                )
                .await
            }
            RequestRanges::Set {
                key: key_range,
                value: value_range,
            } => {
                send_set_request(
                    request.buffer,
                    key_range,
                    value_range,
                    request.callback_index,
                    connection.clone(),
                    accumulated_outputs.clone(),
                    values_written_notifier.clone(),
                )
                .await
            }
            RequestRanges::ServerAddress { address: _ } => {
                unreachable!("Server address can only be sent once")
            }
        };
        if let Err(err) = result {
            write_error(
                err,
                request.callback_index,
                ResponseType::RequestError,
                &accumulated_outputs,
                &values_written_notifier,
            )
            .await;
        }
    });
}

async fn write_error(
    err: RedisError,
    callback_index: u32,
    response_type: ResponseType,
    accumulated_outputs: &Rc<Cell<Vec<u8>>>,
    values_written_notifier: &Rc<Notify>,
) {
    let err_str = err.to_string();
    let error_bytes = err_str.as_bytes();
    let length = HEADER_END + error_bytes.len();
    write_response_header(&accumulated_outputs, callback_index, response_type, length)
        .expect("Failed writing error to vec");
    write_slice_to_output(&accumulated_outputs, err.to_string().as_bytes());

    values_written_notifier.notify_one();
}

async fn handle_requests(
    received_requests: Vec<WholeRequest>,
    connection: &FakeMultiplexer,
    accumulated_outputs: &Rc<Cell<Vec<u8>>>,
    values_written_notifier: &Rc<Notify>,
) {
    for request in received_requests {
        handle_request(
            request,
            connection.clone(),
            accumulated_outputs.clone(),
            values_written_notifier.clone(),
        );
    }
    // Yield to ensure that the subtasks aren't starved.
    task::yield_now().await;
}

fn to_babushka_result<T, E: std::fmt::Display>(
    result: Result<T, E>,
    err_msg: Option<&str>,
) -> Result<T, BabushkaError> {
    result.map_err(|err: E| {
        BabushkaError::BaseError(match err_msg {
            Some(msg) => format!("{}: {}", msg, err),
            None => format!("{}", err),
        })
    })
}

async fn parse_address_create_conn(
    request: &WholeRequest,
    address_range: Range<usize>,
    write_sender: &mut Sender<Vec<u8>>,
    accumulated_outputs: &Rc<Cell<Vec<u8>>>,
) -> Result<FakeMultiplexer, BabushkaError> {
    let address = &request.buffer[address_range];
    let address = to_babushka_result(
        std::str::from_utf8(address),
        Some("Failed to parse address"),
    )?;
    let _client = to_babushka_result(
        Client::open(address),
        Some("Failed to open redis-rs client"),
    )?;
    let connection = FakeMultiplexer {};

    write_null_response_header(accumulated_outputs, request.callback_index)
        .expect("Failed writing address response.");

    let vec = accumulated_outputs.take();
    to_babushka_result(write_sender.send(vec).await, None)?;

    Ok(connection)
}

async fn wait_for_server_address_create_conn(
    client_listener: &mut SocketListener,
    write_sender: &mut Sender<Vec<u8>>,
    accumulated_outputs: &Rc<Cell<Vec<u8>>>,
) -> Result<FakeMultiplexer, BabushkaError> {
    // Wait for the server's address
    let request = client_listener.next_values().await;
    match request {
        Closed(reason) => {
            return Err(BabushkaError::CloseError(reason));
        }
        ReceivedValues(received_requests) => {
            if let Some(index) = (0..received_requests.len()).next() {
                let request = received_requests
                    .get(index)
                    .ok_or_else(|| BabushkaError::BaseError("No received requests".to_string()))?;
                match request.request_type.clone() {
                    RequestRanges::ServerAddress {
                        address: address_range,
                    } => {
                        return parse_address_create_conn(
                            request,
                            address_range,
                            write_sender,
                            accumulated_outputs,
                        )
                        .await
                    }
                    _ => {
                        return Err(BabushkaError::BaseError(
                            "Received another request before receiving server address".to_string(),
                        ))
                    }
                }
            }
        }
    }
    Err(BabushkaError::BaseError(
        "Failed to get the server's address".to_string(),
    ))
}

async fn read_values(
    client_listener: &mut SocketListener,
    accumulated_outputs: &Rc<Cell<Vec<u8>>>,
    connection: FakeMultiplexer,
) -> Result<(), BabushkaError> {
    loop {
        match client_listener.next_values().await {
            Closed(reason) => {
                return Err(BabushkaError::CloseError(reason)); // TODO: implement error protocol, handle error closing reasons
            }
            ReceivedValues(received_requests) => {
                handle_requests(
                    received_requests,
                    &connection,
                    accumulated_outputs,
                    &client_listener.values_written_notifier,
                )
                .await;
            }
        }
    }
}

async fn write_accumulated_outputs(
    write_sender: &mut Sender<Vec<u8>>,
    accumulated_outputs: &Cell<Vec<u8>>,
    write_possible: &Rc<Notify>,
) -> Result<(), BabushkaError> {
    loop {
        // looping is required since notified() might have 2 permits - https://github.com/tokio-rs/tokio/pull/5305
        write_possible.notified().await;
        let vec = accumulated_outputs.take();
        // possible in case of 2 permits
        if vec.is_empty() {
            accumulated_outputs.set(vec);
            continue;
        }

        assert!(!vec.is_empty());
        to_babushka_result(write_sender.send(vec).await, None)?;
    }
}

async fn listen_on_client_stream(
    read_sender: Sender<oneshot::Sender<Vec<u8>>>,
    mut write_sender: Sender<Vec<u8>>,
) -> Result<(), BabushkaError> {
    let notifier = Rc::new(Notify::new());
    let mut client_listener = SocketListener::new(read_sender, notifier.clone());
    let accumulated_outputs = Rc::new(Cell::new(Vec::new()));
    let connection = wait_for_server_address_create_conn(
        &mut client_listener,
        &mut write_sender,
        &accumulated_outputs,
    )
    .await
    .unwrap();
    let result = tokio::try_join!(
        read_values(&mut client_listener, &accumulated_outputs, connection),
        write_accumulated_outputs(&mut write_sender, &accumulated_outputs, &notifier)
    )
    .map(|_| ());
    return result;
}

async fn connect(read_sender: Sender<oneshot::Sender<Vec<u8>>>, write_sender: Sender<Vec<u8>>) {
    let local = task::LocalSet::new();

    let _ = local
        .run_until(listen_on_client_stream(read_sender, write_sender))
        .await;
    println!("RS done listen_on_socket");
}

#[derive(Debug)]
/// Enum describing the reason that a socket listener stopped listening on a socket.
pub enum ClosingReason2 {
    /// The socket was closed. This is usually the required way to close the listener.
    ReadSocketClosed,
    /// The listener encounter an error it couldn't handle.
    UnhandledError(RedisError),
}

/// Enum describing babushka errors
#[derive(Debug)]
enum BabushkaError {
    /// Base error
    BaseError(String),
    /// Close error
    CloseError(ClosingReason2),
}

/// Creates a new thread with a main loop task listening on the socket for new connections.
/// Every new connection will be assigned with a client-listener task to handle their requests.
///
/// # Arguments
/// * `init_callback` - called when the socket listener fails to initialize, with the reason for the failure.
pub fn start_listener_internal(
    read_sender: Sender<oneshot::Sender<Vec<u8>>>,
    write_sender: Sender<Vec<u8>>,
) {
    println!("RS start start_listener");
    thread::Builder::new()
        .name("socket_like_listener_thread".to_string())
        .spawn(move || {
            Builder::new_current_thread()
                .enable_all()
                .thread_name("socket_like_listener_thread")
                .build()
                .unwrap()
                .block_on(connect(read_sender, write_sender));
        })
        .expect("Thread spawn failed. Cannot report error because callback was moved.");
}
