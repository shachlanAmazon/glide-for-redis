use super::rotating_buffer::RotatingBuffer;
use crate::client::Client;
use crate::connection_request::connection_request::ConnectionRequest;
use crate::redis_request::redis_request::{
    ArgsOptions, Command, CommandOptions, RedisRequest, RequestType, Routes, SimpleRoutes,
    SlotTypes, Transaction,
};
use crate::response::response;
use crate::response::response::{
    ConstantResponse, ConstantResponseTableBuilder, PointerBuilder, RequestErrorBuilder, Response,
    ResponseBuilder, StringTable,
};
use crate::retry_strategies::get_fixed_interval_backoff;
use bytes::Bytes;
use directories::BaseDirs;
use dispose::{Disposable, Dispose};
use flatbuffers::FlatBufferBuilder;
use futures::stream::StreamExt;
use logger_core::{log_debug, log_error, log_info, log_trace, log_warn};
use redis::cluster_routing::{
    MultipleNodeRoutingInfo, Route, RoutingInfo, SingleNodeRoutingInfo, SlotAddr,
};
use redis::cluster_routing::{ResponsePolicy, Routable};
use redis::RedisError;
use redis::{cmd, Cmd, Value};
use signal_hook::consts::signal::*;
use signal_hook_tokio::Signals;
use std::cell::Cell;
use std::rc::Rc;
use std::{env, str};
use std::{io, thread};
use thiserror::Error;
use tokio::io::ErrorKind::AddrInUse;
use tokio::net::{UnixListener, UnixStream};
use tokio::runtime::Builder;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::Mutex;
use tokio::task;
use tokio_retry::Retry;
use tokio_util::task::LocalPoolHandle;
use ClosingReason::*;
use PipeListeningResult::*;

/// The socket file name
const SOCKET_FILE_NAME: &str = "babushka-socket";

/// The maximum length of a request's arguments to be passed as a vector of
/// strings instead of a pointer
pub const MAX_REQUEST_ARGS_LENGTH: usize = 2_i32.pow(12) as usize; // TODO: find the right number

/// struct containing all objects needed to bind to a socket and clean it.
struct SocketListener {
    socket_path: String,
    cleanup_socket: bool,
}

impl Dispose for SocketListener {
    fn dispose(self) {
        if self.cleanup_socket {
            // close_socket(&self.socket_path);
        }
    }
}

/// struct containing all objects needed to read from a unix stream.
struct UnixStreamListener {
    read_socket: Rc<UnixStream>,
    rotating_buffer: RotatingBuffer,
}

/// struct containing all objects needed to write to a socket.
struct Writer {
    socket: Rc<UnixStream>,
    lock: Mutex<()>,
    accumulated_outputs: Cell<Vec<u8>>,
    closing_sender: Sender<ClosingReason>,
}

enum PipeListeningResult {
    Closed(ClosingReason),
    ReceivedValues(()),
}

impl From<ClosingReason> for PipeListeningResult {
    fn from(result: ClosingReason) -> Self {
        Closed(result)
    }
}

impl UnixStreamListener {
    fn new(read_socket: Rc<UnixStream>) -> Self {
        // if the logger has been initialized by the user (external or internal) on info level this log will be shown
        log_debug("connection", "new socket listener initiated");
        let rotating_buffer = RotatingBuffer::new(65_536);
        Self {
            read_socket,
            rotating_buffer,
        }
    }

    pub(crate) async fn next_values(&mut self, func: impl FnMut(Bytes)) -> PipeListeningResult {
        loop {
            if let Err(err) = self.read_socket.readable().await {
                return ClosingReason::UnhandledError(err.into()).into();
            }

            let read_result = self
                .read_socket
                .try_read_buf(self.rotating_buffer.current_buffer());
            match read_result {
                Ok(0) => {
                    return ReadSocketClosed.into();
                }
                Ok(_) => {
                    self.rotating_buffer.get_requests(func);
                    return ReceivedValues(());
                }
                Err(ref e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::Interrupted =>
                {
                    continue;
                }
                Err(err) => return UnhandledError(err.into()).into(),
            }
        }
    }
}

async fn write_to_output(writer: &Rc<Writer>) {
    let Ok(_guard) = writer.lock.try_lock() else {
        return;
    };

    let mut output = writer.accumulated_outputs.take();
    loop {
        if output.is_empty() {
            return;
        }
        let mut total_written_bytes = 0;
        while total_written_bytes < output.len() {
            if let Err(err) = writer.socket.writable().await {
                let _res = writer.closing_sender.send(err.into()).await; // we ignore the error, because it means that the reader was dropped, which is ok.
                return;
            }
            match writer.socket.try_write(&output[total_written_bytes..]) {
                Ok(written_bytes) => {
                    total_written_bytes += written_bytes;
                }
                Err(err)
                    if err.kind() == io::ErrorKind::WouldBlock
                        || err.kind() == io::ErrorKind::Interrupted =>
                {
                    continue;
                }
                Err(err) => {
                    let _res = writer.closing_sender.send(err.into()).await; // we ignore the error, because it means that the reader was dropped, which is ok.
                }
            }
        }
        output.clear();
        output = writer.accumulated_outputs.replace(output);
    }
}

fn write_closing_error_to_builder<'a>(
    builder: &mut FlatBufferBuilder<'a>,
    error: String,
) -> flatbuffers::WIPOffset<StringTable<'a>> {
    let string_offset = builder.create_string(error.as_str());
    let mut message_builder = response::StringTableBuilder::new(builder);
    message_builder.add_string(string_offset);
    message_builder.finish()
}

async fn write_closing_error(
    err: ClosingError,
    callback_index: u32,
    writer: &Rc<Writer>,
) -> Result<(), io::Error> {
    let err = err.err_message;
    log_error("client creation", err.as_str());
    let vec = writer.accumulated_outputs.take();
    let mut builder = FlatBufferBuilder::from_vec(vec);
    let offset = write_closing_error_to_builder(&mut builder, err.to_string());
    let mut response_builder = ResponseBuilder::new(&mut builder);
    response_builder.add_callback_idx(callback_index);
    response_builder.add_value(offset.as_union_value());
    let offset = response_builder.finish();
    builder.finish_minimal(offset);
    let (vec, _size) = builder.collapse();
    assert_eq!(vec.len(), _size);
    writer.accumulated_outputs.set(vec);
    write_to_output(writer).await;
    Ok(())
}

/// Create response and write it to the writer
async fn write_result(
    resp_result: ClientUsageResult<Value>,
    callback_index: u32,
    writer: &Rc<Writer>,
) -> Result<(), io::Error> {
    let vec = writer.accumulated_outputs.take();
    let mut builder = FlatBufferBuilder::from_vec(vec);

    let offset = match resp_result {
        Ok(Value::Okay) => {
            let mut constant_value_builder = ConstantResponseTableBuilder::new(&mut builder);
            constant_value_builder.add_response(ConstantResponse::OK);
            constant_value_builder.finish().as_union_value()
        }
        Ok(value) => {
            let pointer = if value != Value::Nil {
                // Since null values don't require any additional data, they can be sent without any extra effort.
                // Move the value to the heap and leak it. The wrapper should use `Box::from_raw` to recreate the box, use the value, and drop the allocation.
                Box::leak(Box::new(value)) as *mut redis::Value as u64
            } else {
                0
            };
            let mut pointer_builder = PointerBuilder::new(&mut builder);
            pointer_builder.add_pointer(pointer);
            pointer_builder.finish().as_union_value()
        }
        Err(ClienUsageError::InternalError(error_message)) => {
            write_closing_error_to_builder(&mut builder, error_message).as_union_value()
        }
        Err(ClienUsageError::RedisError(err)) => {
            let error_message = err.to_string();
            if err.is_connection_refusal() {
                log_error("response error", &error_message);
                write_closing_error_to_builder(&mut builder, error_message).as_union_value()
            } else {
                log_warn("received error", error_message.as_str());
                let message_offset = builder.create_string(error_message.as_str());
                let mut request_error_builder = RequestErrorBuilder::new(&mut builder);
                request_error_builder.add_message(message_offset);
                request_error_builder.add_type_(if err.is_connection_dropped() {
                    response::RequestErrorType::Disconnect
                } else if err.is_timeout() {
                    response::RequestErrorType::Timeout
                } else {
                    match err.kind() {
                        redis::ErrorKind::ExecAbortError => response::RequestErrorType::ExecAbort,
                        _ => response::RequestErrorType::Unspecified,
                    }
                });
                request_error_builder.finish().as_union_value()
            }
        }
    };

    let mut response_builder = ResponseBuilder::new(&mut builder);
    response_builder.add_value(offset);
    response_builder.add_callback_idx(callback_index);
    response_builder.finish();
    builder.finish_minimal(offset);
    let (vec, _size) = builder.collapse();
    assert_eq!(vec.len(), _size);
    writer.accumulated_outputs.set(vec);
    write_to_output(writer).await;
    Ok(())
}

// async fn write_to_writer(response: Response, writer: &Rc<Writer>) -> Result<(), io::Error> {
//     let mut vec = writer.accumulated_outputs.take();
//     let encode_result = response.write_length_delimited_to_vec(&mut vec);

//     // Write the response' length to the buffer
//     match encode_result {
//         Ok(_) => {
//             writer.accumulated_outputs.set(vec);
//             write_to_output(writer).await;
//             Ok(())
//         }
//         Err(err) => {
//             let err_message = format!("failed to encode response: {err}");
//             log_error("response error", err_message.clone());
//             Err(std::io::Error::new(
//                 std::io::ErrorKind::InvalidInput,
//                 err_message,
//             ))
//         }
//     }
// }

fn get_two_word_command(first: &str, second: &str) -> Cmd {
    let mut cmd = cmd(first);
    cmd.arg(second);
    cmd
}

fn get_command(request: &Command) -> Option<Cmd> {
    let request_enum = request.request_type();
    match request_enum {
        RequestType::InvalidRequest => None,
        RequestType::CustomCommand => Some(Cmd::new()),
        RequestType::GetString => Some(cmd("GET")),
        RequestType::SetString => Some(cmd("SET")),
        RequestType::Ping => Some(cmd("PING")),
        RequestType::Info => Some(cmd("INFO")),
        RequestType::Del => Some(cmd("DEL")),
        RequestType::Select => Some(cmd("SELECT")),
        RequestType::ConfigGet => Some(get_two_word_command("CONFIG", "GET")),
        RequestType::ConfigSet => Some(get_two_word_command("CONFIG", "SET")),
        RequestType::ConfigResetStat => Some(get_two_word_command("CONFIG", "RESETSTAT")),
        RequestType::ConfigRewrite => Some(get_two_word_command("CONFIG", "REWRITE")),
        RequestType::ClientGetName => Some(get_two_word_command("CLIENT", "GETNAME")),
        RequestType::ClientGetRedir => Some(get_two_word_command("CLIENT", "GETREDIR")),
        RequestType::ClientId => Some(get_two_word_command("CLIENT", "ID")),
        RequestType::ClientInfo => Some(get_two_word_command("CLIENT", "INFO")),
        RequestType::ClientKill => Some(get_two_word_command("CLIENT", "KILL")),
        RequestType::ClientList => Some(get_two_word_command("CLIENT", "LIST")),
        RequestType::ClientNoEvict => Some(get_two_word_command("CLIENT", "NO-EVICT")),
        RequestType::ClientNoTouch => Some(get_two_word_command("CLIENT", "NO-TOUCH")),
        RequestType::ClientPause => Some(get_two_word_command("CLIENT", "PAUSE")),
        RequestType::ClientReply => Some(get_two_word_command("CLIENT", "REPLY")),
        RequestType::ClientSetInfo => Some(get_two_word_command("CLIENT", "SETINFO")),
        RequestType::ClientSetName => Some(get_two_word_command("CLIENT", "SETNAME")),
        RequestType::ClientUnblock => Some(get_two_word_command("CLIENT", "UNBLOCK")),
        RequestType::ClientUnpause => Some(get_two_word_command("CLIENT", "UNPAUSE")),
        RequestType::Expire => Some(cmd("EXPIRE")),
        RequestType::HashSet => Some(cmd("HSET")),
        RequestType::HashGet => Some(cmd("HGET")),
        RequestType::HashDel => Some(cmd("HDEL")),
        RequestType::HashExists => Some(cmd("HEXISTS")),
        RequestType::MSet => Some(cmd("MSET")),
        RequestType::MGet => Some(cmd("MGET")),
        RequestType::Incr => Some(cmd("INCR")),
        RequestType::IncrBy => Some(cmd("INCRBY")),
        RequestType::IncrByFloat => Some(cmd("INCRBYFLOAT")),
        RequestType::Decr => Some(cmd("DECR")),
        RequestType::DecrBy => Some(cmd("DECRBY")),
        RequestType::HashGetAll => Some(cmd("HGETALL")),
        RequestType::HashMSet => Some(cmd("HMSET")),
        RequestType::HashMGet => Some(cmd("HMGET")),
        RequestType::HashIncrBy => Some(cmd("HINCRBY")),
        RequestType::HashIncrByFloat => Some(cmd("HINCRBYFLOAT")),
        RequestType::LPush => Some(cmd("LPUSH")),
        RequestType::LPop => Some(cmd("LPOP")),
        RequestType::RPush => Some(cmd("RPUSH")),
        RequestType::RPop => Some(cmd("RPOP")),
        RequestType::LLen => Some(cmd("LLEN")),
        RequestType::LRem => Some(cmd("LREM")),
        RequestType::LRange => Some(cmd("LRANGE")),
        RequestType::LTrim => Some(cmd("LTRIM")),
        RequestType::SAdd => Some(cmd("SADD")),
        RequestType::SRem => Some(cmd("SREM")),
        RequestType::SMembers => Some(cmd("SMEMBERS")),
        RequestType::SCard => Some(cmd("SCARD")),
        RequestType::PExpireAt => Some(cmd("PEXPIREAT")),
        RequestType::PExpire => Some(cmd("PEXPIRE")),
        RequestType::ExpireAt => Some(cmd("EXPIREAT")),
        RequestType::Exists => Some(cmd("EXISTS")),
        RequestType::Unlink => Some(cmd("UNLINK")),
        RequestType::TTL => Some(cmd("TTL")),
        _ => panic!("unknown enum"), // TODO
    }
}

fn get_redis_command(command: &Command) -> Result<Cmd, ClienUsageError> {
    let Some(mut cmd) = get_command(command) else {
        return Err(ClienUsageError::InternalError(format!(
            "Received invalid request type: {:?}",
            command.request_type()
        )));
    };

    match command.args_type() {
        ArgsOptions::ArgsArray => {
            let args_array = command.args_as_args_array().unwrap().args();
            for arg in args_array.iter() {
                cmd.arg(arg.as_bytes());
            }
        }
        ArgsOptions::ArgsVecPointer => {
            let pointer = command.args_as_args_vec_pointer().unwrap().pointer();
            let res = *unsafe { Box::from_raw(pointer as *mut Vec<String>) };
            for arg in res {
                cmd.arg(arg.as_bytes());
            }
        }
        ArgsOptions::NONE => {
            return Err(ClienUsageError::InternalError(
                "Failed to get request arguemnts, no arguments are set".to_string(),
            ));
        }
        _ => panic!("unknown enum"), // TODO
    };

    Ok(cmd)
}

async fn send_command(
    cmd: Cmd,
    mut client: Client,
    routing: Option<RoutingInfo>,
) -> ClientUsageResult<Value> {
    client
        .req_packed_command(&cmd, routing)
        .await
        .map_err(|err| err.into())
}

async fn send_transaction(
    mut pipeline: redis::Pipeline,
    mut client: Client,
    routing: Option<RoutingInfo>,
) -> ClientUsageResult<Value> {
    let offset = pipeline.cmd_iter().count();
    pipeline.atomic();

    client
        .req_packed_commands(&pipeline, offset, 1, routing)
        .await
        .map(|mut values| values.pop().unwrap_or(Value::Nil))
        .map_err(|err| err.into())
}

fn get_slot_addr(slot_type: SlotTypes) -> ClientUsageResult<SlotAddr> {
    match slot_type {
        SlotTypes::Primary => Ok(SlotAddr::Master),
        SlotTypes::Replica => Ok(SlotAddr::ReplicaRequired),
        // id => ClienUsageError::InternalError(format!("Received unexpected slot id type {id}")),
        _ => panic!("unknown enum"), // TODO
    }
}

fn get_route(
    request: &RedisRequest,
    response_policy: Option<ResponsePolicy>,
) -> ClientUsageResult<Option<RoutingInfo>> {
    match request.route_type() {
        Routes::SimpleRoutesTable => {
            let simple_route = request.route_as_simple_routes_table().unwrap().route();
            match simple_route {
                SimpleRoutes::AllNodes => Ok(Some(RoutingInfo::MultiNode((
                    MultipleNodeRoutingInfo::AllNodes,
                    response_policy,
                )))),
                SimpleRoutes::AllPrimaries => Ok(Some(RoutingInfo::MultiNode((
                    MultipleNodeRoutingInfo::AllMasters,
                    response_policy,
                )))),
                SimpleRoutes::Random => {
                    Ok(Some(RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random)))
                }
                _ => panic!("unknown enum"), // TODO
            }
        }
        Routes::SlotIdRoute => {
            let slot_id_route = request.route_as_slot_id_route().unwrap();
            Ok(Some(RoutingInfo::SingleNode(
                SingleNodeRoutingInfo::SpecificNode(Route::new(
                    slot_id_route.slot_id() as u16,
                    get_slot_addr(slot_id_route.slot_type())?,
                )),
            )))
        }
        Routes::SlotKeyRoute => {
            let slot_key_route = request.route_as_slot_key_route().unwrap();
            Ok(Some(RoutingInfo::SingleNode(
                SingleNodeRoutingInfo::SpecificNode(Route::new(
                    redis::cluster_topology::get_slot(slot_key_route.slot_key().as_bytes()),
                    get_slot_addr(slot_key_route.slot_type())?,
                )),
            )))
        }
        _ => panic!("unknown enum"), // TODO
    }
}

fn handle_request(request: RedisRequest, client: Client, writer: Rc<Writer>) {
    let callback_idx = request.callback_idx();
    match request.command_type() {
        CommandOptions::Command => {
            let command = request.command_as_command().unwrap();
            match get_redis_command(&command) {
                Ok(cmd) => {
                    let response_policy = cmd
                        .command()
                        .map(|cmd| ResponsePolicy::for_command(&cmd))
                        .unwrap_or(None);

                    match get_route(&request, response_policy) {
                        Ok(routes) => {
                            task::spawn_local(async move {
                                let result = send_command(cmd, client, routes).await;
                                let _res = write_result(result, callback_idx, &writer).await;
                            });
                        }
                        Err(e) => {} //TODO
                    };
                }
                Err(e) => {} //TODO
            }
        }
        CommandOptions::Transaction => {
            let transaction = request.command_as_transaction().unwrap();
            match get_route(&request, None) {
                Ok(routes) => {
                    let commands = transaction.commands();
                    let mut pipeline = redis::Pipeline::with_capacity(commands.len());
                    for command in commands {
                        pipeline.add_command(get_redis_command(&command).unwrap());
                        // TODO handle unwrap
                    }
                    task::spawn_local(async move {
                        let result = send_transaction(pipeline, client, routes).await;
                        let _res = write_result(result, callback_idx, &writer).await;
                    });
                }
                Err(e) => {} //TODO
            }
        }
        _ => panic!("unknown enum"), // TODO
    };
}

pub fn close_socket(socket_path: &String) {
    log_info("close_socket", format!("closing socket at {socket_path}"));
    let _ = std::fs::remove_file(socket_path);
}

async fn create_client<'a>(
    writer: &Rc<Writer>,
    request: ConnectionRequest<'a>,
) -> Result<Client, ClientCreationError> {
    let client = match Client::new(request).await {
        Ok(client) => client,
        Err(err) => return Err(ClientCreationError::ConnectionError(err)),
    };
    write_result(Ok(Value::Nil), 0, writer).await?;
    Ok(client)
}

async fn wait_for_connection_configuration_and_create_client(
    client_listener: &mut UnixStreamListener,
    writer: &Rc<Writer>,
) -> Result<Client, ClientCreationError> {
    let mut connection_request_bytes: Option<Bytes> = None;
    // Wait for the server's address
    match client_listener
        .next_values(|request| connection_request_bytes = Some(request))
        .await
    {
        Closed(reason) => Err(ClientCreationError::SocketListenerClosed(reason)),
        ReceivedValues(()) => {
            if let Some(request) = connection_request_bytes {
                match flatbuffers::root::<ConnectionRequest>(&request) {
                    Ok(connection_request) => create_client(writer, connection_request).await,
                    Err(err) => Err(ClientCreationError::UnhandledError(format!(
                        "Couldn't parse connection request: `{}`",
                        err.to_string()
                    ))),
                }
            } else {
                Err(ClientCreationError::UnhandledError(
                    "No received requests".to_string(),
                ))
            }
        }
    }
}

async fn read_values_loop(
    mut client_listener: UnixStreamListener,
    client: &Client,
    writer: Rc<Writer>,
) -> ClosingReason {
    loop {
        match client_listener
            .next_values(|bytes| {
                let request = flatbuffers::root::<RedisRequest>(&bytes).unwrap();
                handle_request(request, client.clone(), writer.clone())
            })
            .await
        {
            Closed(reason) => {
                return reason;
            }
            ReceivedValues(()) => {
                // Yield to ensure that the subtasks aren't starved.
                task::yield_now().await;
            }
        }
    }
}

async fn listen_on_client_stream(socket: UnixStream) {
    let socket = Rc::new(socket);
    // Spawn a new task to listen on this client's stream
    let write_lock = Mutex::new(());
    let mut client_listener = UnixStreamListener::new(socket.clone());
    let accumulated_outputs = Cell::new(Vec::new());
    let (sender, mut receiver) = channel(1);
    let writer = Rc::new(Writer {
        socket,
        lock: write_lock,
        accumulated_outputs,
        closing_sender: sender,
    });
    let client_creation =
        wait_for_connection_configuration_and_create_client(&mut client_listener, &writer);
    let client = match client_creation.await {
        Ok(conn) => conn,
        Err(ClientCreationError::SocketListenerClosed(ClosingReason::ReadSocketClosed)) => {
            // This isn't an error - it can happen when a new wrapper-client creates a connection in order to check whether something already listens on the socket.
            log_debug(
                "client creation",
                "read socket closed before client was created.",
            );
            return;
        }
        Err(ClientCreationError::SocketListenerClosed(reason)) => {
            let err_message = format!("Socket listener closed due to {reason:?}");
            let _res = write_closing_error(ClosingError { err_message }, u32::MAX, &writer).await;
            return;
        }
        Err(e @ ClientCreationError::UnhandledError(_))
        | Err(e @ ClientCreationError::IO(_))
        | Err(e @ ClientCreationError::ConnectionError(_)) => {
            let err_message = e.to_string();
            let _res = write_closing_error(ClosingError { err_message }, u32::MAX, &writer).await;
            return;
        }
    };
    log_info("connection", "new connection started");
    tokio::select! {
            reader_closing = read_values_loop(client_listener, &client, writer.clone()) => {
                if let ClosingReason::UnhandledError(err) = reader_closing {
                    let _res = write_closing_error(ClosingError{err_message: err.to_string()}, u32::MAX, &writer).await;
                };
                log_trace("client closing", "reader closed");
            },
            writer_closing = receiver.recv() => {
                if let Some(ClosingReason::UnhandledError(err)) = writer_closing {
                    log_error("client closing", format!("Writer closed with error: {err}"));
                } else {
                    log_trace("client closing", "writer closed");
                }
            }
    }
    log_trace("client closing", "closing connection");
}

enum SocketCreationResult {
    // Socket creation was successful, returned a socket listener.
    Created(UnixListener),
    // There's an existing a socket listener.
    PreExisting,
    // Socket creation failed with an error.
    Err(io::Error),
}

impl SocketListener {
    fn new(socket_path: String) -> Self {
        SocketListener {
            socket_path,
            // Don't cleanup the socket resources unless we know that the socket is in use, and owned by this listener.
            cleanup_socket: false,
        }
    }

    /// Return true if it's possible to connect to socket.
    async fn socket_is_available(&self) -> bool {
        if UnixStream::connect(&self.socket_path).await.is_ok() {
            return true;
        }

        let retry_strategy = get_fixed_interval_backoff(10, 3);

        let action = || async {
            UnixStream::connect(&self.socket_path)
                .await
                .map(|_| ())
                .map_err(|_| ())
        };
        let result = Retry::spawn(retry_strategy.get_iterator(), action).await;
        result.is_ok()
    }

    async fn get_socket_listener(&self) -> SocketCreationResult {
        const RETRY_COUNT: u8 = 3;
        let mut retries = RETRY_COUNT;
        while retries > 0 {
            match UnixListener::bind(self.socket_path.clone()) {
                Ok(listener) => {
                    return SocketCreationResult::Created(listener);
                }
                Err(err) if err.kind() == AddrInUse => {
                    if self.socket_is_available().await {
                        return SocketCreationResult::PreExisting;
                    } else {
                        // socket file might still exist, even if nothing is listening on it.
                        // close_socket(&self.socket_path);
                        retries -= 1;
                        continue;
                    }
                }
                Err(err) => {
                    return SocketCreationResult::Err(err);
                }
            }
        }
        SocketCreationResult::Err(io::Error::new(
            io::ErrorKind::Other,
            "Failed to connect to socket",
        ))
    }

    pub(crate) async fn listen_on_socket<InitCallback>(&mut self, init_callback: InitCallback)
    where
        InitCallback: FnOnce(Result<String, String>) + Send + 'static,
    {
        // Bind to socket
        let listener = match self.get_socket_listener().await {
            SocketCreationResult::Created(listener) => listener,
            SocketCreationResult::Err(err) => {
                log_info("listen_on_socket", format!("failed with error: {err}"));
                init_callback(Err(err.to_string()));
                return;
            }
            SocketCreationResult::PreExisting => {
                init_callback(Ok(self.socket_path.clone()));
                return;
            }
        };

        self.cleanup_socket = true;
        init_callback(Ok(self.socket_path.clone()));
        let local_set_pool = LocalPoolHandle::new(num_cpus::get());
        loop {
            tokio::select! {
                listen_v = listener.accept() => {
                    if let Ok((stream, _addr)) = listen_v {
                        // New client
                        local_set_pool.spawn_pinned(move || {
                            listen_on_client_stream(stream)
                        });
                    } else if listen_v.is_err() {
                        return
                    }
                },
                // Interrupt was received, close the socket
                _ = handle_signals() => return
            }
        }
    }
}

#[derive(Debug)]
/// Enum describing the reason that a socket listener stopped listening on a socket.
pub enum ClosingReason {
    /// The socket was closed. This is the expected way that the listener should be closed.
    ReadSocketClosed,
    /// The listener encounter an error it couldn't handle.
    UnhandledError(RedisError),
}

impl From<io::Error> for ClosingReason {
    fn from(error: io::Error) -> Self {
        UnhandledError(error.into())
    }
}

/// Enum describing errors received during client creation.
#[derive(Debug, Error)]
enum ClientCreationError {
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),
    /// An error was returned during the client creation process.
    #[error("Unhandled error: {0}")]
    UnhandledError(String),
    /// Socket listener was closed before receiving the server address.
    #[error("Closing error: {0:?}")]
    SocketListenerClosed(ClosingReason),
    #[error("Connection error: {0:?}")]
    ConnectionError(crate::client::ConnectionError),
}

/// Enum describing errors received during client usage.
#[derive(Debug, Error)]
enum ClienUsageError {
    #[error("Redis error: {0}")]
    RedisError(#[from] RedisError),
    /// An error that stems from wrong behavior of the client.
    #[error("Internal error: {0}")]
    InternalError(String),
}

type ClientUsageResult<T> = Result<T, ClienUsageError>;

/// Defines errors caused the connection to close.
#[derive(Debug, Clone)]
struct ClosingError {
    /// A string describing the closing reason
    err_message: String,
}

/// Get the socket full path.
/// The socket file name will contain the process ID and will try to be saved into the user's runtime directory
/// (e.g. /run/user/1000) in Unix systems. If the runtime dir isn't found, the socket file will be saved to the temp dir.
/// For Windows, the socket file will be saved to %AppData%\Local.
pub fn get_socket_path_from_name(socket_name: String) -> String {
    let base_dirs = BaseDirs::new().expect("Failed to create BaseDirs");
    let tmp_dir;
    let folder = if cfg!(windows) {
        base_dirs.data_local_dir()
    } else {
        base_dirs.runtime_dir().unwrap_or({
            tmp_dir = env::temp_dir();
            tmp_dir.as_path()
        })
    };
    folder
        .join(socket_name)
        .into_os_string()
        .into_string()
        .expect("Couldn't create socket path")
}

/// Get the socket path as a string
pub fn get_socket_path() -> String {
    let socket_name = format!("{}-{}", SOCKET_FILE_NAME, std::process::id());
    get_socket_path_from_name(socket_name)
}

async fn handle_signals() {
    // Handle Unix signals
    let mut signals =
        Signals::new([SIGTERM, SIGQUIT, SIGINT, SIGHUP]).expect("Failed creating signals");
    loop {
        if let Some(signal) = signals.next().await {
            match signal {
                SIGTERM | SIGQUIT | SIGINT | SIGHUP => {
                    log_info("connection", format!("Signal {signal:?} received"));
                    return;
                }
                _ => continue,
            }
        }
    }
}

/// This function is exposed only for the sake of testing with a nonstandard `socket_path`.
/// Avoid using this function, unless you explicitly want to test the behavior of the listener
/// without using the sockets used by other tests.
pub fn start_socket_listener_internal<InitCallback>(
    init_callback: InitCallback,
    socket_path: Option<String>,
) where
    InitCallback: FnOnce(Result<String, String>) + Send + 'static,
{
    thread::Builder::new()
        .name("socket_listener_thread".to_string())
        .spawn(move || {
            let runtime = Builder::new_current_thread().enable_all().build();
            match runtime {
                Ok(runtime) => {
                    let mut listener = Disposable::new(SocketListener::new(
                        socket_path.unwrap_or_else(get_socket_path),
                    ));
                    runtime.block_on(listener.listen_on_socket(init_callback));
                }
                Err(err) => init_callback(Err(err.to_string())),
            };
        })
        .expect("Thread spawn failed. Cannot report error because callback was moved.");
}

/// Creates a new thread with a main loop task listening on the socket for new connections.
/// Every new connection will be assigned with a client-listener task to handle their requests.
///
/// # Arguments
/// * `init_callback` - called when the socket listener fails to initialize, with the reason for the failure.
pub fn start_socket_listener<InitCallback>(init_callback: InitCallback)
where
    InitCallback: FnOnce(Result<String, String>) + Send + 'static,
{
    start_socket_listener_internal(init_callback, None);
}
