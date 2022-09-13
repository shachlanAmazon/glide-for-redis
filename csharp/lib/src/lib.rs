use num_derive::ToPrimitive;
use num_traits::ToPrimitive;
use redis::aio::MultiplexedConnection;
use redis::socket_listener::{start_socket_listener, ClosingReason};
use redis::{AsyncCommands, RedisResult};
use std::{
    ffi::{c_void, CStr, CString},
    os::raw::c_char,
};
use tokio::runtime::Builder;
use tokio::runtime::Runtime;

pub struct Connection {
    connection: MultiplexedConnection,
    success_callback: unsafe extern "C" fn(usize, *const c_char) -> (),
    failure_callback: unsafe extern "C" fn(usize) -> (), // TODO - add specific error codes
    runtime: Runtime,
}

fn create_connection_internal(
    address: *const c_char,
    success_callback: unsafe extern "C" fn(usize, *const c_char) -> (),
    failure_callback: unsafe extern "C" fn(usize) -> (),
) -> RedisResult<Connection> {
    let address_cstring = unsafe { CStr::from_ptr(address as *mut c_char) };
    let address_string = address_cstring.to_str()?;
    let client = redis::Client::open(address_string)?;
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .thread_name("Babushka C# thread")
        .build()?;
    let _runtime_handle = runtime.enter();
    let connection = runtime.block_on(client.get_multiplexed_async_connection())?; // TODO - log errors
    Ok(Connection {
        connection,
        success_callback,
        failure_callback,
        runtime,
    })
}

/// Creates a new connection to the given address. The success callback needs to copy the given string synchronously, since it will be dropped by Rust once the callback returns. All callbacks should be offloaded to separate threads in order not to exhaust the connection's thread pool.
/// TODO make this asynchronous.
#[no_mangle]
pub extern "C" fn create_connection(
    address: *const c_char,
    success_callback: unsafe extern "C" fn(usize, *const c_char) -> (),
    failure_callback: unsafe extern "C" fn(usize) -> (),
) -> *const c_void {
    match create_connection_internal(address, success_callback, failure_callback) {
        Err(_) => std::ptr::null(),
        Ok(connection) => Box::into_raw(Box::new(connection)) as *const c_void,
    }
}

#[no_mangle]
pub extern "C" fn close_connection(connection_ptr: *const c_void) {
    let connection_ptr = unsafe { Box::from_raw(connection_ptr as *mut Connection) };
    let _runtime_handle = connection_ptr.runtime.enter();
    drop(connection_ptr);
}

/// Expects that key and value will be kept valid until the callback is called.
#[no_mangle]
pub extern "C" fn set(
    connection_ptr: *const c_void,
    callback_index: usize,
    key: *const c_char,
    value: *const c_char,
) {
    let connection = unsafe { Box::leak(Box::from_raw(connection_ptr as *mut Connection)) };
    // The safety of this needs to be ensured by the calling code. Cannot dispose of the pointer before all operrations have completed.
    let ptr_address = connection_ptr as usize;

    let key_cstring = unsafe { CStr::from_ptr(key as *mut c_char) };
    let value_cstring = unsafe { CStr::from_ptr(value as *mut c_char) };
    let mut connection_clone = connection.connection.clone();
    connection.runtime.spawn(async move {
        let key_bytes = key_cstring.to_bytes();
        let value_bytes = value_cstring.to_bytes();
        let result: RedisResult<()> = connection_clone.set(key_bytes, value_bytes).await;
        unsafe {
            let connection = Box::leak(Box::from_raw(ptr_address as *mut Connection));
            match result {
                Ok(_) => (connection.success_callback)(callback_index, std::ptr::null()),
                Err(_) => (connection.failure_callback)(callback_index), // TODO - report errors
            };
        }
    });
}

/// Expects that key will be kept valid until the callback is called. If the callback is called with a string pointer, the pointer must
/// be used synchronously, because the string will be dropped after the callback.
#[no_mangle]
pub extern "C" fn get(connection_ptr: *const c_void, callback_index: usize, key: *const c_char) {
    let connection = unsafe { Box::leak(Box::from_raw(connection_ptr as *mut Connection)) };
    // The safety of this needs to be ensured by the calling code. Cannot dispose of the pointer before all operrations have completed.
    let ptr_address = connection_ptr as usize;

    let key_cstring = unsafe { CStr::from_ptr(key as *mut c_char) };
    let mut connection_clone = connection.connection.clone();
    connection.runtime.spawn(async move {
        let key_bytes = key_cstring.to_bytes();
        let result: RedisResult<Option<CString>> = connection_clone.get(key_bytes).await;

        unsafe {
            let connection = Box::leak(Box::from_raw(ptr_address as *mut Connection));
            match result {
                Ok(None) => (connection.success_callback)(callback_index, std::ptr::null()),
                Ok(Some(c_str)) => (connection.success_callback)(callback_index, c_str.as_ptr()),
                Err(_) => (connection.failure_callback)(callback_index), // TODO - report errors
            };
        }
    });
}

fn c_string_to_string(str: *const c_char) -> Option<String> {
    let c_str: &CStr = unsafe { CStr::from_ptr(str) };
    let str_slice: &str = c_str.to_str().ok()?;
    Some(str_slice.to_owned())
}

#[repr(C)]
#[derive(ToPrimitive)]
enum CloseReason {
    SocketClosed = 0,
    FailedParsingInputs = 1,
    CannotConnect = 2,
    UnknownError = 3,
    FailedInitialization = 4,
}

#[no_mangle]
pub extern "C" fn start_socket_listener_wrapper(
    address: *const c_char,
    read_socket_name: *const c_char,
    write_socket_name: *const c_char,
    start_callback: unsafe extern "C" fn() -> (),
    close_callback: unsafe extern "C" fn(usize) -> (),
) {
    let address = match c_string_to_string(address) {
        Some(str) => str,
        None => {
            println!("Cannot parse address");
            unsafe { close_callback(CloseReason::FailedParsingInputs.to_usize().unwrap()) };
            return;
        }
    };

    let client = match redis::Client::open(address) {
        Ok(client) => client,
        Err(err) => {
            println!(
                "Received error \"{:?}\" when trying to create a client",
                err
            );
            unsafe { close_callback(CloseReason::CannotConnect.to_usize().unwrap()) };
            return;
        }
    };
    let read_socket_name = match c_string_to_string(read_socket_name) {
        Some(str) => str,
        None => {
            println!("Cannot parse read_socket_name");
            unsafe { close_callback(CloseReason::FailedParsingInputs.to_usize().unwrap()) };
            return;
        }
    };
    let write_socket_name = match c_string_to_string(write_socket_name) {
        Some(str) => str,
        None => {
            println!("Cannot parse write_socket_name");
            unsafe { close_callback(CloseReason::FailedParsingInputs.to_usize().unwrap()) };
            return;
        }
    };
    start_socket_listener(
        client,
        read_socket_name,
        write_socket_name,
        move || unsafe {
            start_callback();
        },
        move |closing_reason| unsafe {
            match closing_reason {
                ClosingReason::ReadSocketClosed => {
                    close_callback(CloseReason::SocketClosed.to_usize().unwrap())
                }
                ClosingReason::UnhandledError(err) => {
                    println!("Received error \"{:?}\"", err);
                    close_callback(CloseReason::UnknownError.to_usize().unwrap())
                }
                ClosingReason::FailedInitialization(err) => {
                    println!("Received error \"{:?}\" during initialization", err);
                    close_callback(CloseReason::FailedInitialization.to_usize().unwrap())
                }
            }
        },
    );
}
