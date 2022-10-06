use napi::bindgen_prelude::ToNapiValue;
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{Error, JsFunction, Result, Status};
use napi_derive::napi;
use redis::aio::MultiplexedConnection;
use redis::socket_listener::headers::HEADER_END;
use redis::socket_listener::{start_socket_listener, ClosingReason};
use redis::{AsyncCommands, RedisError, RedisResult};
use std::str;
use tokio::task;

#[napi]
struct AsyncClient2Strings {}

#[napi]
struct AsyncClientReceiveString {}

#[napi]
struct AsyncClientReturnString {}

#[napi]
struct AsyncClient0String {}

#[napi]
impl AsyncClient2Strings {
    #[napi(js_name = "CreateConnection")]
    #[allow(dead_code)]
    pub async fn create_connection(connection_address: String) -> Result<AsyncClient2Strings> {
        Ok(Self {})
    }

    #[napi]
    #[allow(dead_code)]
    pub async fn get(&self, key: String) -> Result<Option<String>> {
        task::yield_now().await;
        Ok(Some(key))
    }

    #[napi]
    #[allow(dead_code)]
    pub async fn set(&self, key: String, value: String) -> Result<()> {
        task::yield_now().await;
        Ok(())
    }
}

#[napi]
impl AsyncClientReceiveString {
    #[napi(js_name = "CreateConnection")]
    #[allow(dead_code)]
    pub async fn create_connection(connection_address: String) -> Result<AsyncClientReceiveString> {
        Ok(Self {})
    }

    #[napi]
    #[allow(dead_code)]
    pub async fn get(&self) -> Result<Option<String>> {
        task::yield_now().await;
        Ok(None)
    }

    #[napi]
    #[allow(dead_code)]
    pub async fn set(&self) -> Result<()> {
        task::yield_now().await;
        Ok(())
    }
}

#[napi]
impl AsyncClientReturnString {
    #[napi(js_name = "CreateConnection")]
    #[allow(dead_code)]
    pub async fn create_connection(connection_address: String) -> Result<AsyncClientReturnString> {
        Ok(Self {})
    }

    #[napi]
    #[allow(dead_code)]
    pub async fn get(&self) -> Result<Option<String>> {
        task::yield_now().await;
        Ok(Some("YOLLO".to_owned()))
    }

    #[napi]
    #[allow(dead_code)]
    pub async fn set(&self) -> Result<()> {
        task::yield_now().await;
        Ok(())
    }
}

#[napi]
impl AsyncClient0String {
    #[napi(js_name = "CreateConnection")]
    #[allow(dead_code)]
    pub async fn create_connection(connection_address: String) -> Result<AsyncClient0String> {
        Ok(Self {})
    }

    #[napi]
    #[allow(dead_code)]
    pub async fn get(&self) -> Result<Option<String>> {
        task::yield_now().await;
        Ok(None)
    }

    #[napi]
    #[allow(dead_code)]
    pub async fn set(&self) -> Result<()> {
        task::yield_now().await;
        Ok(())
    }
}
