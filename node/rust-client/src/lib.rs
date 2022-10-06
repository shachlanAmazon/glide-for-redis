use napi::bindgen_prelude::{FromNapiValue, ToNapiValue};
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{Error, JsFunction, JsTypedArray, Result, Status};
use napi_derive::napi;
use redis::aio::MultiplexedConnection;
use redis::socket_listener::headers::HEADER_END;
use redis::socket_listener::{start_socket_listener, ClosingReason};
use redis::{AsyncCommands, RedisError, RedisResult};
use std::str;
use tokio::task;

struct RawSendPointer {
    send_pointer: usize,
}

impl RawSendPointer {
    fn as_ptr<T>(&self) -> *const T {
        self.send_pointer as *const T
    }
}

impl FromNapiValue for RawSendPointer {
    unsafe fn from_napi_value(
        env: napi::sys::napi_env,
        napi_val: napi::sys::napi_value,
    ) -> Result<Self> {
        let typed_array =
            <JsTypedArray as napi::bindgen_prelude::FromNapiValue>::from_napi_value(env, napi_val)?
                .into_value()?;
        let offset = typed_array.byte_offset;
        let pointr = typed_array.arraybuffer.into_value()?.as_ptr();
        Ok(RawSendPointer {
            send_pointer: pointr as usize + offset,
        })
    }
}

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
