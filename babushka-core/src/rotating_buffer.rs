use bytes::{Bytes, BytesMut};
use integer_encoding::VarInt;
use logger_core::log_error;
use protobuf::Message;
use std::{cell::RefCell, io, mem, rc::Rc};

type Pool = Rc<RefCell<Vec<BytesMut>>>;

struct RecycleableBuffer {
    pool: Pool,
    buffer: BytesMut,
}

impl Drop for RecycleableBuffer {
    fn drop(&mut self) {
        self.pool.borrow_mut().push(mem::take(&mut self.buffer));
    }
}

/// An object handling a arranging read buffers, and parsing the data in the buffers into requests.
pub(super) struct RotatingBuffer {
    /// Object pool for the internal buffers.
    pool: Pool,
    /// Buffer for next read request.
    current_read_buffer: RecycleableBuffer,
    buffer_size: usize,
}

fn draw_from_pool(pool: Pool, buffer_size: usize) -> RecycleableBuffer {
    let buffer = {
        let mut pool_ref = RefCell::borrow_mut(&pool);
        pool_ref
            .pop()
            .unwrap_or_else(|| BytesMut::with_capacity(buffer_size))
    };
    RecycleableBuffer { buffer, pool }
}

impl RotatingBuffer {
    pub(super) fn new(initial_buffers: usize, buffer_size: usize) -> Self {
        let pool = (0..initial_buffers)
            .into_iter()
            .map(|_| BytesMut::with_capacity(buffer_size))
            .collect();
        let pool = Rc::new(RefCell::new(pool));
        let current_read_buffer = draw_from_pool(pool.clone(), buffer_size);
        Self {
            current_read_buffer,
            buffer_size,
            pool,
        }
    }

    /// Adjusts the current buffer size so that it will fit required_length.
    fn match_capacity(&mut self, required_length: usize) {
        let extra_capacity = required_length - self.current_read_buffer.buffer.len();
        self.current_read_buffer.buffer.reserve(extra_capacity);
    }

    /// Replace the buffer, and copy the ending of the current incomplete message to the beginning of the next buffer.
    fn copy_from_old_buffer(
        &mut self,
        old_buffer: Bytes,
        required_length: Option<usize>,
        cursor: usize,
    ) {
        self.match_capacity(
            required_length.unwrap_or_else(|| self.current_read_buffer.buffer.capacity()),
        );
        let slice = &old_buffer[cursor..];
        debug_assert!(self.current_read_buffer.buffer.len() == 0);
        self.current_read_buffer.buffer.extend_from_slice(slice);
    }

    fn get_new_buffer(&mut self) -> RecycleableBuffer {
        let mut buffer = draw_from_pool(self.pool.clone(), self.buffer_size);
        buffer.buffer.clear();
        buffer
    }

    /// Parses the requests in the buffer.
    pub(super) fn get_requests<T: Message>(&mut self) -> io::Result<Vec<T>> {
        // We replace the buffer on every call, because we want to prevent the next read from affecting existing results.
        let new_buffer = self.get_new_buffer();
        let mut backing_buffer = mem::replace(&mut self.current_read_buffer, new_buffer);
        let buffer = backing_buffer.buffer.split().freeze();
        let mut results: Vec<T> = vec![];
        let mut prev_position = 0;
        let buffer_len = buffer.len();
        while prev_position < buffer_len {
            if let Some((request_len, bytes_read)) = u32::decode_var(&buffer[prev_position..]) {
                let start_pos = prev_position + bytes_read;
                if (start_pos + request_len as usize) > buffer_len {
                    break;
                } else {
                    match T::parse_from_bytes(
                        &buffer.slice(start_pos..start_pos + request_len as usize),
                    ) {
                        Ok(request) => {
                            prev_position += request_len as usize + bytes_read;
                            results.push(request);
                        }
                        Err(err) => {
                            log_error("parse input", format!("Failed to parse request: {err}"));
                            return Err(err.into());
                        }
                    }
                }
            } else {
                break;
            }
        }

        if prev_position != buffer.len() {
            self.copy_from_old_buffer(buffer, None, prev_position);
        } else {
            self.current_read_buffer.buffer.clear();
        }
        Ok(results)
    }

    pub(super) fn current_buffer(&mut self) -> &mut BytesMut {
        &mut self.current_read_buffer.buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis_request::{command, redis_request};
    use crate::redis_request::{Command, RedisRequest, RequestType};
    use bytes::BufMut;
    use rand::{distributions::Alphanumeric, Rng};
    use rstest::rstest;

    fn write_length(buffer: &mut BytesMut, length: u32) {
        let required_space = u32::required_space(length);
        let new_len = buffer.len() + required_space;
        buffer.resize(new_len, 0_u8);
        u32::encode_var(length, &mut buffer[new_len - required_space..]);
    }

    fn create_command_request(
        callback_index: u32,
        args: Vec<String>,
        request_type: RequestType,
        args_pointer: bool,
    ) -> RedisRequest {
        let mut request = RedisRequest::new();
        request.callback_idx = callback_index;
        let mut command = Command::new();
        command.request_type = request_type.into();
        if args_pointer {
            command.args = Some(command::Args::ArgsVecPointer(Box::leak(Box::new(args))
                as *mut Vec<String>
                as u64));
        } else {
            let mut args_array = command::ArgsArray::new();
            args_array.args = args;
            command.args = Some(command::Args::ArgsArray(args_array));
        }
        request.command = Some(redis_request::Command::SingleCommand(command));
        request
    }

    fn write_message(
        buffer: &mut BytesMut,
        callback_index: u32,
        args: Vec<String>,
        request_type: RequestType,
        args_pointer: bool,
    ) {
        let request = create_command_request(callback_index, args, request_type, args_pointer);
        let message_length = request.compute_size() as usize;
        write_length(buffer, message_length as u32);
        buffer.extend_from_slice(&request.write_to_bytes().unwrap());
    }

    fn write_get(buffer: &mut BytesMut, callback_index: u32, key: &str, args_pointer: bool) {
        write_message(
            buffer,
            callback_index,
            vec![key.to_string()],
            RequestType::GetString,
            args_pointer,
        );
    }

    fn write_set(
        buffer: &mut BytesMut,
        callback_index: u32,
        key: &str,
        value: String,
        args_pointer: bool,
    ) {
        write_message(
            buffer,
            callback_index,
            vec![key.to_string(), value],
            RequestType::SetString,
            args_pointer,
        );
    }

    fn assert_request(
        request: &RedisRequest,
        expected_type: RequestType,
        expected_index: u32,
        expected_args: Vec<String>,
        args_pointer: bool,
    ) {
        assert_eq!(request.callback_idx, expected_index);
        let Some(redis_request::Command::SingleCommand(ref command)) = request.command else {
            panic!("expected single command");
        };
        assert_eq!(command.request_type, expected_type.into());
        let args: Vec<String> = if args_pointer {
            *unsafe { Box::from_raw(command.args_vec_pointer() as *mut Vec<String>) }
        } else {
            command
                .args_array()
                .args
                .iter()
                .map(|chars| chars.to_string())
                .collect()
        };
        assert_eq!(args, expected_args);
    }

    fn generate_random_string(length: usize) -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect()
    }

    #[rstest]
    fn get_right_sized_buffer() {
        let mut rotating_buffer = RotatingBuffer::new(1, 128);
        assert_eq!(rotating_buffer.current_buffer().capacity(), 128);
        assert_eq!(rotating_buffer.current_buffer().len(), 0);
    }

    #[rstest]
    fn get_requests(#[values(false, true)] args_pointer: bool) {
        const BUFFER_SIZE: usize = 50;
        let mut rotating_buffer = RotatingBuffer::new(1, BUFFER_SIZE);
        write_get(rotating_buffer.current_buffer(), 100, "key", args_pointer);
        write_set(
            rotating_buffer.current_buffer(),
            5,
            "key",
            "value".to_string(),
            args_pointer,
        );
        let requests = rotating_buffer.get_requests().unwrap();
        assert_eq!(requests.len(), 2);
        assert_request(
            &requests[0],
            RequestType::GetString,
            100,
            vec!["key".to_string()],
            args_pointer,
        );
        assert_request(
            &requests[1],
            RequestType::SetString,
            5,
            vec!["key".to_string(), "value".to_string()],
            args_pointer,
        );
    }

    #[rstest]
    fn repeating_requests_from_same_buffer(#[values(false, true)] args_pointer: bool) {
        const BUFFER_SIZE: usize = 50;
        let mut rotating_buffer = RotatingBuffer::new(1, BUFFER_SIZE);
        write_get(rotating_buffer.current_buffer(), 100, "key", args_pointer);
        let requests = rotating_buffer.get_requests().unwrap();
        assert_request(
            &requests[0],
            RequestType::GetString,
            100,
            vec!["key".to_string()],
            args_pointer,
        );
        write_set(
            rotating_buffer.current_buffer(),
            5,
            "key",
            "value".to_string(),
            args_pointer,
        );
        let requests = rotating_buffer.get_requests().unwrap();
        assert_eq!(requests.len(), 1);
        assert_request(
            &requests[0],
            RequestType::SetString,
            5,
            vec!["key".to_string(), "value".to_string()],
            args_pointer,
        );
    }

    #[rstest]
    fn next_write_doesnt_affect_values() {
        const BUFFER_SIZE: u32 = 16;
        let mut rotating_buffer = RotatingBuffer::new(1, BUFFER_SIZE as usize);
        write_get(rotating_buffer.current_buffer(), 100, "key", false);

        let requests = rotating_buffer.get_requests().unwrap();
        assert_eq!(requests.len(), 1);
        assert_request(
            &requests[0],
            RequestType::GetString,
            100,
            vec!["key".to_string()],
            false,
        );

        while rotating_buffer.current_buffer().len() < rotating_buffer.current_buffer().capacity() {
            rotating_buffer.current_buffer().put_u8(0_u8);
        }
        assert_request(
            &requests[0],
            RequestType::GetString,
            100,
            vec!["key".to_string()],
            false,
        );
    }

    #[rstest]
    fn copy_full_message_and_a_second_length_with_partial_message_to_next_buffer(
        #[values(false, true)] args_pointer: bool,
    ) {
        const NUM_OF_MESSAGE_BYTES: usize = 2;
        let mut rotating_buffer = RotatingBuffer::new(1, 24);
        write_get(rotating_buffer.current_buffer(), 100, "key1", args_pointer);

        let mut second_request_bytes = BytesMut::new();
        write_get(&mut second_request_bytes, 101, "key2", args_pointer);
        let buffer = rotating_buffer.current_buffer();
        buffer.extend_from_slice(&second_request_bytes[..NUM_OF_MESSAGE_BYTES]);
        let requests = rotating_buffer.get_requests().unwrap();
        assert_eq!(requests.len(), 1);
        assert_request(
            &requests[0],
            RequestType::GetString,
            100,
            vec!["key1".to_string()],
            args_pointer,
        );
        let buffer = rotating_buffer.current_buffer();
        assert_eq!(buffer.len(), NUM_OF_MESSAGE_BYTES);
        buffer.extend_from_slice(&second_request_bytes[NUM_OF_MESSAGE_BYTES..]);
        let requests = rotating_buffer.get_requests().unwrap();
        assert_eq!(requests.len(), 1);
        assert_request(
            &requests[0],
            RequestType::GetString,
            101,
            vec!["key2".to_string()],
            args_pointer,
        );
    }

    #[rstest]
    fn copy_partial_length_to_buffer(#[values(false, true)] args_pointer: bool) {
        const NUM_OF_LENGTH_BYTES: usize = 1;
        const KEY_LENGTH: usize = 10000;
        let mut rotating_buffer = RotatingBuffer::new(1, 24);
        let buffer = rotating_buffer.current_buffer();
        let key = generate_random_string(KEY_LENGTH);
        let mut request_bytes = BytesMut::new();
        write_get(&mut request_bytes, 100, key.as_str(), args_pointer);

        let required_varint_length = u32::required_space(KEY_LENGTH as u32);
        assert!(required_varint_length > 1); // so we could split the write of the varint
        buffer.extend_from_slice(&request_bytes[..NUM_OF_LENGTH_BYTES]);
        let requests = rotating_buffer.get_requests::<RedisRequest>().unwrap();
        assert_eq!(requests.len(), 0);
        let buffer = rotating_buffer.current_buffer();
        buffer.extend_from_slice(&request_bytes[NUM_OF_LENGTH_BYTES..]);
        let requests = rotating_buffer.get_requests().unwrap();
        assert_eq!(requests.len(), 1);
        assert_request(
            &requests[0],
            RequestType::GetString,
            100,
            vec![key],
            args_pointer,
        );
    }

    #[rstest]
    fn copy_partial_length_to_buffer_after_a_full_message(
        #[values(false, true)] args_pointer: bool,
    ) {
        const NUM_OF_LENGTH_BYTES: usize = 1;
        const KEY_LENGTH: usize = 10000;
        let mut rotating_buffer = RotatingBuffer::new(1, 24);
        let key2 = generate_random_string(KEY_LENGTH);
        let required_varint_length = u32::required_space(KEY_LENGTH as u32);
        assert!(required_varint_length > 1); // so we could split the write of the varint
        write_get(rotating_buffer.current_buffer(), 100, "key1", args_pointer);
        let mut request_bytes = BytesMut::new();
        write_get(&mut request_bytes, 101, key2.as_str(), args_pointer);

        let buffer = rotating_buffer.current_buffer();
        buffer.extend_from_slice(&request_bytes[..NUM_OF_LENGTH_BYTES]);
        let requests = rotating_buffer.get_requests().unwrap();
        assert_eq!(requests.len(), 1);
        assert_request(
            &requests[0],
            RequestType::GetString,
            100,
            vec!["key1".to_string()],
            args_pointer,
        );
        let buffer = rotating_buffer.current_buffer();
        assert_eq!(buffer.len(), NUM_OF_LENGTH_BYTES);
        buffer.extend_from_slice(&request_bytes[NUM_OF_LENGTH_BYTES..]);
        let requests = rotating_buffer.get_requests().unwrap();
        assert_eq!(requests.len(), 1);
        assert_request(
            &requests[0],
            RequestType::GetString,
            101,
            vec![key2],
            args_pointer,
        );
    }
}
