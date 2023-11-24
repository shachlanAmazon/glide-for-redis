use bytes::{Bytes, BytesMut};
use integer_encoding::VarInt;

/// An object handling a arranging read buffers, and parsing the data in the buffers into requests.
pub struct RotatingBuffer {
    backing_buffer: BytesMut,
}

impl RotatingBuffer {
    pub fn new(buffer_size: usize) -> Self {
        Self {
            backing_buffer: BytesMut::with_capacity(buffer_size),
        }
    }

    /// Parses the requests in the buffer.
    pub fn get_requests(&mut self, mut func: impl FnMut(Bytes)) {
        let buffer = self.backing_buffer.split().freeze();
        let mut prev_position = 0;
        let buffer_len = buffer.len();
        while prev_position < buffer_len {
            if let Some((request_len, bytes_read)) = u32::decode_var(&buffer[prev_position..]) {
                let start_pos = prev_position + bytes_read;
                if (start_pos + request_len as usize) > buffer_len {
                    break;
                } else {
                    let slice = buffer.slice(start_pos..start_pos + request_len as usize);
                    prev_position += request_len as usize + bytes_read;
                    func(slice);
                }
            } else {
                break;
            }
        }

        if prev_position != buffer.len() {
            self.backing_buffer
                .extend_from_slice(&buffer[prev_position..]);
        }
    }

    pub fn current_buffer(&mut self) -> &mut BytesMut {
        &mut self.backing_buffer
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis_request::redis_request::{
        ArgsArrayBuilder, ArgsOptions, ArgsVecPointerBuilder, CommandBuilder, CommandOptions,
        RedisRequest, RedisRequestBuilder, RequestType,
    };
    use bytes::BufMut;
    use flatbuffers::FlatBufferBuilder;
    use rand::{distributions::Alphanumeric, Rng};
    use rstest::rstest;

    fn write_length(buffer: &mut BytesMut, length: u32) {
        let required_space = u32::required_space(length);
        let new_len = buffer.len() + required_space;
        buffer.resize(new_len, 0_u8);
        u32::encode_var(length, &mut buffer[new_len - required_space..]);
    }

    fn create_command_request<'a>(
        callback_index: u32,
        args: Vec<String>,
        request_type: RequestType,
        args_pointer: bool,
    ) -> FlatBufferBuilder<'a> {
        let mut builder = FlatBufferBuilder::new();
        let command_offset = if args_pointer {
            let mut args_builder = ArgsVecPointerBuilder::new(&mut builder);
            args_builder.add_pointer(Box::leak(Box::new(args)) as *mut Vec<String> as u64);
            let offset = args_builder.finish();
            let mut command_builder = CommandBuilder::new(&mut builder);
            command_builder.add_args(offset.as_union_value());
            command_builder.add_args_type(ArgsOptions::ArgsVecPointer);
            command_builder.add_request_type(request_type);
            command_builder.finish()
        } else {
            let pushed_args_vec: Vec<_> = args
                .into_iter()
                .map(|str| builder.create_string(&str))
                .collect();
            let offset = builder.create_vector_from_iter(pushed_args_vec.iter());
            let mut args_builder = ArgsArrayBuilder::new(&mut builder);
            args_builder.add_args(offset);
            let offset = args_builder.finish();
            let mut command_builder = CommandBuilder::new(&mut builder);
            command_builder.add_args(offset.as_union_value());
            command_builder.add_args_type(ArgsOptions::ArgsArray);
            command_builder.add_request_type(request_type);
            command_builder.finish()
        };

        let mut request_builder = RedisRequestBuilder::new(&mut builder);
        request_builder.add_command(command_offset.as_union_value());
        request_builder.add_callback_idx(callback_index);
        request_builder.add_command_type(CommandOptions::Command);
        let offset = request_builder.finish();
        builder.finish_minimal(offset);
        builder
    }

    fn write_message(
        buffer: &mut BytesMut,
        callback_index: u32,
        args: Vec<String>,
        request_type: RequestType,
        args_pointer: bool,
    ) {
        let builder = create_command_request(callback_index, args, request_type, args_pointer);

        let data = builder.finished_data();
        write_length(buffer, data.len() as u32);
        buffer.extend_from_slice(builder.finished_data());
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
        request: &Bytes,
        expected_type: RequestType,
        expected_index: u32,
        expected_args: Vec<String>,
        args_pointer: bool,
    ) {
        let request = flatbuffers::root::<RedisRequest>(request).unwrap();
        assert_eq!(request.callback_idx(), expected_index);
        let Some(command) = request.command_as_command() else {
            panic!("expected single command");
        };
        assert_eq!(command.request_type(), expected_type.into());
        let args: Vec<String> = if args_pointer {
            *unsafe {
                Box::from_raw(
                    command.args_as_args_vec_pointer().unwrap().pointer() as *mut Vec<String>
                )
            }
        } else {
            command
                .args_as_args_array()
                .unwrap()
                .args()
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

    fn get_requests(rotating_buffer: &mut RotatingBuffer) -> Vec<Bytes> {
        let mut requests = vec![];
        rotating_buffer.get_requests(|request| requests.push(request));
        requests
    }

    #[rstest]
    fn get_right_sized_buffer() {
        let mut rotating_buffer = RotatingBuffer::new(128);
        assert_eq!(rotating_buffer.current_buffer().capacity(), 128);
        assert_eq!(rotating_buffer.current_buffer().len(), 0);
    }

    #[rstest]
    fn get_all_requests(#[values(false, true)] args_pointer: bool) {
        const BUFFER_SIZE: usize = 50;
        let mut rotating_buffer = RotatingBuffer::new(BUFFER_SIZE);
        write_get(rotating_buffer.current_buffer(), 100, "key", args_pointer);
        write_set(
            rotating_buffer.current_buffer(),
            5,
            "key",
            "value".to_string(),
            args_pointer,
        );
        let requests = get_requests(&mut rotating_buffer);
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
        let mut rotating_buffer = RotatingBuffer::new(BUFFER_SIZE);
        write_get(rotating_buffer.current_buffer(), 100, "key", args_pointer);
        let requests = get_requests(&mut rotating_buffer);
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
        let requests = get_requests(&mut rotating_buffer);
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
        let mut rotating_buffer = RotatingBuffer::new(BUFFER_SIZE as usize);
        write_get(rotating_buffer.current_buffer(), 100, "key", false);

        let requests = get_requests(&mut rotating_buffer);
        assert_eq!(requests.len(), 1);
        assert_request(
            &requests[0],
            RequestType::GetString,
            100,
            vec!["key".to_string()],
            false,
        );

        while rotating_buffer.backing_buffer.len() < rotating_buffer.backing_buffer.capacity() {
            rotating_buffer.backing_buffer.put_u8(0_u8);
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
        let mut rotating_buffer = RotatingBuffer::new(24);
        write_get(rotating_buffer.current_buffer(), 100, "key1", args_pointer);

        let mut second_request_bytes = BytesMut::new();
        write_get(&mut second_request_bytes, 101, "key2", args_pointer);
        let buffer = rotating_buffer.current_buffer();
        buffer.extend_from_slice(&second_request_bytes[..NUM_OF_MESSAGE_BYTES]);
        let requests = get_requests(&mut rotating_buffer);
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
        let requests = get_requests(&mut rotating_buffer);
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
        let mut rotating_buffer = RotatingBuffer::new(24);
        let buffer = rotating_buffer.current_buffer();
        let key = generate_random_string(KEY_LENGTH);
        let mut request_bytes = BytesMut::new();
        write_get(&mut request_bytes, 100, key.as_str(), args_pointer);

        let required_varint_length = u32::required_space(KEY_LENGTH as u32);
        assert!(required_varint_length > 1); // so we could split the write of the varint
        buffer.extend_from_slice(&request_bytes[..NUM_OF_LENGTH_BYTES]);
        let requests = get_requests(&mut rotating_buffer);
        assert_eq!(requests.len(), 0);
        let buffer = rotating_buffer.current_buffer();
        buffer.extend_from_slice(&request_bytes[NUM_OF_LENGTH_BYTES..]);
        let requests = get_requests(&mut rotating_buffer);
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
        let mut rotating_buffer = RotatingBuffer::new(24);
        let key2 = generate_random_string(KEY_LENGTH);
        let required_varint_length = u32::required_space(KEY_LENGTH as u32);
        assert!(required_varint_length > 1); // so we could split the write of the varint
        write_get(rotating_buffer.current_buffer(), 100, "key1", args_pointer);
        let mut request_bytes = BytesMut::new();
        write_get(&mut request_bytes, 101, key2.as_str(), args_pointer);

        let buffer = rotating_buffer.current_buffer();
        buffer.extend_from_slice(&request_bytes[..NUM_OF_LENGTH_BYTES]);
        let requests = get_requests(&mut rotating_buffer);
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
        let requests = get_requests(&mut rotating_buffer);
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
