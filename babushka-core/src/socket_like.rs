use bytes::BufMut;
use std::{
    cell::{Cell, RefCell},
    cmp::min,
    ops::{Deref, DerefMut},
    rc::Rc,
    thread,
};
use tokio::{
    join,
    runtime::Builder,
    sync::{
        mpsc::{channel, unbounded_channel, Receiver, UnboundedReceiver, UnboundedSender},
        oneshot, Notify,
    },
    task,
};

use crate::socket_like_listener::start_listener_internal;

///
pub struct SocketWriteRequest {
    buffer: Box<dyn Deref<Target = [u8]>>,
    completion: Box<dyn FnOnce(usize)>,
}

///
pub struct SocketReadRequest {
    buffer: Box<dyn DerefMut<Target = [u8]>>,
    completion: Box<dyn FnOnce((usize, usize))>,
}

impl SocketWriteRequest {
    ///
    pub fn new(buffer: Box<dyn Deref<Target = [u8]>>, completion: Box<dyn FnOnce(usize)>) -> Self {
        SocketWriteRequest { buffer, completion }
    }
}

impl SocketReadRequest {
    ///
    pub fn new(
        buffer: Box<dyn DerefMut<Target = [u8]>>,
        completion: Box<dyn FnOnce((usize, usize))>,
    ) -> Self {
        SocketReadRequest { buffer, completion }
    }
}

///
pub type ReadSender = UnboundedSender<SocketReadRequest>;

///
pub type WriteSender = UnboundedSender<SocketWriteRequest>;

async fn external_write_to_internal(
    external_write_request_receiver: UnboundedReceiver<SocketWriteRequest>,
    internal_read_request_receiver: Receiver<oneshot::Sender<Vec<u8>>>,
) {
    let write_possible = Rc::new(Notify::new());
    let data = Cell::new(Vec::<u8>::new());

    let write_to_internal =
        write_accumulated_inputs(internal_read_request_receiver, &data, &write_possible);
    let receive_data_from_external = receive_data_from_external(
        external_write_request_receiver,
        write_possible.clone(),
        &data,
    );
    let _ = join!(write_to_internal, receive_data_from_external);
}

async fn receive_data_from_external(
    mut external_write_request_receiver: UnboundedReceiver<SocketWriteRequest>,
    read_available: Rc<Notify>,
    data: &Cell<Vec<u8>>,
) {
    loop {
        let Some(read_request) = external_write_request_receiver.recv().await else {
            return;
        };
        let reference = read_request.buffer.as_ref().as_ref();
        assert!(!reference.is_empty());
        let mut accumulated_inputs = data.take();
        accumulated_inputs.extend_from_slice(reference);

        let completion = read_request.completion;
        completion(reference.len());
        data.set(accumulated_inputs);
        read_available.notify_one();
    }
}

async fn write_accumulated_inputs(
    mut internal_read_request_receiver: Receiver<oneshot::Sender<Vec<u8>>>,
    data: &Cell<Vec<u8>>,
    write_possible: &Notify,
) {
    loop {
        let Some(sender) = internal_read_request_receiver.recv().await else {
            return;
        };

        loop {
            write_possible.notified().await;
            let vec = data.take();
            // possible in case of 2 permits
            if vec.is_empty() {
                continue;
            }
            let _ = sender.send(vec);
            break;
        }
    }
}

async fn write_accumulated_outputs(
    mut read_request_receiver: UnboundedReceiver<SocketReadRequest>,
    accumulated_outputs: &RefCell<Vec<u8>>,
    read_possible: &Rc<Notify>,
) -> Result<(), ()> {
    loop {
        let Some(mut write_request) = read_request_receiver.recv().await else {
            return Err(());
        };
        // looping is required since notified() might have 2 permits - https://github.com/tokio-rs/tokio/pull/5305
        loop {
            read_possible.notified().await;
            let mut vec = accumulated_outputs.borrow_mut();
            // possible in case of 2 permits
            if vec.is_empty() {
                continue;
            }

            assert!(!vec.is_empty());
            let mut reference = write_request.buffer.as_mut().as_mut();

            let bytes_to_write = min(reference.len(), vec.len());
            reference.put(vec.drain(0..bytes_to_write).as_slice());
            if !vec.is_empty() {
                read_possible.notify_one();
            }
            let remaining_vec_len = vec.len();

            let completion = write_request.completion;
            completion((bytes_to_write, remaining_vec_len));
            break;
        }
    }
}

async fn external_read_from_internal(
    external_read_request_receiver: UnboundedReceiver<SocketReadRequest>,
    internal_write_request_receiver: Receiver<Vec<u8>>,
) {
    let read_possible = Rc::new(Notify::new());
    let data = RefCell::new(Vec::<u8>::new());

    let write_to_external =
        write_accumulated_outputs(external_read_request_receiver, &data, &read_possible);
    let receive_data_from_internal = receive_data(
        internal_write_request_receiver,
        read_possible.clone(),
        &data,
    );
    let _ = join!(write_to_external, receive_data_from_internal);
}

async fn receive_data(
    mut write_request_receiver: Receiver<Vec<u8>>,
    clone: Rc<Notify>,
    data: &RefCell<Vec<u8>>,
) {
    loop {
        let Some(new_data) = write_request_receiver.recv().await else {
            return;
        };
        assert!(!new_data.is_empty());

        let mut existing_data = data.borrow_mut();
        existing_data.extend_from_slice(new_data.as_slice());
        clone.notify_one();
    }
}

async fn listen_on_socket<InitCallback>(init_callback: InitCallback)
where
    InitCallback: FnOnce(Result<(WriteSender, ReadSender), String>) + Send + 'static,
{
    let local = task::LocalSet::new();
    let (external_read_request_sender, external_read_request_receiver) = unbounded_channel();
    let (external_write_request_sender, external_write_request_receiver) = unbounded_channel();
    let (internal_read_request_sender, internal_read_request_receiver) = channel(1);
    let (internal_write_request_sender, internal_write_request_receiver) = channel(1);
    start_listener_internal(internal_read_request_sender, internal_write_request_sender);
    init_callback(Ok((
        external_write_request_sender,
        external_read_request_sender,
    )));
    local.spawn_local(external_write_to_internal(
        external_write_request_receiver,
        internal_read_request_receiver,
    ));
    local.spawn_local(external_read_from_internal(
        external_read_request_receiver,
        internal_write_request_receiver,
    ));
    local.await;
}

pub fn start_listener<InitCallback>(init_callback: InitCallback)
where
    InitCallback: FnOnce(Result<(WriteSender, ReadSender), String>) + Send + 'static,
{
    println!("RS start start_listener");
    thread::Builder::new()
        .name("socket_like_thread".to_string())
        .spawn(move || {
            let runtime = Builder::new_current_thread().enable_all().build();
            match runtime {
                Ok(runtime) => {
                    runtime.block_on(listen_on_socket(init_callback));
                }
                Err(err) => init_callback(Err(err.to_string())),
            };
        })
        .expect("Thread spawn failed. Cannot report error because callback was moved.");
}
