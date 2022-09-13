using System.Buffers;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;

namespace babushka
{
    public class AsyncSocketClient
    {
        #region public methods

        public static Task<AsyncSocketClient> CreateSocketClient(string address)
        {
            string tempDirectory = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
            Directory.CreateDirectory(tempDirectory);
            var readSocketName = Path.Combine(tempDirectory, "read");
            var writeSocketName = Path.Combine(tempDirectory, "write");

            var client = new AsyncSocketClient(address, readSocketName, writeSocketName);

            return client.completionSource.Task;
        }

        public async Task SetAsync(string key, string value)
        {
            var (message, task) = messageContainer.GetMessageForCall(key, value);
            await WriteToSocket(key, value, RequestType.SetString, message.Index);
            await task;
        }

        public async Task<string?> GetAsync(string key)
        {
            var (message, task) = messageContainer.GetMessageForCall(key, null);
            await WriteToSocket(key, null, RequestType.GetString, message.Index);
            return await task;
        }

        #endregion public methods

        #region private types


        // TODO - this repetition will become unmaintainable. We need to do this in macros.
        private enum RequestType
        {
            /// Type of a get string request.
            GetString = 1,
            /// Type of a set string request.
            SetString = 2,
        }

        // TODO - this repetition will become unmaintainable. We need to do this in macros.
        private enum ResponseType
        {
            /// Type of a response that returns a null.
            Null = 0,
            /// Type of a response that returns a string.
            String = 1,
        }

        // TODO - this repetition will become unmaintainable. We need to do this in macros.
        private const int HEADER_LENGTH_IN_BYTES = 12;

        #endregion private types

        #region private methods

        private AsyncSocketClient(string address, string readSocketName, string writeSocketName)
        {
            readServer = CreateServer(readSocketName);
            writeServer = CreateServer(writeSocketName);
            startCallbackDelegate = StartCallbackImpl;
            closeCallbackDelegate = CloseCallbackImpl;
            StartSocketListener(address,
                writeSocketName,
                readSocketName,
                Marshal.GetFunctionPointerForDelegate(startCallbackDelegate),
                Marshal.GetFunctionPointerForDelegate(closeCallbackDelegate));
            readSocket = AcceptSocket(readServer);
            writeSocket = AcceptSocket(writeServer);
            StartListeningOnReadSocket(readSocket, messageContainer);
        }

        ~AsyncSocketClient()
        {
            CloseConnections();
        }

        private void CloseConnections()
        {
            this.readServer.Dispose();
            this.writeServer.Dispose();
            this.readSocket.Dispose();
            this.writeSocket.Dispose();
        }

        private struct Header
        {
            internal UInt32 length;
            internal UInt32 callbackIndex;
            internal ResponseType responseType;
        }

        private static Header GetHeader(byte[] buffer, int position)
        {
            var span = MemoryMarshal.Cast<byte, UInt32>(new ReadOnlySpan<byte>(buffer, position, HEADER_LENGTH_IN_BYTES));
            return new Header
            {
                length = span[0],
                callbackIndex = span[1],
                responseType = (ResponseType)span[2]
            };
        }

        private static byte[] GetBuffer(ArraySegment<byte> previousBuffer)
        {
            var newBufferLength = 4096;
            if (previousBuffer.Count >= 4)
            {
                newBufferLength = MemoryMarshal.Read<int>(previousBuffer);
            }
            var newBuffer = ArrayPool<byte>.Shared.Rent(newBufferLength);
            if (previousBuffer.Array is not null)
            {
                Buffer.BlockCopy(previousBuffer.Array, previousBuffer.Offset, newBuffer, 0, previousBuffer.Count);
            }
            return newBuffer;
        }

        private static ArraySegment<byte> ParseReadResults(byte[] buffer, int messageLength, MessageContainer messageContainer)
        {
            var counter = 0;
            while (counter + HEADER_LENGTH_IN_BYTES <= messageLength)
            {
                var header = GetHeader(buffer, counter);
                if (header.length == 0)
                {
                    throw new ArgumentException("length 0");
                }
                if (counter + header.length > messageLength)
                {
                    return new ArraySegment<byte>(buffer, counter, messageLength - counter);
                }
                var message = messageContainer.GetMessage((int)header.callbackIndex);

                switch (header.responseType)
                {
                    case ResponseType.Null:
                        message.SetResult(null);
                        break;
                    case ResponseType.String:
                        var valueLength = header.length - HEADER_LENGTH_IN_BYTES;
                        message.SetResult(Encoding.UTF8.GetString(new Span<byte>(buffer,
                            (int)(counter + HEADER_LENGTH_IN_BYTES),
                            (int)valueLength
                        )));
                        break;
                }


                counter += (int)header.length;
                var offset = counter % 4;
                if (offset != 0)
                {
                    // align counter to 4.
                    counter += 4 - offset;
                }
            }

            return new ArraySegment<byte>(buffer, counter, messageLength - counter);
        }

        // this method is static, in order not to hold a reference to an AsyncSocketClient, so that it won't prevent GC and thus disposal.
        private static void StartListeningOnReadSocket(Socket readSocket, MessageContainer messageContainer)
        {
            Task.Run(async () =>
            {
                var previousSegment = new ArraySegment<byte>();
                while (readSocket.Connected)
                {
                    var buffer = GetBuffer(previousSegment);
                    var segmentAfterPreviousData = new ArraySegment<byte>(buffer, previousSegment.Count, buffer.Length - previousSegment.Count);
                    var receivedLength = await readSocket.ReceiveAsync(segmentAfterPreviousData, SocketFlags.None);
                    var newBuffer = ParseReadResults(buffer, receivedLength + previousSegment.Count, messageContainer);
                    if (previousSegment.Array is not null)
                    {
                        ArrayPool<byte>.Shared.Return(previousSegment.Array);
                    }
                    previousSegment = newBuffer;
                }
            });
        }

        private void StartCallbackImpl()
        {
            this.completionSource.SetResult(this);
        }

        private void CloseCallbackImpl(ulong code)
        {
            CloseConnections();
            this.completionSource.TrySetException(new Exception("Client failed starting"));
        }

        private Socket CreateServer(string path)
        {
            var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.IP);
            var endpoint = new UnixDomainSocketEndPoint(path);
            socket.Bind(endpoint);
            socket.Listen(1);
            return socket;
        }

        private Socket AcceptSocket(Socket server)
        {
            var socket = server.Accept();
            // TODO - make this configurable?
            socket.SendBufferSize = 2 ^ 22;
            socket.ReceiveBufferSize = 2 ^ 22;
            return socket;
        }

        private void WriteUint32ToBuffer(UInt32 value, byte[] target, int offset)
        {
            var encodedVal = BitConverter.GetBytes(value);
            Buffer.BlockCopy(encodedVal, 0, target, offset, encodedVal.Length);
        }

        private readonly SemaphoreSlim toLock = new(1, 1);
        private async Task WriteToSocket(string key, string? value, RequestType requestType, int callbackIndex)
        {
            var encoding = Encoding.UTF8;
            var headerLength = HEADER_LENGTH_IN_BYTES + ((value == null) ? 0 : 4);
            var maxLength = headerLength + key.Length * 3 + ((value == null) ? 0 : value.Length * 3);
            var buffer = ArrayPool<byte>.Shared.Rent(maxLength);
            var firstStringLength = encoding.GetBytes(key, 0, key.Length, buffer, (int)headerLength);
            var secondStringLength = (value == null) ? 0 :
                encoding.GetBytes(value, 0, value.Length, buffer, (int)headerLength + firstStringLength);
            var length = headerLength + firstStringLength + secondStringLength;
            WriteUint32ToBuffer((UInt32)length, buffer, 0);
            WriteUint32ToBuffer((UInt32)callbackIndex, buffer, 4);
            WriteUint32ToBuffer((UInt32)requestType, buffer, 8);
            if (value != null)
            {
                WriteUint32ToBuffer((UInt32)firstStringLength, buffer, HEADER_LENGTH_IN_BYTES);
            }

            await toLock.WaitAsync();
            var sentBytes = await this.writeSocket.SendAsync(new ArraySegment<byte>(buffer, 0, (int)length), SocketFlags.None);
            toLock.Release();
            if (sentBytes != length)
            {
                throw new Exception($"Wanted to write {length} bytes, actually wrote {sentBytes}");
            }
            ArrayPool<byte>.Shared.Return(buffer);
        }

        #endregion private methods

        #region private fields

        private readonly Socket readServer;
        private readonly Socket writeServer;
        private Socket readSocket;
        private Socket writeSocket;

        // The callback is being held by the client, in order to ensure that it isn't garbage collected.
        private readonly StartCallback startCallbackDelegate;
        // The callback is being held by the client, in order to ensure that it isn't garbage collected.
        private readonly CloseCallback closeCallbackDelegate;
        private readonly TaskCompletionSource<AsyncSocketClient> completionSource = new();
        private readonly MessageContainer messageContainer = new();

        #endregion private types

        #region rust bindings

        private delegate void StartCallback();
        private delegate void CloseCallback(ulong returnCode);
        [DllImport("libbabushka_csharp", CallingConvention = CallingConvention.Cdecl, EntryPoint = "start_socket_listener_wrapper")]
        private static extern void StartSocketListener(string address, string readSocketName, string writeSocketName, IntPtr startCallback, IntPtr closeCallback);

        #endregion
    }
}
