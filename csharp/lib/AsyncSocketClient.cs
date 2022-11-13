using System;
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

        public static async Task<AsyncSocketClient> CreateSocketClient(string address)
        {
            // Console.WriteLine("start create client");
            var completionSource = new TaskCompletionSource<string>();
            InitCallback initCallback = (IntPtr successPointer, IntPtr errorPointer) =>
            {
                if (successPointer != IntPtr.Zero)
                {
                    var address = Marshal.PtrToStringAnsi(successPointer);
                    if (address is not null)
                    {
                        completionSource.SetResult(address);
                    }
                    else
                    {
                        completionSource.SetException(new Exception("Received address that couldn't be converted to string"));
                    }
                }
                else if (errorPointer != IntPtr.Zero)
                {
                    var errorMessage = Marshal.PtrToStringAnsi(errorPointer);
                    completionSource.SetException(new Exception(errorMessage));
                }
                else
                {
                    completionSource.SetException(new Exception("Did not receive results from init callback"));
                }
            };
            var callbackPointer = Marshal.GetFunctionPointerForDelegate(initCallback);
            StartSocketListener(callbackPointer);

            var socketName = await completionSource.Task;
            // Console.WriteLine("got socket name");

            return new AsyncSocketClient(socketName, address);
        }

        public async Task SetAsync(string key, string value)
        {
            // Console.WriteLine("set start");
            var socket = await GetSocketAsync();
            // Console.WriteLine("set get socket");
            await WriteToSocketAsync(socket, key, value, RequestType.SetString);
            // Console.WriteLine("set send message");
            await GetResponseAsync(socket);
            // Console.WriteLine("set receive message");
            availableSockets.Enqueue(socket);
        }

        public async Task<string?> GetAsync(string key)
        {
            // Console.WriteLine("get start");
            var socket = await GetSocketAsync();
            // Console.WriteLine("get get socket");
            await WriteToSocketAsync(socket, key, null, RequestType.GetString);
            // Console.WriteLine("get send message");
            var result = await GetResponseAsync(socket);
            // Console.WriteLine("get receive message");
            availableSockets.Enqueue(socket);
            return result;
        }

        #endregion public methods

        #region private types

        // TODO - this repetition will become unmaintainable. We need to do this in macros.
        private enum RequestType
        {
            /// Type of a set server address request. This request should happen once, when the socket connection is initialized.
            SetServerAddress = 1,
            /// Type of a get string request.
            GetString = 2,
            /// Type of a set string request.
            SetString = 3,
        }

        // TODO - this repetition will become unmaintainable. We need to do this in macros.
        private enum ResponseType
        {
            /// Type of a response that returns a null.
            Null = 0,
            /// Type of a response that returns a string.
            String = 1,
            RequestError = 2,
            ClosingError = 3,
        }

        // TODO - this repetition will become unmaintainable. We need to do this in macros.
        private const int HEADER_LENGTH_IN_BYTES = 12;

        #endregion private types

        #region private methods

        private async Task<Socket> GetSocketAsync()
        {
            if (!availableSockets.TryDequeue(out var socket))
            {
                socket = CreateSocket(socketName);
                await SetServerAddress(socket);
            }
            return socket;
        }

        private AsyncSocketClient(string socketName, string address)
        {
            this.socketName = socketName;
            this.address = address;
        }

        private async Task SetServerAddress(Socket socket)
        {
            await WriteToSocketAsync(socket, address, null, RequestType.SetServerAddress);
            await GetResponseAsync(socket);
        }

        ~AsyncSocketClient()
        {
            CloseConnections();
        }

        private void CloseConnections()
        {
            foreach (var socket in this.availableSockets)
            {
                socket.Dispose();
            }
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

        private static async Task<string?> GetResponseAsync(Socket socket)
        {
            var headerBuffer = new byte[HEADER_LENGTH_IN_BYTES];
            await socket.ReceiveAsync(new ArraySegment<byte>(headerBuffer), SocketFlags.None);
            var header = GetHeader(headerBuffer, 0);
            // Console.WriteLine("Length: " + header.length);
            if (header.responseType == ResponseType.Null)
            {
                return null;
            }
            if (header.length == HEADER_LENGTH_IN_BYTES)
            {
                return "";
            }
            var stringLength = (int)header.length - HEADER_LENGTH_IN_BYTES;
            var buffer = ArrayPool<byte>.Shared.Rent(stringLength);
            // Console.WriteLine("Read buffer length " + buffer.Length);
            var bytesRead = 0;
            while (bytesRead < stringLength)
            {
                bytesRead += await socket.ReceiveAsync(new ArraySegment<byte>(buffer, bytesRead, buffer.Length - bytesRead), SocketFlags.None);
            }
            // Console.WriteLine("Bytes read " + bytesRead);
            var result = Encoding.UTF8.GetString(new Span<byte>(buffer, 0, stringLength));
            ArrayPool<byte>.Shared.Return(buffer);
            if (header.responseType == ResponseType.String)
            {
                return result;
            }
            if (header.responseType == ResponseType.RequestError)
            {
                Console.WriteLine("request failure " + result);
                throw new Exception(result);
            }
            if (header.responseType == ResponseType.ClosingError)
            {
                Console.WriteLine("clsoing failure " + result);
                throw new Exception(result);
            }
            throw new Exception("Unknown response type: " + header.responseType);
        }

        private static Socket CreateSocket(string socketAddress)
        {
            var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.IP);
            var endpoint = new UnixDomainSocketEndPoint(socketAddress);
            socket.Blocking = false;
            socket.SendBufferSize = 2 ^ 22;
            socket.ReceiveBufferSize = 2 ^ 22;
            socket.Connect(endpoint);
            return socket;
        }

        private void WriteUint32ToBuffer(UInt32 value, byte[] target, int offset)
        {
            var encodedVal = BitConverter.GetBytes(value);
            Buffer.BlockCopy(encodedVal, 0, target, offset, encodedVal.Length);
        }

        private async Task WriteToSocketAsync(Socket socket, string key, string? value, RequestType requestType)
        {
            var encoding = Encoding.UTF8;
            var headerLength = HEADER_LENGTH_IN_BYTES + ((value == null) ? 0 : 4);
            var maxLength = headerLength + key.Length * 3 + ((value == null) ? 0 : value.Length * 3);
            var buffer = ArrayPool<byte>.Shared.Rent(maxLength);
            // Console.WriteLine("write buffer length " + buffer.Length);
            var firstStringLength = encoding.GetBytes(key, 0, key.Length, buffer, (int)headerLength);
            var secondStringLength = (value == null) ? 0 :
                encoding.GetBytes(value, 0, value.Length, buffer, (int)headerLength + firstStringLength);
            var length = headerLength + firstStringLength + secondStringLength;
            // Console.WriteLine("write length " + length);
            WriteUint32ToBuffer((UInt32)length, buffer, 0);
            WriteUint32ToBuffer((UInt32)0, buffer, 4);
            WriteUint32ToBuffer((UInt32)requestType, buffer, 8);
            if (value != null)
            {
                WriteUint32ToBuffer((UInt32)firstStringLength, buffer, HEADER_LENGTH_IN_BYTES);
            }

            await socket.SendAsync(new ReadOnlyMemory<byte>(buffer, 0, length), SocketFlags.None);
            ArrayPool<byte>.Shared.Return(buffer);
        }

        #endregion private methods

        #region private fields

        private readonly ConcurrentQueue<Socket> availableSockets = new();

        #endregion private types

        #region rust bindings

        private delegate void InitCallback(IntPtr addressPointer, IntPtr errorPointer);
        [DllImport("libbabushka_csharp", CallingConvention = CallingConvention.Cdecl, EntryPoint = "start_socket_listener_wrapper")]
        private static extern void StartSocketListener(IntPtr initCallback);
        private readonly string socketName;
        private readonly string address;

        #endregion
    }
}
