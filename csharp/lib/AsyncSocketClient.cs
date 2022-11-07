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

            return new AsyncSocketClient(socketName, address);
        }

        public async Task SetAsync(string key, string value)
        {
            var socket = await GetSocketAsync();
            await WriteToSocketAsync(socket, key, value, RequestType.SetString);
            await GetResponseAsync<object>(socket);
            availableSockets.Enqueue(socket);
        }

        public async Task<string?> GetAsync(string key)
        {
            var socket = await GetSocketAsync();
            await WriteToSocketAsync(socket, key, null, RequestType.GetString);
            var result = await GetResponseAsync<string?>(socket);
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
        }

        // TODO - this repetition will become unmaintainable. We need to do this in macros.
        private const int HEADER_LENGTH_IN_BYTES = 12;

        #endregion private types

        #region private methods

        private async Task<Socket> GetSocketAsync()
        {
            if (!availableSockets.TryDequeue(out var socket))
            {
                socket = CreateSocket(address);
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
            await GetResponseAsync<object>(socket);
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

        // private static ArraySegment<byte> ParseReadResults(byte[] buffer, int messageLength)
        // {
        //     var header = GetHeader(buffer, counter);
        //     if (header.length == 0)
        //     {
        //         throw new ArgumentException("length 0");
        //     }
        //     if (counter + header.length > messageLength)
        //     {
        //         return new ArraySegment<byte>(buffer, counter, messageLength - counter);
        //     }

        //     switch (header.responseType)
        //     {
        //         case ResponseType.Null:
        //             message.SetResult(null);
        //             break;
        //         case ResponseType.String:
        //             var valueLength = header.length - HEADER_LENGTH_IN_BYTES;
        //             message.SetResult(Encoding.UTF8.GetString(new Span<byte>(buffer,
        //                 (int)(counter + HEADER_LENGTH_IN_BYTES),
        //                 (int)valueLength
        //             )));
        //             break;
        //     }


        //     counter += (int)header.length;
        //     var offset = counter % 4;
        //     if (offset != 0)
        //     {
        //         // align counter to 4.
        //         counter += 4 - offset;
        //     }
        // }

        //     return new ArraySegment<byte>(buffer, counter, messageLength - counter);
        // }

        private static async Task<string?> GetResponseAsync(Socket socket)
        {
            var headerBuffer = new byte[HEADER_LENGTH_IN_BYTES];
            var previousSegment = new ArraySegment<byte>(headerBuffer);
            var receivedLength = await socket.ReceiveAsync(segmentAfterPreviousData, SocketFlags.None);
            var buffer = GetBuffer(previousSegment);
            var segmentAfterPreviousData = new ArraySegment<byte>(buffer, previousSegment.Count, buffer.Length - previousSegment.Count);
            var receivedLength = await socket.ReceiveAsync(segmentAfterPreviousData, SocketFlags.None);
            var newBuffer = ParseReadResults(buffer, receivedLength + previousSegment.Count);
            if (previousSegment.Array is not null)
            {
                ArrayPool<byte>.Shared.Return(previousSegment.Array);
            }
            previousSegment = newBuffer;
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
            var firstStringLength = encoding.GetBytes(key, 0, key.Length, buffer, (int)headerLength);
            var secondStringLength = (value == null) ? 0 :
                encoding.GetBytes(value, 0, value.Length, buffer, (int)headerLength + firstStringLength);
            var length = headerLength + firstStringLength + secondStringLength;
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
