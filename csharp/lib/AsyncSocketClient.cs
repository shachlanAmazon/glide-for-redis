using System.Buffers;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;

namespace babushka
{
    public class AsyncSocketClient : IDisposable
    {
        #region public methods

        public static async Task<AsyncSocketClient> CreateSocketClient(string address)
        {
            var socketName = await GetSocketNameAsync();
            return new AsyncSocketClient(socketName, address);
        }

        public async Task SetAsync(string key, string value)
        {
            var task = await ExecuteOnConnect(new AsyncSocketConnection.WriteRequest { type = AsyncSocketConnection.RequestType.SetString, args = new() { key, value } });
            await task;
        }

        public async Task<string?> GetAsync(string key)
        {
            var task = await ExecuteOnConnect(new AsyncSocketConnection.WriteRequest { type = AsyncSocketConnection.RequestType.GetString, args = new() { key } });
            return await task;
        }

        public void Dispose()
        {
            DisposeWithError(null);
        }

        #endregion public methods

        #region private methods

        private void DisposeWithError(Exception? error)
        {
            if (Interlocked.CompareExchange(ref this.disposedFlag, 1, 0) == 1)
            {
                return;
            }
            foreach (var connection in allConnections)
            {
                if (error is null)
                {
                    connection.Dispose();
                }
                else
                {
                    connection.DisposeWithError(error);
                }
            }
        }

        private async Task<Task<string?>> ExecuteOnConnect(AsyncSocketConnection.WriteRequest writeRequest)
        {
            var connection = await GetConnection();
            var task = await connection.StartRequest(writeRequest);
            availableConnections.Enqueue(connection);
            return task;
        }

        private AsyncSocketClient(string socketName, string address)
        {
            this.socketName = socketName;
            this.address = address;
            this.clientIdentifier = Interlocked.Increment(ref clientIdentifiers);
        }

        ~AsyncSocketClient()
        {
            Dispose();
        }

        private async Task<AsyncSocketConnection> GetConnection()
        {
            if (!availableConnections.TryDequeue(out var connection))
            {
                connection = await AsyncSocketConnection.CreateSocketConnection(socketName, address, clientIdentifier);
                allConnections.Add(connection);
            }
            return connection;
        }

        /// Triggers the creation of a Rust-side socket server if one isn't running, and returns the name of the socket the server is listening on.
        private static Task<string> GetSocketNameAsync()
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
            return completionSource.Task;
        }

        #endregion private methods

        #region private fields

        private readonly ConcurrentQueue<AsyncSocketConnection> availableConnections = new();
        private readonly ConcurrentBag<AsyncSocketConnection> allConnections = new();
        /// 1 when disposed, 0 before
        private int disposedFlag = 0;
        private bool IsDisposed => disposedFlag == 1;

        private readonly string socketName;
        private readonly UInt64 clientIdentifier;
        private readonly string address;
        private static UInt64 clientIdentifiers;

        #endregion private types

        #region rust bindings

        private delegate void InitCallback(IntPtr addressPointer, IntPtr errorPointer);
        [DllImport("libbabushka_csharp", CallingConvention = CallingConvention.Cdecl, EntryPoint = "start_socket_listener_wrapper")]
        private static extern void StartSocketListener(IntPtr initCallback);

        #endregion
    }
}
