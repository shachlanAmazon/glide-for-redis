using System.Collections.Concurrent;

namespace babushka
{

    internal class MessageContainer
    {
        internal Message<string?> GetMessage(int index)
        {
            return messages[index];
        }

        internal (Message<string?>, Task<string?>) GetMessageForCall(string? key, string? value)
        {
            var message = GetFreeMessage();
            var task = message.CreateTask(key, value, this).ContinueWith(result =>
            {
                ReturnFreeMessage(message);
                return result.Result;
            });
            return (message, task);
        }

        private Message<string?> GetFreeMessage()
        {
            if (!availableMessages.TryDequeue(out var message))
            {
                lock (messages)
                {
                    var index = messages.Count;
                    message = new Message<string?>(index);
                    messages.Add(message);
                }
            }
            return message;
        }

        private void ReturnFreeMessage(Message<string?> message)
        {
            availableMessages.Enqueue(message);
        }

        internal void DisposeWithError(Exception? error)
        {
            lock (messages)
            {
                foreach (var message in messages)
                {
                    try
                    {
                        message.SetException(new TaskCanceledException("Client closed", error));
                    }
                    catch (Exception) { }
                }
                messages.Clear();
            }
            availableMessages.Clear();
        }

        /// This list allows us random-access to the message in each index,
        /// which means that once we receive a callback with an index, we can
        /// find the message to resolve in constant time.
        private List<Message<string?>> messages = new();

        /// This queue contains the messages that were created and are currently unused by any task,
        /// so they can be reused y new tasks instead of allocating new messages.
        private ConcurrentQueue<Message<string?>> availableMessages = new();
    }

}
