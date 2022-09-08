namespace tests;

using babushka;

// TODO - need to start a new redis server for each test?
public class AsyncSocketClientTests
{
    private async Task GetAndSetRandomValues(AsyncSocketClient client)
    {
        var key = Guid.NewGuid().ToString();
        var value = Guid.NewGuid().ToString();
        await client.SetAsync(key, value);
        var result = await client.GetAsync(key);
        Assert.That(result, Is.EqualTo(value));
    }

    [Test, Timeout(200)]
    public async Task GetReturnsLastSet()
    {
        var client = await AsyncSocketClient.CreateSocketClient("redis://localhost:6379");
        await GetAndSetRandomValues(client);
    }

    [Test, Timeout(200)]
    public async Task GetAndSetCanHandleNonASCIIUnicode()
    {
        var client = await AsyncSocketClient.CreateSocketClient("redis://localhost:6379");
        var key = Guid.NewGuid().ToString();
        var value = "שלום hello 汉字";
        await client.SetAsync(key, value);
        var result = await client.GetAsync(key);
        Assert.That(result, Is.EqualTo(value));
    }

    [Test, Timeout(200)]
    public async Task GetReturnsNull()
    {
        var client = await AsyncSocketClient.CreateSocketClient("redis://localhost:6379");
        var result = await client.GetAsync(Guid.NewGuid().ToString());
        Assert.That(result, Is.EqualTo(null));
    }

    [Test, Timeout(200)]
    public async Task GetReturnsEmptyString()
    {
        var client = await AsyncSocketClient.CreateSocketClient("redis://localhost:6379");
        var key = Guid.NewGuid().ToString();
        var value = "";
        await client.SetAsync(key, value);
        var result = await client.GetAsync(key);
        Assert.That(result, Is.EqualTo(value));
    }

    [Test, Timeout(2000)]
    public async Task HandleVeryLargeInput()
    {
        var client = await AsyncSocketClient.CreateSocketClient("redis://localhost:6379");
        var key = Guid.NewGuid().ToString();
        var value = "";
        while (value.Length < Math.Pow(2, 16))
        {
            value += Guid.NewGuid().ToString();
        }
        await client.SetAsync(key, value);
        var result = await client.GetAsync(key);
        Assert.That(result, Is.EqualTo(value));
    }

    // This test is slow and hardly a unit test, but it caught timing and releasing issues in the past,
    // so it's being kept.
    [Test, Timeout(2000)]
    public async Task ConcurrentOperationsWork()
    {
        var client = await AsyncSocketClient.CreateSocketClient("redis://localhost:6379");
        var operations = new List<Task>();

        for (int i = 0; i < 100; ++i)
        {
            var index = i;
            operations.Add(Task.Run(async () =>
            {
                for (int i = 0; i < 100; ++i)
                {
                    if ((i + index) % 2 == 0)
                    {
                        await GetAndSetRandomValues(client);
                    }
                    else
                    {
                        var result = await client.GetAsync(Guid.NewGuid().ToString());
                        Assert.That(result, Is.EqualTo(null));
                    }
                }
            }));
        }

        Task.WaitAll(operations.ToArray(), 5000);
    }
}
