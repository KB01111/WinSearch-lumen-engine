using System.Collections;
using System.IO.Pipes;
using System.Reflection;
using System.Text.Json;
using WinSearch.Core.Ipc;
using WinSearch.Core.Protocol;
using Xunit;

namespace WinSearch.Tests;

public class IndexingPipeTests
{
    private static async Task<ClientMessage> ReadAsync(Stream stream, CancellationToken token)
    {
        var header = new byte[4]; await stream.ReadExactlyAsync(header, token);
        var payload = new byte[BitConverter.ToInt32(header)]; await stream.ReadExactlyAsync(payload, token);
        return JsonSerializer.Deserialize<ClientMessage>(payload, ProtocolJson.Options)!;
    }
    private static async Task WriteAsync(Stream stream, ServerMessage message, CancellationToken token)
    {
        var payload = JsonSerializer.SerializeToUtf8Bytes(message, ProtocolJson.Options);
        await stream.WriteAsync(BitConverter.GetBytes(payload.Length), token);
        await stream.WriteAsync(payload, token);
        await stream.FlushAsync(token);
    }
    private static int Pending(EngineClient client, string field) =>
        ((IDictionary)typeof(EngineClient).GetField(field, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(client)!).Count;

    [Fact]
    public async Task FailedWritesReleaseUnaryAndSearchWaiters()
    {
        await using var client = new EngineClient("unused-" + Guid.NewGuid());
        await Assert.ThrowsAsync<IOException>(() => client.GetIndexingConfigAsync());
        Assert.Equal(0, Pending(client, "_unary"));
        await Assert.ThrowsAsync<IOException>(async () => { await foreach (var _ in client.SearchAsync(new())) { } });
        Assert.Equal(0, Pending(client, "_streams"));
    }

    [Fact]
    public async Task ConcurrentPipeClientsDispatchFullPolicyPushesAndCancellation()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15)); var token = timeout.Token;
        var pipe = "winsearch-dotnet-policy-" + Guid.NewGuid();
        using var server1 = new NamedPipeServerStream(pipe, PipeDirection.InOut, 2, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        using var server2 = new NamedPipeServerStream(pipe, PipeDirection.InOut, 2, PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
        var received = new System.Collections.Concurrent.ConcurrentDictionary<string, TaskCompletionSource>();
        async Task Serve(NamedPipeServerStream server)
        {
            await server.WaitForConnectionAsync(token);
            Assert.IsType<HelloMessage>(await ReadAsync(server, token));
            await WriteAsync(server, new HelloAckMessage { ProtocolVersion = 1 }, token);
            var set = Assert.IsType<SetIndexingConfigMessage>(await ReadAsync(server, token));
            var gotRequest = received.GetOrAdd(set.Config.Roots[0], _ => new(TaskCreationOptions.RunContinuationsAsynchronously));
            Assert.Equal(IndexingMode.Scheduled, set.Config.Mode);
            Assert.Equal((ushort)1320, set.Config.HoursStart);
            await WriteAsync(server, new IndexingConfigMessage { Id = set.Id, Config = set.Config }, token);
            await WriteAsync(server, new IndexingConfigMessage { Config = set.Config }, token);
            Assert.IsType<GetIndexingConfigMessage>(await ReadAsync(server, token));
            gotRequest.TrySetResult();
            // Deliberately leave one unary unanswered; cancellation must release it.
            await Task.Delay(100, token);
        }
        var servers = Task.WhenAll(Serve(server1), Serve(server2));
        async Task Exercise(string root)
        {
            await using var client = new EngineClient(pipe);
            var pushed = new TaskCompletionSource<IndexingConfig>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.IndexingConfigChanged += (_, config) => pushed.TrySetResult(config);
            await client.ConnectAsync(token);
            var config = new IndexingConfig { Roots = [root], CodeEnabled = true, CodeRoots = [@"D:\Code"],
                ExcludedPaths = [root + @"\Skip"], ExcludedExtensions = ["pdf"], Mode = IndexingMode.Scheduled, HoursStart = 1320, HoursEnd = 360 };
            var copy = await client.SetIndexingConfigAsync(config, token);
            Assert.Equal(root, Assert.Single(copy.Roots)); Assert.Equal((ushort)360, copy.HoursEnd);
            Assert.Equal(root, Assert.Single((await pushed.Task.WaitAsync(token)).Roots));
            using var cancelled = CancellationTokenSource.CreateLinkedTokenSource(token);
            var request = client.GetIndexingConfigAsync(cancelled.Token);
            await received.GetOrAdd(root, _ => new(TaskCreationOptions.RunContinuationsAsynchronously)).Task.WaitAsync(token);
            cancelled.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => request);
            Assert.Equal(0, Pending(client, "_unary"));
        }
        await Task.WhenAll(Exercise(@"C:\A"), Exercise(@"C:\B"));
        await servers;
    }
}
