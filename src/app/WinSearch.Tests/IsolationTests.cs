using System.Security.Principal;
using WinSearch.Core.Ipc;
using WinSearch.Core.Settings;
using Xunit;

namespace WinSearch.Tests;

[CollectionDefinition("IsolatedEnvironment", DisableParallelization = true)]
public class IsolatedEnvironmentCollection;

[Collection("IsolatedEnvironment")]
public class IsolationTests
{
    [Fact]
    public void DataPipesAppMutexAndActivationEventsStayInTheirInstance()
    {
        var oldData = Environment.GetEnvironmentVariable("WINSEARCH_DATA_DIR");
        var oldInstance = Environment.GetEnvironmentVariable("WINSEARCH_INSTANCE_ID");
        var fixture = Path.Combine(Path.GetTempPath(), "winsearch-dotnet-isolation-" + Guid.NewGuid());
        Directory.CreateDirectory(fixture);
        try
        {
            Environment.SetEnvironmentVariable("WINSEARCH_DATA_DIR", Path.Combine(fixture, "a"));
            Environment.SetEnvironmentVariable("WINSEARCH_INSTANCE_ID", "fixture-A");
            var pipeA = EnginePipe.NameForCurrentUser();
            var mutexA = EnginePipe.ObjectNameForCurrentUser(@"Local\WinSearch.App.v1");
            using var heldA = new Mutex(false, mutexA);
            using var signalA = new EventWaitHandle(false, EventResetMode.AutoReset, EnginePipe.ObjectNameForCurrentUser(@"Local\WinSearch.ShowOverlay"));
            new AppSettings { LastQuery = "a" }.Save();
            Environment.SetEnvironmentVariable("WINSEARCH_DATA_DIR", Path.Combine(fixture, "b"));
            Environment.SetEnvironmentVariable("WINSEARCH_INSTANCE_ID", "fixture-B");
            var pipeB = EnginePipe.NameForCurrentUser();
            using var heldB = new Mutex(false, EnginePipe.ObjectNameForCurrentUser(@"Local\WinSearch.App.v1"));
            using var signalB = new EventWaitHandle(false, EventResetMode.AutoReset, EnginePipe.ObjectNameForCurrentUser(@"Local\WinSearch.ShowOverlay"));
            Assert.NotEqual(pipeA, pipeB);
            Assert.NotEqual(mutexA, EnginePipe.ObjectNameForCurrentUser(@"Local\WinSearch.App.v1"));
            using var user = WindowsIdentity.GetCurrent(); Assert.Contains(user.User!.Value, pipeB);
            Assert.Equal("", AppSettings.Load().LastQuery);
            new AppSettings { LastQuery = "b" }.Save();
            signalA.Set(); Assert.True(signalA.WaitOne(0)); Assert.False(signalB.WaitOne(0));
            Environment.SetEnvironmentVariable("WINSEARCH_DATA_DIR", Path.Combine(fixture, "a"));
            Assert.Equal("a", AppSettings.Load().LastQuery);
            Environment.SetEnvironmentVariable("WINSEARCH_DATA_DIR", "relative");
            Assert.Throws<InvalidOperationException>(() => AppSettings.Load());
            Environment.SetEnvironmentVariable("WINSEARCH_INSTANCE_ID", "bad/value");
            Assert.Throws<InvalidOperationException>(() => EnginePipe.NameForCurrentUser());
            Environment.SetEnvironmentVariable("WINSEARCH_INSTANCE_ID", null);
            Assert.Equal(@"Local\WinSearch.App.v1", EnginePipe.ObjectNameForCurrentUser(@"Local\WinSearch.App.v1"));
        }
        finally
        {
            Environment.SetEnvironmentVariable("WINSEARCH_DATA_DIR", oldData);
            Environment.SetEnvironmentVariable("WINSEARCH_INSTANCE_ID", oldInstance);
            Directory.Delete(fixture, true);
        }
    }
}
