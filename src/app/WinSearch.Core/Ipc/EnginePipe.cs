using System.Security.Principal;

namespace WinSearch.Core.Ipc;

/// <summary>
/// Derives the engine pipe name. It embeds the user SID so two accounts on the
/// same machine never collide — matching ws-platform's naming on the Rust side
/// (which uses the full \\.\pipe\ path for CreateNamedPipeW).
/// </summary>
public static class EnginePipe
{
    /// <summary>Bare pipe name for NamedPipeClientStream (no \\.\pipe\ prefix).</summary>
    public static string NameForCurrentUser()
    {
        using var identity = WindowsIdentity.GetCurrent();
        var sid = identity.User?.Value
            ?? throw new InvalidOperationException("cannot determine current user SID");
        return $"winsearch-{sid}{InstanceSuffix()}";
    }

    public static string InstanceSuffix()
    {
        var value = Environment.GetEnvironmentVariable("WINSEARCH_INSTANCE_ID");
        if (value is null) return "";
        ValidateInstanceId(value);
        return $"-{value}";
    }
    public static void ValidateInstanceId(string value)
    {
        if (value.Length is < 1 or > 48 || value.Any(c => !char.IsAsciiLetterOrDigit(c) && c != '_' && c != '-'))
            throw new InvalidOperationException("WINSEARCH_INSTANCE_ID must be 1..48 ASCII letters, digits, underscore or hyphen");
    }
    public static string ObjectNameForCurrentUser(string baseName)
    {
        var suffix = InstanceSuffix();
        if (suffix.Length == 0) return baseName;
        using var identity = WindowsIdentity.GetCurrent();
        var sid = identity.User?.Value ?? throw new InvalidOperationException("cannot determine current user SID");
        return $"{baseName}-{sid}{suffix}";
    }
}
