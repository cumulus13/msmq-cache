// File: tests\MsmqCache.Tests\CoreTests.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-17
// Description: 
// License: MIT

using System.Linq;
using System.Text;
using MsmqCache.Core;
using Xunit;

public class RespTests
{
    [Fact]
    public void Roundtrip_binary_safe_command()
    {
        var val = new byte[] { 1, 13, 10, 0, 255, 36, 42 };           // contains CRLF, NUL, '$', '*'
        var enc = Resp.EncodeCommand(new System.Collections.Generic.List<byte[]> { Encoding.ASCII.GetBytes("SET"), Encoding.ASCII.GetBytes("k"), val });
        var cmd = RespReader.ParseCommand(enc);
        Assert.Equal(3, cmd.Count);
        Assert.Equal(val, cmd[2]);
    }

    [Fact]
    public void Inline_command_with_quotes()
    {
        var cmd = RespReader.ParseCommand(Encoding.ASCII.GetBytes("SET key \"hello world\" EX 10\r\n"));
        Assert.Equal(new[] { "SET", "key", "hello world", "EX", "10" }, cmd.Select(b => Encoding.UTF8.GetString(b)).ToArray());
    }

    [Fact]
    public void Oversized_bulk_is_rejected()
    {
        var data = Encoding.ASCII.GetBytes("*1\r\n$100\r\n" + new string('x', 100) + "\r\n");
        var r = new RespReader(new System.IO.MemoryStream(data), 10);
        Assert.Throws<ProtocolException>(() => r.ReadCommandAsync(default).GetAwaiter().GetResult());
    }

    [Fact]
    public void Replies_parse()
    {
        Assert.Equal(42, RespReader.ParseReply(Resp.Integer(42)).Integer);
        Assert.True(RespReader.ParseReply(Resp.Nil).IsNil);
        Assert.True(RespReader.ParseReply(Resp.Error("ERR x")).IsError);
        var arr = RespReader.ParseReply(Resp.Array(new System.Collections.Generic.List<byte[]> { Encoding.ASCII.GetBytes("a"), null }));
        Assert.Equal(2, arr.Items.Count);
        Assert.True(arr.Items[1].IsNil);
    }

    [Fact]
    public void Unbalanced_quotes_throw() { Assert.Throws<ProtocolException>(() => ArgSplitter.Split("SET a \"b")); }
}

public class GlobTests
{
    [Theory]
    [InlineData("*", "anything", true)]
    [InlineData("user:*", "user:42", true)]
    [InlineData("user:?", "user:42", false)]
    [InlineData("h[ae]llo", "hallo", true)]
    [InlineData("h[^e]llo", "hello", false)]
    [InlineData("a[a-c]", "ab", true)]
    public void Matches(string p, string t, bool expected) { Assert.Equal(expected, Glob.IsMatch(p, t)); }
}

public class ConfigTests
{
    [Fact]
    public void Parses_directives()
    {
        var c = ServerConfig.Parse(new[] { "# c", "port 7000", "bind 0.0.0.0", "recoverable no", "requirepass \"a b\"", "ingress-send-account \"D\\u\"" });
        Assert.Equal(7000, c.Port);
        Assert.False(c.Recoverable);
        Assert.Equal("a b", c.RequirePass);
        Assert.Equal(@".\private$\mcache.ingress", c.IngressPath);
    }

    [Fact]
    public void Unknown_directive_reports_line()
    {
        var e = Assert.Throws<ConfigException>(() => ServerConfig.Parse(new[] { "port 1", "bogus 1" }, "x.conf"));
        Assert.Contains("x.conf:2", e.Message);
    }

    [Fact]
    public void Bad_prefix_rejected() { Assert.Throws<ConfigException>(() => ServerConfig.Parse(new[] { "queue-prefix a/b" })); }

    [Fact]
    public void Port_range_checked() { Assert.Throws<ConfigException>(() => ServerConfig.Parse(new[] { "port 70000" })); }
}
