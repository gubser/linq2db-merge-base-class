using Microsoft.Extensions.Logging;

namespace Linq2DbMergeBaseClass;

public class TestContextLoggerProvider : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName) => new TestContextLogger();
    public void Dispose() { }

    private class TestContextLogger : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull
        {
            return null;
        }

        public bool IsEnabled(LogLevel logLevel)
        {
            return true;
        }

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            TestContext.Out.WriteLine(formatter(state, exception));
        }
    }
}
