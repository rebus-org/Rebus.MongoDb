using System;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace Rebus.MongoDb.Tests.Extensions
{
    static class MongoDbCollectionExtensions
    {
        public static async Task WaitFor<TDoc>(this IMongoCollection<TDoc> collection, Func<IMongoCollection<TDoc>, bool> predicate, int timeoutSeconds = 5)
        {
            using (var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
            {
                var cancellationToken = cancellationTokenSource.Token;

                try
                {
                    while (true)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        if (predicate(collection)) return;

                        await Task.Delay(200, cancellationToken);
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw new TimeoutException($"Operation did not finish within {timeoutSeconds} s timeout");
                }
            }
        }
    }
}