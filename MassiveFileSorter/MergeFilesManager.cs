using System.Collections.Concurrent;

namespace MassiveFileSorter;

public static class MergeFilesManager
{
    public static async Task MergeSortedChunksWithGroupingAsync(string tempDirectory, string outputFilePath)
    {
        var sortedFilePaths = Directory.GetFiles(tempDirectory);
        var readers = sortedFilePaths.Select(file => new StreamReader(file)).ToList();
    
        await using (var writer = new StreamWriter(outputFilePath))
        {
            var queue = new ConcurrentDictionary<string, ConcurrentBag<string>>();
            var tasks = new List<Task>();
    
            // Initialize queue with the first line of each file
            foreach (var reader in readers)
            {
                tasks.Add(ReadAndEnqueueAsync(reader, queue));
            }
    
            await Task.WhenAll(tasks);
    
            while (queue.Count > 0)
            {
                var minGroup = queue.MinBy(kv => kv.Key);
                foreach (var line in minGroup.Value)
                {
                    await writer.WriteLineAsync(line);
                }
    
                queue.TryRemove(minGroup.Key, out _);
    
                foreach (var reader in readers)
                {
                    if (!reader.EndOfStream)
                    {
                        await ReadAndEnqueueAsync(reader, queue);
                    }
                }
            }
        }
    
        foreach (var reader in readers)
        {
            reader.Dispose();
        }
    }
    
    private static async Task ReadAndEnqueueAsync(StreamReader reader, ConcurrentDictionary<string, ConcurrentBag<string>> queue)
    {
        var line = await reader.ReadLineAsync();
        var split = line?.Split('.');
        var key = split is { Length: > 1 } ? split[1] : null;
    
        if (key != null)
        {
            queue.AddOrUpdate(key,
                _ =>
                {
                    if (line != null) return new ConcurrentBag<string> { line };
                    return null;
                },
                (_, bag) =>
                {
                    if (line != null) bag.Add(line);
                    return bag;
                });
        }
    }
}