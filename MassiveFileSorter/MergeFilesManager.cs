using System.Collections.Concurrent;

namespace MassiveFileSorter;

public static class MergeFilesManager
{
    public static async Task MergeSortedChunksAsync(string tempDirectory, string outputFilePath)
    {
        var sortedFilePaths = Directory.GetFiles(tempDirectory);
        var readers = sortedFilePaths.Select(file => new StreamReader(file)).ToList();
        var minHeap = new SortedDictionary<string, Queue<string>>();
        var tasks = new List<Task>();

        foreach (var reader in readers)
        {
            tasks.Add(ReadAndEnqueueAsync(reader, minHeap));
        }

        await Task.WhenAll(tasks);

        await using (var writer = new StreamWriter(outputFilePath))
        {
            while (minHeap.Count > 0)
            {
                var minGroup = minHeap.First();
                foreach (var line in minGroup.Value)
                {
                    await writer.WriteLineAsync(line);
                }

                minHeap.Remove(minGroup.Key);

                foreach (var reader in readers)
                {
                    if (!reader.EndOfStream)
                    {
                        await ReadAndEnqueueAsync(reader, minHeap);
                    }
                }
            }
        }

        foreach (var reader in readers)
        {
            reader.Dispose();
        }
    }

    private static async Task ReadAndEnqueueAsync(StreamReader reader, SortedDictionary<string, Queue<string>> minHeap)
    {
        var line = await reader.ReadLineAsync();
        var split = line?.Split('.');
        var key = split is { Length: > 1 } ? split[1] : null;

        if (key != null)
        {
            lock (minHeap)
            {
                if (!minHeap.ContainsKey(key))
                {
                    minHeap[key] = new Queue<string>();
                }

                if (line != null) minHeap[key].Enqueue(line);
            }
        }
    }
}