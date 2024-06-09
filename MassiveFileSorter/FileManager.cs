using System.Diagnostics;
using System.Text;

namespace MassiveFileSorter;

public static class FileManager
{
    private const int MaxChunks = 50;
    public static async Task DivideFileIntoChunksParallel(string inputFilePath, string tempDirectory)
    {
        var stopwatch = new Stopwatch();
        stopwatch.Start();

        if (!File.Exists(inputFilePath))
        {
            Console.WriteLine($"Input file '{inputFilePath}' not found.");
            return;
        }
        
        var tasks = new List<Task>();
        await using var fileStream = new FileStream(inputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize:  4096, useAsync: true);
        long totalSize = fileStream.Length;
        long chunkSize = (long)Math.Ceiling((double)totalSize / MaxChunks);

        for (long i = 0; i < MaxChunks; i++)
        {
            long chunkStart = i * chunkSize;
            if (chunkStart >= totalSize)
            {
                break;
            }

            tasks.Add(ProcessChunk(fileStream, chunkStart, chunkSize, tempDirectory, i));
        }

        await Task.WhenAll(tasks);
    }

    static async Task ProcessChunk(FileStream fileStream, long chunkStart, long chunkSize, string tempDirectory,
        long chunkIndex)
    {
        byte[] buffer = new byte[chunkSize];
        fileStream.Seek(chunkStart, SeekOrigin.Begin);
        var bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length);

        if (bytesRead > 0)
        {
            var lines = await ConvertBytesToText(buffer, bytesRead);
            
            var groupedLines = SortAndGroupLines(lines);
            Console.WriteLine($"Sorting is over {chunkIndex}");
            
            string chunkFilePath = Path.Combine(tempDirectory, $"chunk_{chunkIndex}.txt");

            // Use a larger buffer size for the StreamWriter
            await using var writer = new StreamWriter(chunkFilePath, false, Encoding.UTF8, 65536); // 64 KB buffer size

            // Batch write lines to reduce I/O operations
            const int batchSize = 1000;
            for (int i = 0; i < groupedLines.Count; i += batchSize)
            {
                var batch = groupedLines.Skip(i).Take(batchSize);
                await writer.WriteAsync(string.Join(Environment.NewLine, batch) + Environment.NewLine);
            }
        }
    }

    private static async Task<List<string>> ConvertBytesToText(byte[] buffer, int bytesRead)
    {
        var lines = new List<string>();
        using var memoryStream = new MemoryStream(buffer, 0, bytesRead);
        using var reader = new StreamReader(memoryStream);
        while (await reader.ReadLineAsync() is { } line)
        {
            lines.Add(line);
        }

        return lines;
    }

    private static List<string> SortAndGroupLines(List<string> lines)
    {
        var groupedLinesDictionary = new Dictionary<string, List<string>>();

        // Iterate over each line in the list
        foreach (var line in lines)
        {
            var split = line.Split('.');

            // Check if the split result has at least 2 elements
            if (split.Length >= 2)
            {
                var key = split[1];

                // If the key doesn't exist in the dictionary, add it
                if (!groupedLinesDictionary.ContainsKey(key))
                {
                    groupedLinesDictionary[key] = new List<string>();
                }

                // Add the line to the corresponding key's list
                groupedLinesDictionary[key].Add(line);
            }
        }

        // Flatten the dictionary values into a single list
        var groupedLines = groupedLinesDictionary
            .SelectMany(kv => kv.Value)
            .ToList();

        return groupedLines;
        
    }
}