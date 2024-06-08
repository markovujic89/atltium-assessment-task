using System.Diagnostics;

namespace MassiveFileSorter;

public static class FileManager
{
    public static async Task DivideFileIntoChunksParallel(string inputFilePath, string tempDirectory)
    {
        var stopwatch = new Stopwatch();
        stopwatch.Start();

        if (!File.Exists(inputFilePath))
        {
            Console.WriteLine($"Input file '{inputFilePath}' not found.");
            return;
        }

        const int maxChunks = 150;

        var tasks = new List<Task>();
        await using var fileStream = new FileStream(inputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read,
            bufferSize: 4096, useAsync: true);
        long totalSize = fileStream.Length;
        long chunkSize = (long)Math.Ceiling((double)totalSize / maxChunks);

        for (long i = 0; i < maxChunks; i++)
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
        int bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length);

        if (bytesRead > 0)
        {
            // Convert the byte array to a list of strings (lines)
            var lines = new List<string>();
            using (var memoryStream = new MemoryStream(buffer, 0, bytesRead))
            using (var reader = new StreamReader(memoryStream))
            {
                while (await reader.ReadLineAsync() is { } line)
                {
                    lines.Add(line);
                }
            }

            // Sort the lines with grouping logic
            var groupedLines = lines
                .GroupBy(line =>
                {
                    var split = line.Split('.');
                    return split.ElementAtOrDefault(1);
                })
                .OrderBy(group => group.Key)
                .SelectMany(group => group.OrderBy(line =>
                {
                    var split = line.Split('.');
                    if (split.Length >= 1 && int.TryParse(split[0], out var value))
                    {
                        return value;
                    }
                    return 0;
                }))
                .ToList();

            // Write the sorted data to a new chunk file
            string chunkFilePath = Path.Combine(tempDirectory, $"chunk_{chunkIndex}.txt");
            await using var writer = new StreamWriter(chunkFilePath, false);

            foreach (var value in groupedLines)
            {
                await writer.WriteLineAsync(value);
            }
        }
    }
}