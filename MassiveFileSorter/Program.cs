using System.Collections.Concurrent;
using System.Diagnostics;

string tempDirectory = @"C:\Temp\temp_chunks";
string outputFilePath = @"C:\Temp\sorted_largefile.txt";

var stopwatch = new Stopwatch();
stopwatch.Start();

await DivideFileIntoChunksParallel(@"C:\Temp\input.txt", tempDirectory);
Console.WriteLine("File divided into chunks finished!!.");
Console.WriteLine($"Elapsed time: {stopwatch.Elapsed.Minutes}:{stopwatch.Elapsed.Seconds}");

// await SortChunksWithGroupingParallelAsync(tempDirectory);
// Console.WriteLine("Sort chunks with grouping finished!!.");
// Console.WriteLine($"Elapsed time: {stopwatch.Elapsed.Minutes}");

Console.WriteLine("Starting to merge!!.");
await MergeSortedChunksWithGroupingAsync(tempDirectory, outputFilePath);

stopwatch.Stop();

static async Task DivideFileIntoChunksParallel(string inputFilePath, string tempDirectory)
{
    var stopwatch = new Stopwatch();
    stopwatch.Start();
    
    if (!File.Exists(inputFilePath))
    {
        Console.WriteLine($"Input file '{inputFilePath}' not found.");
        return;
    }

    const int maxChunks = 50;

    var tasks = new List<Task>();
    await using var fileStream = new FileStream(inputFilePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 4096, useAsync: true);
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

static async Task ProcessChunk(FileStream fileStream, long chunkStart, long chunkSize, string tempDirectory, long chunkIndex)
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

// static async Task SortChunksWithGroupingParallelAsync(string tempDirectory)
// {
//     var chunkFiles = Directory.GetFiles(tempDirectory);
//     var tasks = new List<Task>();
//
//     foreach (var filePath in chunkFiles)
//     {
//         // Start each sorting task asynchronously and add it to the list of tasks
//         tasks.Add(SortChunkWithGroupingAsync(filePath));
//     }
//
//     // Await the completion of all sorting tasks
//     await Task.WhenAll(tasks);
// }
//
// static async Task SortChunkWithGroupingAsync(string filePath)
// {
//     var lines = await File.ReadAllLinesAsync(filePath);
//
//     var groupedLines = lines
//         .GroupBy(line =>
//         {
//             var split = line.Split('.');
//             return split.ElementAtOrDefault(1);
//         })
//         .OrderBy(group => group.Key)
//         .SelectMany(group => group.OrderBy(line =>
//         {
//             var split = line.Split('.');
//             if (split.Length >= 1 && int.TryParse(split[0], out var value))
//             {
//                 return value;
//             }
//
//             return 0;
//         }))
//         .ToList();
//
//     await File.WriteAllLinesAsync(filePath, groupedLines);
// }

static async Task MergeSortedChunksWithGroupingAsync(string tempDirectory, string outputFilePath)
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

static async Task ReadAndEnqueueAsync(StreamReader reader, ConcurrentDictionary<string, ConcurrentBag<string>> queue)
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