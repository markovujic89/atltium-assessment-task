using System.Collections.Concurrent;
using System.Text;

namespace MassiveFileSorter;

public static class MergeFilesManager
{
    public static async Task MergeSortedChunksAsync(string tempDirectory, string outputFilePath)
    {
        var sortedFilePaths = Directory.GetFiles(tempDirectory).ToList();

        while (sortedFilePaths.Count > 1)
        {
            var mergedFilePaths = new ConcurrentBag<string>();

            var paths = sortedFilePaths;
            await Task.WhenAll(
                Partitioner.Create(0, sortedFilePaths.Count, 2).GetPartitions(Environment.ProcessorCount).Select(
                    async partition =>
                    {
                        using (partition)
                        {
                            while (partition.MoveNext())
                            {
                                await ExecuteFilesMerge(tempDirectory, partition, paths, mergedFilePaths);
                            }
                        }
                    })
            );

            sortedFilePaths = mergedFilePaths.ToList();
        }

        File.Move(sortedFilePaths[0], outputFilePath);
    }

    private static async Task ExecuteFilesMerge(string tempDirectory, IEnumerator<Tuple<int, int>> partition,
        List<string> paths,
        ConcurrentBag<string> mergedFilePaths)
    {
        var range = partition.Current;
        if (range.Item2 - range.Item1 == 2)
        {
            var mergedFilePath = Path.Combine(tempDirectory, $"merged_{Guid.NewGuid()}.txt");
            await MergeTwoFilesAsync(paths[range.Item1], paths[range.Item1 + 1],
                mergedFilePath);
            mergedFilePaths.Add(mergedFilePath);

            // Clean up old files
            File.Delete(paths[range.Item1]);
            File.Delete(paths[range.Item1 + 1]);
        }
        else if (range.Item2 - range.Item1 == 1)
        {
            mergedFilePaths.Add(paths[range.Item1]);
        }
    }

    private static async Task MergeTwoFilesAsync(string file1, string file2, string outputFile)
    {
        using var streamReaderOne = new StreamReader(file1, Encoding.UTF8, false, 65536); // 64 KB buffer size
        using var streamReaderTwo = new StreamReader(file2, Encoding.UTF8, false, 65536); // 64 KB buffer size
        await using var writer = new StreamWriter(outputFile, false, Encoding.UTF8, 65536); // 64 KB buffer size
        
        var buffer = new List<string>(1000); // Adjust size as needed

        string? lineOne = await streamReaderOne.ReadLineAsync();
        string? lineTwo = await streamReaderTwo.ReadLineAsync();

        while (lineOne != null && lineTwo != null)
        {
            var splitOne = lineOne.Split('.');
            var splitTwo = lineTwo.Split('.');

            var key1 = splitOne.Length > 1 ? splitOne[1] : null;
            var key2 = splitTwo.Length > 1 ? splitTwo[1] : null;

            if (key1 != null && key2 != null)
            {
                if (string.Compare(key1, key2, StringComparison.Ordinal) <= 0)
                {
                    buffer.Add(lineOne);
                    lineOne = await streamReaderOne.ReadLineAsync();
                }
                else
                {
                    buffer.Add(lineTwo);
                    lineTwo = await streamReaderTwo.ReadLineAsync();
                }
            }
            else
            {
                buffer.Add(lineOne);
                lineOne = await streamReaderOne.ReadLineAsync();

                buffer.Add(lineTwo);
                lineTwo = await streamReaderTwo.ReadLineAsync();
            }

            // Write buffer to file if it reaches capacity
            if (buffer.Count >= 1000)
            {
                await writer.WriteLineAsync(string.Join(Environment.NewLine, buffer));
                buffer.Clear();
            }
        }

        // Write any remaining lines
        while (lineOne != null)
        {
            buffer.Add(lineOne);
            lineOne = await streamReaderOne.ReadLineAsync();
            if (buffer.Count >= 1000)
            {
                await writer.WriteLineAsync(string.Join(Environment.NewLine, buffer));
                buffer.Clear();
            }
        }

        while (lineTwo != null)
        {
            buffer.Add(lineTwo);
            lineTwo = await streamReaderTwo.ReadLineAsync();
            if (buffer.Count >= 1000)
            {
                await writer.WriteLineAsync(string.Join(Environment.NewLine, buffer));
                buffer.Clear();
            }
        }
        
        if (buffer.Count > 0)
        {
            await writer.WriteLineAsync(string.Join(Environment.NewLine, buffer));
        }
    }
}