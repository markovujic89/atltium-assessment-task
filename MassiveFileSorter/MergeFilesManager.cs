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

            // Use Parallel.ForEach to merge files in parallel
            var paths = sortedFilePaths;
            await Task.WhenAll(
                Partitioner.Create(0, sortedFilePaths.Count, 2).GetPartitions(Environment.ProcessorCount).Select(
                    async partition =>
                    {
                        using (partition)
                        {
                            while (partition.MoveNext())
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
                        }
                    })
            );

            sortedFilePaths = mergedFilePaths.ToList();
        }

        // Rename the final merged file to the output file path
        File.Move(sortedFilePaths[0], outputFilePath);
    }

    private static async Task MergeTwoFilesAsync(string file1, string file2, string outputFile)
    {
        using var reader1 = new StreamReader(file1, Encoding.UTF8, false, 65536); // 64 KB buffer size
        using var reader2 = new StreamReader(file2, Encoding.UTF8, false, 65536); // 64 KB buffer size
        await using var writer = new StreamWriter(outputFile, false, Encoding.UTF8, 65536); // 64 KB buffer size

        // Use a list to batch write lines for better performance
        var buffer = new List<string>(1000); // Adjust size as needed

        string? line1 = await reader1.ReadLineAsync();
        string? line2 = await reader2.ReadLineAsync();

        while (line1 != null && line2 != null)
        {
            var split1 = line1.Split('.');
            var split2 = line2.Split('.');

            var key1 = split1.Length > 1 ? split1[1] : null;
            var key2 = split2.Length > 1 ? split2[1] : null;

            if (key1 != null && key2 != null)
            {
                if (string.Compare(key1, key2, StringComparison.Ordinal) <= 0)
                {
                    buffer.Add(line1);
                    line1 = await reader1.ReadLineAsync();
                }
                else
                {
                    buffer.Add(line2);
                    line2 = await reader2.ReadLineAsync();
                }
            }
            else
            {
                buffer.Add(line1);
                line1 = await reader1.ReadLineAsync();

                buffer.Add(line2);
                line2 = await reader2.ReadLineAsync();
            }

            // Write buffer to file if it reaches capacity
            if (buffer.Count >= 1000)
            {
                await writer.WriteLineAsync(string.Join(Environment.NewLine, buffer));
                buffer.Clear();
            }
        }

        // Write any remaining lines
        while (line1 != null)
        {
            buffer.Add(line1);
            line1 = await reader1.ReadLineAsync();
            if (buffer.Count >= 1000)
            {
                await writer.WriteLineAsync(string.Join(Environment.NewLine, buffer));
                buffer.Clear();
            }
        }

        while (line2 != null)
        {
            buffer.Add(line2);
            line2 = await reader2.ReadLineAsync();
            if (buffer.Count >= 1000)
            {
                await writer.WriteLineAsync(string.Join(Environment.NewLine, buffer));
                buffer.Clear();
            }
        }

        // Write any remaining buffer content to file
        if (buffer.Count > 0)
        {
            await writer.WriteLineAsync(string.Join(Environment.NewLine, buffer));
        }
    }
}