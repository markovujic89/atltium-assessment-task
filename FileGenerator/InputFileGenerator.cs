using System.Text;

namespace FileGenerator;

internal static class InputFileGenerator
{
    internal static void GeneratePartialFile(int taskId, long sizePerTask)
    {
        string partialFilePath = $"input_{taskId}.txt";
        long currentSizeInBytes = 0;
        
        var random = new Random();

        using (StreamWriter writer = new StreamWriter(partialFilePath))
        {
            while (currentSizeInBytes < sizePerTask)
            {
                var randomNumberPrefix = random.Next(1, 1500);
                var randomNumber = random.Next(1, 1000);
                string line = $"{randomNumberPrefix}.Apple_{randomNumber}";

                // Validate the format before writing
                if (IsValidLine(line))
                {
                    writer.WriteLine(line);
                    currentSizeInBytes += Encoding.UTF8.GetByteCount(line + Environment.NewLine);
                }
            }
        }

        Console.WriteLine($"Task {taskId} complete. Generated {currentSizeInBytes / (1024 * 1024)} MB.");
    }

    internal static void ConcatenateFiles(string outputFilePath, int numberOfFiles)
    {
        using var output = new FileStream(outputFilePath, FileMode.Create);
        for (int i = 0; i < numberOfFiles; i++)
        {
            var partialFilePath = $"input_{i}.txt";
            using var input = new FileStream(partialFilePath, FileMode.Open);
            input.CopyTo(output);
        }
    }
    
    private static bool IsValidLine(string line)
    {
        
        if (string.IsNullOrEmpty(line))
        {
            return false;
        }

        var parts = line.Split('.');

        if (parts.Length != 2)
        {
            return false;
        }

        return true;
    }
}