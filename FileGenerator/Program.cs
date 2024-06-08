// See https://aka.ms/new-console-template for more information

string outputFilePath = @"C:\Temp\input.txt";
long targetSizeInBytes = 1L * 1024 * 1024 * 1024; // 50 GB
int numberOfTasks = Environment.ProcessorCount; // Number of tasks equal to the number of CPU cores

// Calculate size per task
long sizePerTask = targetSizeInBytes / numberOfTasks;

Task[] tasks = new Task[numberOfTasks];
for (int i = 0; i < numberOfTasks; i++)
{
    int taskId = i;
    tasks[taskId] = Task.Run(() => GeneratePartialFile(taskId, sizePerTask));
}

Task.WaitAll(tasks);

// Concatenate partial files
ConcatenateFiles(outputFilePath, numberOfTasks);

// Clean up partial files
for (int i = 0; i < numberOfTasks; i++)
{
    File.Delete($"input_{i}.txt");
}

Console.WriteLine("File generation complete.");

static void GeneratePartialFile(int taskId, long sizePerTask)
{
    string partialFilePath = $"input_{taskId}.txt";
    long currentSizeInBytes = 0;
    int proggress = 1;
    Random random = new Random();

    using (StreamWriter writer = new StreamWriter(partialFilePath))
    {
        while (currentSizeInBytes < sizePerTask)
        {
            var randomNumber = random.Next(1, 1500);
            string line = $"{randomNumber}.Apple_{randomNumber}";
            writer.WriteLine(line);
            currentSizeInBytes += System.Text.Encoding.UTF8.GetByteCount(line + Environment.NewLine);
            proggress++;

            // Optional: Display progress for every 1 million lines written
            if (proggress % 1_000_000 == 0)
            {
                Console.WriteLine($"Task {taskId}: Written {currentSizeInBytes / (1024 * 1024)} MB so far.");
            }
        }
    }

    Console.WriteLine($"Task {taskId} complete. Generated {currentSizeInBytes / (1024 * 1024)} MB.");
}

static void ConcatenateFiles(string outputFilePath, int numberOfFiles)
{
    using var output = new FileStream(outputFilePath, FileMode.Create);
    for (int i = 0; i < numberOfFiles; i++)
    {
        string partialFilePath = $"input_{i}.txt";
        using var input = new FileStream(partialFilePath, FileMode.Open);
        input.CopyTo(output);
    }
}