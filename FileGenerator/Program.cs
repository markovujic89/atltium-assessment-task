// See https://aka.ms/new-console-template for more information

using FileGenerator;

string outputFilePath = @"C:\Temp\input.txt";
long targetSizeInBytes = 1L * 1024 * 1024 * 1024; // 3 GB
int numberOfTasks = Environment.ProcessorCount; // Number of tasks equal to the number of CPU cores

// Calculate size per task
long sizePerTask = targetSizeInBytes / numberOfTasks;

Task[] tasks = new Task[numberOfTasks];
for (int i = 0; i < numberOfTasks; i++)
{
    int taskId = i;
    tasks[taskId] = Task.Run(() => InputFileGenerator.GeneratePartialFile(taskId, sizePerTask));
}

Task.WaitAll(tasks);

// Concatenate partial files
InputFileGenerator.ConcatenateFiles(outputFilePath, numberOfTasks);

Console.WriteLine("File generation complete.");

