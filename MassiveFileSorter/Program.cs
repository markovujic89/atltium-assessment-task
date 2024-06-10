using System.Diagnostics;
using MassiveFileSorter;

// Set up input and output paths
string tempDirectory = @"C:\Temp\temp_chunks";
string outputFilePath = @"C:\Temp\sorted_largefile.txt";

var stopwatch = new Stopwatch();
stopwatch.Start();

Process.GetCurrentProcess().PriorityClass = ProcessPriorityClass.High;

Console.WriteLine("Starting to divide file into chunks!!.");
await FileManager.DivideFileIntoChunksParallel(@"C:\Temp\input.txt", tempDirectory);
Console.WriteLine("File divided into chunks finished!!.");
Console.WriteLine($"Elapsed time: {stopwatch.Elapsed.Minutes}:{stopwatch.Elapsed.Seconds}");

Console.WriteLine("Starting to merge!!");
var stopwatchForMergeProcess = new Stopwatch();
stopwatchForMergeProcess.Start();
await MergeFilesManager.MergeSortedChunksAsync(tempDirectory, outputFilePath);
Console.WriteLine($"Elapsed time for merging: {stopwatchForMergeProcess.Elapsed.Minutes}:{stopwatchForMergeProcess.Elapsed.Seconds}");
stopwatchForMergeProcess.Stop();

stopwatch.Stop();









