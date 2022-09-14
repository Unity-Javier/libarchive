#include "write_multithreaded.h"
#include <stdio.h>
#include <iostream>
#include <string>
#include "concurrentqueue.h"
#include <vector>
#include <windows.h>
#include <filesystem>
#include <set>
#include <string_view>

#define CREATE_THREADS 8
#define WRITE_THREADS 8
#define CLOSE_THREADS 8
#define NumberOfConcurrentThreads 4
#define TOTAL_THREADS (CREATE_THREADS + WRITE_THREADS + CLOSE_THREADS)

#define CREATE_DIRECTORIES_THREADS 2

struct IOCPFile
{
	const char* sourceContents;

	HANDLE destinationHandle;
	HANDLE iocpDestinationHandle;
	std::wstring destinationPath;
	size_t destinationFileSize;
	OVERLAPPED destinationOverlapped;


	IOCPFile() : sourceContents(nullptr)
		, destinationHandle(INVALID_HANDLE_VALUE)
		, iocpDestinationHandle(0)
		, destinationFileSize(0)
		, destinationPath(L"")
	{
		RtlZeroMemory(&destinationOverlapped, sizeof(destinationOverlapped));
		destinationOverlapped.Offset = 0xFFFFFFFF; //Write to end of file
		destinationOverlapped.OffsetHigh = 0xFFFFFFFF; //Write to end of file
	}

	IOCPFile(IOCPFile&& other) noexcept :
			destinationHandle(other.destinationHandle)
		,	iocpDestinationHandle(other.destinationHandle)
		,	destinationPath(std::move(other.destinationPath))
		,	destinationFileSize(other.destinationFileSize)
		,	sourceContents(std::move(other.sourceContents))
		,	destinationOverlapped(std::move(destinationOverlapped))
	{

	}

	void operator = (IOCPFile&& other)
	{
		destinationHandle = other.destinationHandle;
		iocpDestinationHandle = other.destinationHandle;
		destinationPath = std::move(other.destinationPath);
		destinationFileSize = other.destinationFileSize;
		sourceContents = std::move(other.sourceContents);
		destinationOverlapped = std::move(destinationOverlapped);
	}
};


std::wstring s2ws(const std::string& str)
{
	int size_needed = MultiByteToWideChar(CP_UTF8, 0, &str[0], (int)str.size(), NULL, 0);
	std::wstring wstrTo(size_needed, 0);
	MultiByteToWideChar(CP_UTF8, 0, &str[0], (int)str.size(), &wstrTo[0], size_needed);
	return wstrTo;
}


int write_multithreaded(const std::vector<FileData> &files)
{
	//find all files
	std::thread programThreads[TOTAL_THREADS];

	moodycamel::ConcurrentQueue <IOCPFile*> createFileQueue;
	moodycamel::ConcurrentQueue <IOCPFile*> writeFileQueue(files.size());
	moodycamel::ConcurrentQueue <IOCPFile*> closeFileQueue;

	std::atomic<size_t> filesCreated = 0;
	std::atomic<size_t> totalFilesRead = 0;
	std::atomic<size_t> totalFilesClosed = 0;
	std::atomic<size_t> totalDirectoriesCreated = 0;
	size_t dirCount = 0;

	//std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();

	std::set<std::wstring> directoryPaths;

	moodycamel::ConcurrentQueue<std::wstring> directoryQueue;

	for (size_t i = 0; i < files.size(); ++i)
	{
		std::wstring curPath = s2ws(files[i].path);

		size_t parentPathIndex = curPath.find_last_of(L"/");
		if (parentPathIndex != std::string::npos && directoryPaths.find(curPath.substr(0, parentPathIndex)) == directoryPaths.end())
		{
			directoryPaths.insert(curPath.substr(0, parentPathIndex));
			dirCount++;
		}

		IOCPFile *curFile = new IOCPFile();
		curFile->destinationFileSize = files[i].contentSize;
		curFile->sourceContents = files[i].contents;
		curFile->destinationPath = s2ws(files[i].path);

		createFileQueue.enqueue(std::move(curFile));
	} 

	std::vector<std::wstring> dirsToSort(directoryPaths.begin(), directoryPaths.end());
	std::sort(dirsToSort.begin(), dirsToSort.end());

	directoryQueue.enqueue_bulk(dirsToSort.begin(), dirsToSort.size());
	
	std::thread createDirectoriesThreads[CREATE_DIRECTORIES_THREADS];

	for (size_t i = 0; i < CREATE_DIRECTORIES_THREADS; ++i)
	{
		createDirectoriesThreads[i] = std::thread([&](size_t max)
		{
			do 
			{
				std::vector<std::wstring> directories;
				directories.resize(max);
				size_t actualDequeued = directoryQueue.try_dequeue_bulk(directories.begin(), max);

				if (actualDequeued > 0)
				{
					for (size_t i = 0; i < actualDequeued; ++i)
					{
						if (!CreateDirectoryW(directories[i].c_str(), NULL))
						{
							if(GetLastError() == ERROR_PATH_NOT_FOUND)
								directoryQueue.enqueue(directories[i]);
							else
							{
								printf("Could not create directory: %S\n", directories[i].c_str());
								totalDirectoriesCreated++;
							}
						}
						else
						{
							totalDirectoriesCreated++;
						}
					}
				}
			}
			while (totalDirectoriesCreated != dirCount);
			

		}, dirsToSort.size() / CREATE_DIRECTORIES_THREADS);
	}

	for (size_t i = 0; i < CREATE_DIRECTORIES_THREADS; ++i)
		createDirectoriesThreads[i].join();

	for (size_t curThread = 0; curThread < CREATE_THREADS; ++curThread)
	{
		programThreads[curThread] = std::thread([&filesCreated, &createFileQueue, &writeFileQueue](size_t max, size_t curThread)
		{
			std::vector<IOCPFile*> files;

			size_t actualDequeued = 0;
			do
			{
				files.resize(max);
				actualDequeued = createFileQueue.try_dequeue_bulk(files.begin(), max);

				for (size_t i = 0; i < actualDequeued; ++i)
				{
					DWORD creationDisposition = CREATE_ALWAYS;
					DWORD destinationFileFlagsAndAttributes = FILE_FLAG_OVERLAPPED;

					files[i]->destinationHandle = ::CreateFileW(
						files[i]->destinationPath.c_str(),
						GENERIC_READ | GENERIC_WRITE,
						FILE_SHARE_READ,
						nullptr,
						creationDisposition,
						destinationFileFlagsAndAttributes,
						nullptr);

					//printf("Creating file: %S\n", files[i].destinationPath.c_str());

					if (files[i]->destinationHandle == INVALID_HANDLE_VALUE)
					{
						const DWORD lastError = ::GetLastError();
						printf("Error creating destination file %d\n", lastError);
					}

					files[i]->iocpDestinationHandle = ::CreateIoCompletionPort(
						files[i]->destinationHandle
						, NULL
						, 0
						, 0);

					if (!writeFileQueue.try_enqueue(std::move(files[i])))
						printf("Error: Could not enqueue files\n");
					else
						filesCreated++;
				}
			} while (actualDequeued != 0 || createFileQueue.size_approx() > 0);

		}, files.size() / (CREATE_THREADS), curThread);  
	}

	for (int i = CREATE_THREADS; i < CREATE_THREADS + WRITE_THREADS; ++i)
	{
		programThreads[i] = std::thread([&totalFilesRead, &writeFileQueue, &closeFileQueue](size_t max, size_t allFilesSize, int curThread)
		{
			std::vector<IOCPFile*> files;

			size_t actualDequeued = 0;

			do
			{
				files.resize(max);
				actualDequeued = writeFileQueue.try_dequeue_bulk(files.begin(), max);

				for (int i = 0; i < actualDequeued; ++i)
				{
					//Write it out
					const BOOL writeResult = ::WriteFile(files[i]->destinationHandle
						, files[i]->sourceContents
						, files[i]->destinationFileSize
						, nullptr
						, (LPOVERLAPPED) &files[i]->destinationOverlapped);

					if (!writeResult)
					{
						const DWORD lastError = ::GetLastError();
						if (lastError == ERROR_IO_PENDING)
						{
							LPOVERLAPPED ovl = nullptr;
							DWORD transferred = 0;
							ULONG_PTR completionKey = 0;
							const BOOL result = ::GetQueuedCompletionStatus(files[i]->iocpDestinationHandle
								, &transferred
								, &completionKey
								, &ovl, INFINITE);

							if (!result)
							{
								printf("Error: IOCP Writing file");
								continue;
							}

							if (files[i]->destinationOverlapped.InternalHigh != transferred)
								printf("Error: Size read mismatch\n");
							//printf("Write %d bytes to %S\n", transferred, files[i].destinationPath.c_str());
						}
					}

					totalFilesRead++;
					closeFileQueue.enqueue(std::move(files[i]));
				}
			} while (totalFilesRead < allFilesSize);

		}, files.size() / WRITE_THREADS, files.size(), i - CREATE_THREADS);
	}

	for (size_t i = (CREATE_THREADS + WRITE_THREADS); i < (CREATE_THREADS + WRITE_THREADS + CLOSE_THREADS); ++i)
	{
		programThreads[i] = std::thread([&totalFilesClosed, &totalFilesRead, &closeFileQueue](size_t max, size_t allFilesSize, size_t dirCount, int curThread)
		{
			size_t actualDequeued = 0;
			std::vector<IOCPFile*> files;

			do
			{
				files.resize(max);
				actualDequeued = closeFileQueue.try_dequeue_bulk(files.begin(), max);
				for (size_t i = 0; i < actualDequeued; ++i)
				{
					::CloseHandle(files[i]->destinationHandle);
					::CloseHandle(files[i]->iocpDestinationHandle);
					totalFilesClosed++;
				}
			} while (totalFilesClosed < (allFilesSize - dirCount));
		}, files.size() / CLOSE_THREADS, files.size(), dirCount, i - (CREATE_THREADS + WRITE_THREADS));
	}

	for (size_t i = 0; i < CREATE_THREADS + WRITE_THREADS + CLOSE_THREADS; ++i)
		programThreads[i].join();

	if (createFileQueue.size_approx() || writeFileQueue.size_approx())
		printf("Error: Threads exited with queues not-empty\n");

	//std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
	//std::cout << "Completed.\nTime difference = " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "[ms]" << std::endl;

	return 0;
}
