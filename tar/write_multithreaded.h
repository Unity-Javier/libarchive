#pragma once

#include <vector>

struct FileData
{
	const char* path;
	const char* contents;
	const size_t contentSize;
	FileData(const char* filePath, const char* fileContents, size_t fileSize) : path(filePath), contents(fileContents), contentSize(fileSize)
	{

	}
};

int write_multithreaded(const std::vector<FileData> &files);
