#pragma once

#include <vector>

struct FileData
{
	const char* path;
	const char* contents;
	FileData(const char* filePath, const char* fileContents) : path(filePath), contents(fileContents)
	{

	}
};

int write_multithreaded(const std::vector<FileData> &files);
