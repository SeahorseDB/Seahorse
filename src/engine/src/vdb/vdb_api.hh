#pragma once
#include <unistd.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

struct client;

// Commands
void TestCommand(client *c);

void ListTableCommand(client *c);
void CreateTableCommand(client *c);
void DropTableCommand(client *c);
void DescribeTableCommand(client *c);

void InsertCommand(client *c);
void BatchInsertCommand(client *c);
void DebugScanCommand(client *c);
void ScanCommand(client *c);
void AnnCommand(client *c);
void BatchAnnCommand(client *c);
void CheckIndexingCommand(client *c);
void CountIndexedElementsCommand(client *c);

/* snapshot apis */
void CheckVdbSnapshot();
void PrepareVdbSnapshot();
bool SaveVdbSnapshot(char *directory_path_);
bool LoadVdbSnapshot(char *directory_path_);
void PostVdbSnapshot();
bool RemoveDirectory(char *directory_path);

// Utility functions
void AllocateTableDictionary();
void DeallocateTableDictionary();

void AlterSuffix(char *str, const char *suffix);
#ifdef __cplusplus
}
#endif
