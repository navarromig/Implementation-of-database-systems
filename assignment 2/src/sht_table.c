#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_table.h"
#include "ht_table.h"
#include "record.h"

#define CALL_BF_1(call)       \
{                             \
  BF_ErrorCode code = call;   \
  if (code != BF_OK) {        \
    BF_PrintError(code);      \
    return -1 ;               \
  }                           \
}

#define CALL_BF_2(call)       \
{                             \
  BF_ErrorCode code = call;   \
  if (code != BF_OK) {        \
    BF_PrintError(code);      \
    return NULL ;             \
  }                           \
}

int SHT_CreateSecondaryIndex(char *sfileName,  int buckets, char* fileName){
  BF_Block *block ;
  int fd ;
  char *data ;

  // CREATING AND OPENING FILE

  CALL_BF_1(BF_CreateFile(sfileName)) ;
  CALL_BF_1(BF_OpenFile(sfileName, &fd)) ;

  // CREATING BLOCK 0 

  BF_Block_Init(&block) ;
  CALL_BF_1(BF_AllocateBlock(fd, block)) ; 
  data = BF_Block_GetData(block) ;

  // INITIALIZING METADATA OF FILE 

  SHT_info info ;
  strcpy(info.primary_index, fileName) ;
  info.fileDesc = fd ;
  info.stored_block = 0 ;
  info.block_capacity = (BF_BLOCK_SIZE - sizeof(SHT_block_info)) / sizeof(SHT_Record) ;
  info.type_file = 3 ;
  info.numBuckets = buckets;

  if (buckets > 20 || buckets < 1){
    printf("Invalid buckets number!") ;
    return -1 ;
  }

  for (int i=0; i<=buckets; i++){
    info.hash_table[i] = -1;
  }

  memcpy(data, &info, sizeof(SHT_info)) ;

  // SETTING BLOCK AS DIRTY AND UNPINNING

  BF_Block_SetDirty(block) ;
  CALL_BF_1(BF_UnpinBlock(block)) ;
  BF_Block_Destroy(&block) ;   
  return 0;
}

SHT_info* SHT_OpenSecondaryIndex(char *indexName){
  BF_Block *block ;
  int fd ;
  char *data ;

  CALL_BF_2(BF_OpenFile(indexName, &fd)) ;
  BF_Block_Init(&block) ;
  CALL_BF_2(BF_GetBlock(fd, 0, block)) ;
  data = BF_Block_GetData(block) ;
  SHT_info *info = (SHT_info *)data ;

  BF_Block_Destroy(&block) ;  
  if (info->type_file != 3){    // not a secondary index file
    return NULL ;
  }  
  return info ;
}


int SHT_CloseSecondaryIndex( SHT_info* SHT_info ){
  BF_Block *block ;
  BF_Block_Init(&block) ;

  CALL_BF_1(BF_GetBlock(SHT_info->fileDesc, SHT_info->stored_block, block)) ;  // getting block 0 
  CALL_BF_1(BF_UnpinBlock(block)) ;                                            // time to unpin block 0 
  BF_Block_Destroy(&block) ;
  CALL_BF_1(BF_CloseFile(SHT_info->fileDesc)) ;

  return 0;
}


int hash_function(char *string, int buckets)
{
  int sum = 0 ;
  for (int i=0 ; i<strlen(string) ; i++){
    sum += string[i] ;
  }
  return sum % buckets ;
}


int SHT_SecondaryInsertEntry(SHT_info* sht_info, Record record, int block_id){
  BF_Block *block, *overflow_block;
  SHT_block_info *block_info, insert_info;
  
  SHT_Record sht_record ;
  strcpy(sht_record.name, record.name);
  sht_record.block_id = block_id ; 

  char* data;
  int id;                                                   // id of the block where the record will be inserted 
  int num_blocks;                                           // blocks of file
  int offset = BF_BLOCK_SIZE - sizeof(SHT_block_info);      // where to start reading block info
  int dirty = 0;                                            // set as 1 when block 0 contents are changed
  int bucket = hash_function(record.name, sht_info->numBuckets) ;
  BF_Block_Init(&block);

  if (sht_info->hash_table[bucket] == -1){                    // No block has been allocated for this bucket
    CALL_BF_1(BF_AllocateBlock(sht_info->fileDesc, block));
    BF_GetBlockCounter(sht_info->fileDesc, &num_blocks);
    id = num_blocks - 1;                // id of allocated block
    sht_info->hash_table[bucket] = id;
    dirty = 1;

    insert_info.id = id;
    insert_info.records = 1;
    insert_info.next_block = -1;        // there is no next block

    data = BF_Block_GetData(block);
    memcpy(data + offset, &insert_info, sizeof(SHT_block_info));     // writing SHT_block_info at the end of block
    memcpy(data, &sht_record, sizeof(SHT_Record));                   // writing 1st record at the beginning of block 
    BF_Block_SetDirty(block);   
  }
  else{                                                       // A block has been allocated for this bucket
    CALL_BF_1(BF_GetBlock(sht_info->fileDesc, sht_info->hash_table[bucket], block));
    data = BF_Block_GetData(block);
    block_info = (SHT_block_info*)(data + offset);    // block info of current block 

    if (block_info->records < sht_info->block_capacity){  // record fits into block 
      id = sht_info->hash_table[bucket];
      char *addr = data + (block_info->records) * sizeof(SHT_Record); // writing new record after last record of block 
      memcpy(addr, &sht_record, sizeof(SHT_Record));
      block_info->records++;
      BF_Block_SetDirty(block);
    }
    else{                                                // block full of records. Overflow block needs to be created 
      BF_Block_Init(&overflow_block);
      CALL_BF_1(BF_AllocateBlock(sht_info->fileDesc, overflow_block));
      CALL_BF_1(BF_GetBlockCounter(sht_info->fileDesc, &num_blocks));
      id = num_blocks - 1;                          // id of overflow block
      int prev_id = sht_info->hash_table[bucket];
      sht_info->hash_table[bucket] = id;            // bucket pointing to overflow block
      dirty = 1;

      insert_info.id = id;
      insert_info.records = 1;
      insert_info.next_block = prev_id;   // overflow block pointing to previous block

      data = BF_Block_GetData(overflow_block);
      memcpy(data + offset, &insert_info, sizeof(SHT_block_info));
      memcpy(data, &sht_record, sizeof(SHT_Record));
      BF_Block_SetDirty(overflow_block);
      CALL_BF_1(BF_UnpinBlock(overflow_block));
      BF_Block_Destroy(&overflow_block);
    }
  }
  CALL_BF_1(BF_UnpinBlock(block));
  if (dirty == 1){
    CALL_BF_1(BF_GetBlock(sht_info->fileDesc, sht_info->stored_block, block));
    BF_Block_SetDirty(block);
  }
  BF_Block_Destroy(&block);
  return id;
}


// searches for a record with the given name in a block with id block_id
int FindName(HT_info* ht_info, char* name, int block_id)
{
  BF_Block *block ;
  char *data ;
  int offset = BF_BLOCK_SIZE - sizeof(HT_block_info);
  Record *rec ;
  HT_block_info *block_info ;
  BF_Block_Init(&block) ;

  CALL_BF_1(BF_GetBlock(ht_info->fileDesc, block_id, block)) ; 
  data = BF_Block_GetData(block) ;
  block_info = (HT_block_info *)(data + offset) ;
  rec = (Record *)data ;

  for (int i=0 ; i < block_info->records ; i++){
    if (strcmp(rec[i].name, name) == 0){
      printRecord(rec[i]) ;
    }
  }
  
  CALL_BF_1(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
  return 0 ;
}


int SHT_SecondaryGetAllEntries(HT_info* ht_info, SHT_info* sht_info, char* name){
  BF_Block *block;
  SHT_Record *rec;
  SHT_block_info *block_info;
  char *data;
  int bucket = hash_function(name, sht_info->numBuckets) ;
  int block_id = sht_info->hash_table[bucket];                // current block id
  int offset = BF_BLOCK_SIZE - sizeof(SHT_block_info);        // where to start reading block info
  int read = 0;                                               // number of read blocks
  BF_Block_Init(&block);

  while (block_id != -1){  // while id is valid
    BF_GetBlock(sht_info->fileDesc, block_id, block);
    read++;
    data = BF_Block_GetData(block);
    block_info = (SHT_block_info *)(data + offset);   // block info of current block
    rec = (SHT_Record *)data ;

    for (int i=0 ; i < block_info->records ; i++){
      if (strcmp(rec[i].name, name) == 0){
        FindName(ht_info, name, rec[i].block_id) ;
      }
    }

    block_id = block_info->next_block ; 
    BF_UnpinBlock(block);
  }

  BF_Block_Destroy(&block);
  printf("Read %d blocks\n",read);
  return read;
}



