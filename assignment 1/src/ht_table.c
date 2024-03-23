#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "ht_table.h"
#include "record.h"

#define CALL_BF_1(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return -1 ;             \
  }                        \
}

#define CALL_BF_2(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return NULL ;             \
  }                        \
}

int HT_CreateFile(char *fileName,  int buckets){
  BF_Block *block ;
  int fd ;
  char *data ;

  // CREATING AND OPENING FILE

  CALL_BF_1(BF_CreateFile(fileName)) ;
  CALL_BF_1(BF_OpenFile(fileName,&fd)) ;

  // CREATING BLOCK 0 

  BF_Block_Init(&block) ;
  CALL_BF_1(BF_AllocateBlock(fd,block)) ; 
  data = BF_Block_GetData(block) ;

  // INITIALIZING METADATA OF FILE 

  HT_info info ;
  info.fileDesc = fd ;
  info.stored_block = 0 ;
  info.block_capacity = (BF_BLOCK_SIZE - sizeof(HT_block_info)) / sizeof(Record) ;
  info.type_file = 2 ;
  info.numBuckets = buckets;
  if (buckets > 20 || buckets < 1){
    printf("Invalid buckets number!") ;
    return -1 ;
  }
  for (int i=0; i<=buckets; i++){
    info.hash_table[i] = -1;
  }
  memcpy(data,&info,sizeof(HT_info)) ;

  // SETTING BLOCK AS DIRTY AND UNPINNING

  BF_Block_SetDirty(block) ;
  CALL_BF_1(BF_UnpinBlock(block)) ;
  BF_Block_Destroy(&block) ;   
  return 0;
}

HT_info* HT_OpenFile(char *fileName){
  BF_Block *block ;
  int fd ;
  char *data ;

  CALL_BF_2(BF_OpenFile(fileName,&fd)) ;
  BF_Block_Init(&block) ;
  CALL_BF_2(BF_GetBlock(fd,0,block)) ;
  data = BF_Block_GetData(block) ;
  HT_info *info = (HT_info *)data ;

  BF_Block_Destroy(&block) ;  
  if (info->type_file != 2){  // not static hashing file
    return NULL ;
  }  
  return info ;
}

int HT_CloseFile( HT_info* HT_info ){

  BF_Block *block ;
  BF_Block_Init(&block) ;

  CALL_BF_1(BF_GetBlock(HT_info->fileDesc,HT_info->stored_block,block)) ;  // getting block 0 
  CALL_BF_1(BF_UnpinBlock(block)) ;     // time to unpin block 0 
  BF_Block_Destroy(&block) ;
  CALL_BF_1(BF_CloseFile(HT_info->fileDesc)) ;

  return 0;
}

int HT_InsertEntry(HT_info* ht_info, Record record){
  BF_Block *block, *overflow_block;
  HT_block_info *block_info, insert_info;
  char* data;
  int block_id;   //id of the block where the record will be inserted 
  int num_blocks;  //blocks of file
  int offset = BF_BLOCK_SIZE - sizeof(HT_block_info);  // where to start reading/copying block info
  int dirty = 0;  // 1 when block 0 contents are changed
  int bucket = record.id % ht_info->numBuckets;
  BF_Block_Init(&block);

  if (ht_info->hash_table[bucket] == -1){   //No block has been allocated for this bucket
    CALL_BF_1(BF_AllocateBlock(ht_info->fileDesc, block));
    BF_GetBlockCounter(ht_info->fileDesc,&num_blocks);
    block_id = num_blocks - 1;    //id of allocated block
    ht_info->hash_table[bucket] = block_id;
    dirty = 1;

    insert_info.id = block_id;
    insert_info.records = 1;
    insert_info.next_block = -1;  //there is no next block
    data = BF_Block_GetData(block);
    memcpy(data+offset, &insert_info, sizeof(HT_block_info));       // writing HT_block_info at the end of block
    memcpy(data, &record, sizeof(Record));        // writing 1st record at the beginning of block 
    BF_Block_SetDirty(block);   
  }
  else{ // A block has been allocated for this bucket
    CALL_BF_1(BF_GetBlock(ht_info->fileDesc, ht_info->hash_table[bucket], block));
    data = BF_Block_GetData(block);
    block_info = (HT_block_info*)(data + offset);

    if (block_info->records < ht_info->block_capacity){  // record fits into block 
      block_id = ht_info->hash_table[bucket];
      char *addr = data + (block_info->records) * sizeof(Record); // writing new record after last record of block 
      memcpy(addr,&record,sizeof(Record));
      block_info->records++;
      BF_Block_SetDirty(block);
    }
    else{ // block full of records. Overflow block needs to be created 
      BF_Block_Init(&overflow_block);
      CALL_BF_1(BF_AllocateBlock(ht_info->fileDesc, overflow_block));
      CALL_BF_1(BF_GetBlockCounter(ht_info->fileDesc, &num_blocks));
      block_id = num_blocks - 1; // id of overflow block

      int prev_id = ht_info->hash_table[bucket];
      ht_info->hash_table[bucket] = block_id; // bucket pointing to overflow block
      dirty = 1;

      insert_info.id = block_id;
      insert_info.records = 1;
      insert_info.next_block = prev_id;   // overflow block pointing to previous block

      data = BF_Block_GetData(overflow_block);
      memcpy(data+offset, &insert_info, sizeof(HT_block_info));
      memcpy(data, &record, sizeof(Record));

      BF_Block_SetDirty(overflow_block);
      CALL_BF_1(BF_UnpinBlock(overflow_block));
      BF_Block_Destroy(&overflow_block);
    }
  }
  CALL_BF_1(BF_UnpinBlock(block));
  if (dirty == 1){
    CALL_BF_1(BF_GetBlock(ht_info->fileDesc, ht_info->stored_block, block));
    BF_Block_SetDirty(block);
  }
  BF_Block_Destroy(&block);
  return block_id;
}

int HT_GetAllEntries(HT_info* ht_info, int value ){
  BF_Block *block;
  Record *rec;
  HT_block_info *block_info;
  char *data;
  int bucket = value % ht_info->numBuckets;
  int block_id = ht_info->hash_table[bucket];   //current block id
  int offset = BF_BLOCK_SIZE - sizeof(HT_block_info);  // where to start reading block info
  int read = 0;   //number of read blocks
  BF_Block_Init(&block);

  while (block_id != -1){  // while id is valid
    BF_GetBlock(ht_info->fileDesc, block_id, block);
    read++;
    data = BF_Block_GetData(block);
    block_info = (HT_block_info *)(data + offset);
    rec = (Record *)data ;

    for (int i=0 ; i < block_info->records ; i++){
      if (rec[i].id == value){
        printRecord(rec[i]) ;
      }
    }
    
    block_id = block_info->next_block ; 
    BF_UnpinBlock(block);
  }
  BF_Block_Destroy(&block);
  printf("Read %d blocks\n",read);
  return read;
}




