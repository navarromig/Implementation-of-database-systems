#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hp_file.h"
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

int HP_CreateFile(char *fileName){

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

  HP_info info ;
  info.fileDesc = fd ;
  info.stored_block = 0 ;
  info.last_block = 0 ;
  info.block_capacity = (BF_BLOCK_SIZE - sizeof(HP_block_info)) / sizeof(Record) ;
  info.type_file = 1 ;
  info.next_block = -1 ;
  memcpy(data,&info,sizeof(HP_info)) ;

  // SETTING BLOCK AS DIRTY AND UNPINNING

  BF_Block_SetDirty(block) ;
  CALL_BF_1(BF_UnpinBlock(block)) ;
  BF_Block_Destroy(&block) ;   
  return 0 ;
}

HP_info* HP_OpenFile(char *fileName){

  BF_Block *block ;
  int fd ;
  char *data ;

  CALL_BF_2(BF_OpenFile(fileName,&fd)) ;
  BF_Block_Init(&block) ;
  CALL_BF_2(BF_GetBlock(fd,0,block)) ;
  data = BF_Block_GetData(block) ;
  HP_info *info = (HP_info *)data ;

  BF_Block_Destroy(&block) ;  
  if (info->type_file != 1){  // not heap file
    return NULL ;
  }  
  return info ;
}


int HP_CloseFile( HP_info* hp_info ){
  BF_Block *block ;
  BF_Block_Init(&block) ;

  CALL_BF_1(BF_GetBlock(hp_info->fileDesc,hp_info->stored_block,block)) ;  // getting block 0 
  CALL_BF_1(BF_UnpinBlock(block)) ;     // time to unpin block 0 
  BF_Block_Destroy(&block) ;
  CALL_BF_1(BF_CloseFile(hp_info->fileDesc)) ;

  return 0 ;
}

int HP_InsertEntry(HP_info* hp_info, Record record){
  BF_Block *block_1 , *block_2 ;
  HP_block_info *block_info, insert_info ; 
  char *data ;
  int block_id;  // id of the block where the record will be inserted 
  int num_blocks; // blocks of file
  int offset = BF_BLOCK_SIZE - sizeof(HP_block_info) ; // where to start reading/copying block info
  int dirty = 0 ;  // 1 when block 0 contents are changed
  BF_Block_Init(&block_1) ;

  if (hp_info->last_block == 0){   // only block 0 available

    CALL_BF_1(BF_AllocateBlock(hp_info->fileDesc,block_1)) ;  // block 1 created
    hp_info->last_block = 1 ;
    hp_info->next_block = 1 ;
    dirty = 1 ;  

    insert_info.id = 1 ;
    insert_info.next_block = -1 ;
    insert_info.records = 1 ;
    data = BF_Block_GetData(block_1) ;
    memcpy(data + offset,&insert_info,sizeof(HP_block_info)) ;  // writing HP_block_info at the end of block
    memcpy(data,&record,sizeof(record)) ;             // writing 1st record in the beginning of block 

    BF_Block_SetDirty(block_1) ;
  }
  else{   // other blocks available other than block 0
    CALL_BF_1(BF_GetBlock(hp_info->fileDesc,hp_info->last_block,block_1)) ;  // getting last block of file
    data = BF_Block_GetData(block_1) ;
    block_info = (HP_block_info*)(data + offset) ;

    if (block_info->records < hp_info->block_capacity){  // record fits into block 
      char *addr = data + (block_info->records) * sizeof(Record) ; // writing new record after last record of block 
      memcpy(addr,&record,sizeof(Record)) ;
      block_info->records++ ;      // +1 record in block 
      BF_Block_SetDirty(block_1) ;
    }
    else{   // block full of records. New block needs to be created 
      BF_Block_Init(&block_2) ;
      CALL_BF_1(BF_AllocateBlock(hp_info->fileDesc,block_2)) ;  
      hp_info->last_block++ ;
      dirty = 1 ;
      block_info->next_block = block_info->id + 1 ;

      insert_info.id = block_info->next_block ;
      insert_info.next_block = -1;
      insert_info.records = 1 ;
      data = BF_Block_GetData(block_2) ;
      memcpy(data + offset,&insert_info,sizeof(HP_block_info)) ;  // writing HP_block_info at the end of block
      memcpy(data,&record,sizeof(Record)) ;               // writing 1st record at the beginning of block 

      BF_Block_SetDirty(block_1) ;
      BF_Block_SetDirty(block_2) ;
      CALL_BF_1(BF_UnpinBlock(block_2)) ;
      BF_Block_Destroy(&block_2) ;
    }
  }
  CALL_BF_1(BF_UnpinBlock(block_1)) ;   
  if (dirty == 1){
    CALL_BF_1(BF_GetBlock(hp_info->fileDesc,hp_info->stored_block,block_1)) ;  // getting block 0
    BF_Block_SetDirty(block_1) ; // setting it dirty 
  }
  BF_Block_Destroy(&block_1) ;
  BF_GetBlockCounter(hp_info->fileDesc, &num_blocks);
  block_id = num_blocks - 1;
  return block_id;
}

int HP_GetAllEntries(HP_info* hp_info, int value){
  BF_Block *block ;
  Record *rec ;
  HP_block_info *block_info ;
  char *data ;
  int blocks;     // blocks of file
  int block_id = hp_info->next_block;     //current block id
  int offset = BF_BLOCK_SIZE - sizeof(HP_block_info);   // where to start reading block info
  int read = 0;   //number of read blocks
  CALL_BF_1(BF_GetBlockCounter(hp_info->fileDesc,&blocks)) ;
  BF_Block_Init(&block) ;
  
  for (int i=1 ; i<blocks ; i++){  // reading blocks 1 , 2 , ...  , 
    CALL_BF_1(BF_GetBlock(hp_info->fileDesc,block_id,block)) ;
    read++ ;
    data = BF_Block_GetData(block) ;
    block_info = (HP_block_info *)(data + offset) ;
    rec = (Record *)data ;
    for (int j=0 ; j < block_info->records ; j++){
      if (rec[j].id == value){
        printRecord(rec[j]) ; 
      }
    }
    block_id = block_info->next_block ;
    CALL_BF_1(BF_UnpinBlock(block)) ;
  }
  BF_Block_Destroy(&block) ;
  printf("Read %d blocks\n",read) ;
  return read;
}

