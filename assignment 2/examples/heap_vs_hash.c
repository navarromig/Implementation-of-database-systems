#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hp_file.h"
#include "ht_table.h"

#define RECORDS_NUM 10000 
#define FILE_NAME_1 "data_hp.db"
#define FILE_NAME_2 "data_ht.db"

int main() {
  remove(FILE_NAME_1) ;
  remove(FILE_NAME_2) ;

  BF_Init(LRU);

  HP_CreateFile(FILE_NAME_1);
  HT_CreateFile(FILE_NAME_2,10);
  HP_info* info_hp = HP_OpenFile(FILE_NAME_1);
  HT_info* info_ht = HT_OpenFile(FILE_NAME_2);
  Record record;
  srand(1234);

  for (int id = 0; id < RECORDS_NUM; ++id) {
    record = randomRecord();
    HP_InsertEntry(info_hp, record);
	  HT_InsertEntry(info_ht, record);
  }

  int id = rand() % RECORDS_NUM;
  printf("\nSearching for: %d\n\n",id);
  printf("Heap : \n") ;
  HP_GetAllEntries(info_hp, id);
  printf("\nHash : \n") ;
  HT_GetAllEntries(info_ht, id);

  HP_CloseFile(info_hp);
  HT_CloseFile(info_ht);
  BF_Close();
}
