#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "ht_table.h"
#include "sht_table.h"
#include "stats.h"


#define RECORDS_NUM 102560 
#define FILE_NAME "data.db"
#define INDEX_NAME "index.db"

int main() {

  remove(FILE_NAME) ;
  remove(INDEX_NAME) ;

  srand(1234);
  BF_Init(LRU);

 
  HT_CreateFile(FILE_NAME,15);
  SHT_CreateSecondaryIndex(INDEX_NAME,15,FILE_NAME);
  HT_info* info = HT_OpenFile(FILE_NAME);
  SHT_info* index_info = SHT_OpenSecondaryIndex(INDEX_NAME);

  Record record=randomRecord();
  char searchName[15];
  strcpy(searchName, record.name);

  for (int id = 0; id < RECORDS_NUM; ++id) {
      record = randomRecord();
      int block_id = HT_InsertEntry(info, record);
      SHT_SecondaryInsertEntry(index_info, record, block_id);
  }

  // Εκτυπώνουμε τα στατιστικά του δευτερευόντος ευρετηρίου
  HashStatistics(INDEX_NAME);

  SHT_CloseSecondaryIndex(index_info);
  HT_CloseFile(info);

  BF_Close();
}


