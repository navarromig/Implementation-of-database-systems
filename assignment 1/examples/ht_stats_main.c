#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "ht_table.h"
#include "stats.h"

#define RECORDS_NUM 10256 
#define FILE_NAME "data.db"

int main() {

  remove("data.db") ;

  BF_Init(LRU);

  HT_CreateFile(FILE_NAME,10);
  HT_info* info = HT_OpenFile(FILE_NAME);

  Record record;
  srand(1234);

  for (int id = 0; id < RECORDS_NUM; ++id) {
    record = randomRecord();
    HT_InsertEntry(info, record);
  }

  // Εκτυπώνουμε τα στατιστικά του πρωτεύοντος ευρετηρίου
  HT_Statistics(FILE_NAME) ;
	
  HT_CloseFile(info);
  BF_Close();
}
