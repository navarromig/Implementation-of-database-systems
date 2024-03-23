#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "ht_table.h"

#define CALL_BF_1(call)       \
;{                           \
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

int HT_Bucket_statistics(int, int, int, int*);  // calculates the number of blocks and records of a given bucket

int HT_Statistics(char *filename){

	int fd ;
	char *data ;
	BF_Block *block_0;
	CALL_BF_1(BF_OpenFile(filename,&fd)) ;
	BF_Block_Init(&block_0) ;
	CALL_BF_1(BF_GetBlock(fd,0,block_0)) ;			//Getting block 0 to read HT_info
	data = BF_Block_GetData(block_0) ;
	HT_info *info = (HT_info *) data;
	if (info->type_file != 2){					
		printf("Wrong file type %d\n", info->type_file);
		return -1;
	}


	int total_blocks;
	CALL_BF_1(BF_GetBlockCounter(info->fileDesc,&total_blocks));
	printf("Total blocks of file: %d\n",total_blocks);
	int overflow_buckets = 0;				//Number of buckets that have overflow blocks
	int sum_blocks = 0;
	int sum_records = 0;
	int min;
	int id_min;
	int max = -1;
	int id_max;
	int num_records;
	int empty = 0;

	for(int i=0; i < info->numBuckets; i++){
		printf("\n\n-Bucket %d- \n",i);
		int num_blocks = HT_Bucket_statistics(info->hash_table[i], info->fileDesc, info->block_capacity, &num_records);
		if (num_blocks == -1){
			empty++;
			continue;
		}
		if (num_blocks > 1){   			//Bucket has more than one block, overflow has occured
			overflow_buckets++;
		}
		if (i == 0){
			min = num_records;
			id_min = 0;
		}
		else if (min > num_records){
			min = num_records;
			id_min = i;
		}
		if (max < num_records){
			max = num_records;
			id_max = i;
		}
		sum_blocks += num_blocks;
		sum_records += num_records;

		printf("Records : %d \nOverflow blocks : %d\n",num_records, num_blocks-1);
	}
	double avg_block = (double)sum_blocks / (info->numBuckets - empty);
	double avg_record = (double)sum_records / (info->numBuckets - empty);
	printf("\n\nAverage blocks per bucket: %f\nAverage records per bucket: %f\n",avg_block, avg_record);
	printf("Min records %d at Bucket %d\n", min, id_min);
	printf("Max records %d at Bucket %d\n", max, id_max);

	printf("Buckets with overflow blocks: %d\n\n",overflow_buckets);
	BF_Block_Destroy(&block_0) ;  
}

int HT_Bucket_statistics(int block_id, int fd, int block_capacity, int* record_count){
	if (block_id == -1){  //Invalid block id
		printf("Empty bucket\n");
		*record_count = 0;
		return -1;
	}
	BF_Block *block;
	BF_Block_Init(&block);
	char* data;
	HT_block_info *block_info;
	int offset = BF_BLOCK_SIZE - sizeof(HT_block_info);
	int block_count = 0; 
	*record_count = 0;
	while (block_id != -1){         // Taversing blocks of bucket
		block_count++;
		CALL_BF_1(BF_GetBlock(fd,block_id,block));
		data = BF_Block_GetData(block);
		block_info = (HT_block_info*)(data + offset);
		*record_count += block_info->records;
		block_id = block_info->next_block
		CALL_BF_1(BF_UnpinBlock(block));
	}
	
	return block_count;

}