#include <stdio.h>
#include <stdlib.h>
#include "AM.h"
#include "bf.h"
#include "string.h"

//Macro for handling errors when call BF functions
#define CALL_OR_ERROR(call){  \
    BF_ErrorCode code = call; \
    if (code != BF_OK){       \
      BF_PrintError(code);    \
			AM_errno = AME_BF_ERROR;\
      return AME_BF_ERROR;    \
    }                         \
}

#define AM_IDENTIFIER 22

//Struct for the metadata in the 1st block
typedef struct AM_Metadata{
	int identifier;
	int rootIndex;
	int depth;
	int blocksAllocated;
	int recordCount;
	char attrType1;
	int attrLength1;
	char attrType2;
	int attrLength2;
	int dirBlockCap;
	int dataBlockCap;
}AM_Metadata;

int AM_errno = AME_OK;
open_file open_files[MAXOPENFILES];
scan_file scan_files[MAXSCANS];

void AM_Init() {
  int i;

  BF_Init(LRU);

  //Initialize the values in the helping structs
  for(i = 0; i < MAXOPENFILES; i++){
    open_files[i].fd = -1;
  }

  for(i = 0; i < MAXSCANS; i++){
    scan_files[i].open_file_pos = -1;
  }

	return;
}


int AM_CreateIndex(char *fileName, char attrType1, int attrLength1, char attrType2, int attrLength2) {
	AM_Metadata metadata;
	int pointersUsed;

	int fileDesc;
	char *data;

	BF_Block *block;
	BF_Block_Init(&block);

	CALL_OR_ERROR(BF_CreateFile(fileName));
	CALL_OR_ERROR(BF_OpenFile(fileName, &fileDesc));
	CALL_OR_ERROR(BF_AllocateBlock(fileDesc, block));
	data = BF_Block_GetData(block);

	//Write metadata to block
	metadata.identifier = AM_IDENTIFIER;
	metadata.rootIndex = 2;
	metadata.depth = 1;
	metadata.blocksAllocated = 2;
	metadata.recordCount = 0;
	metadata.attrType1 = attrType1;
	metadata.attrLength1 = attrLength1;
	metadata.attrType2 = attrType2;
	metadata.attrLength2 = attrLength2;
	metadata.dirBlockCap = (BF_BLOCK_SIZE - 2 * sizeof(int)) / (sizeof(int) + attrLength2);
	metadata.dataBlockCap = (BF_BLOCK_SIZE - 2 * sizeof(int)) / (attrLength1 + attrLength2);

	memcpy(data, &metadata, sizeof(AM_Metadata));

	BF_Block_SetDirty(block);
	CALL_OR_ERROR(BF_UnpinBlock(block));

	//Allocate and initialize the root
	CALL_OR_ERROR(BF_AllocateBlock(fileDesc, block));
	data  = BF_Block_GetData(block);
	pointersUsed = 0;
	memcpy(data, &pointersUsed, sizeof(int));

	BF_Block_SetDirty(block);
	CALL_OR_ERROR(BF_UnpinBlock(block));

	BF_Block_Destroy(&block);
	CALL_OR_ERROR(BF_CloseFile(fileDesc));

  return AME_OK;
}


int AM_DestroyIndex(char *fileName) {
	int i;

  for(i = 0; i < MAXOPENFILES; i++){
    if(open_files[i].fd != -1){
      if(strcmp(open_files[i].fileName, fileName) == 0){
        return AME_DEL_OPEN;
      }
    }
  }

	if(remove(fileName) == 0){
		return AME_OK;
	}
	else{
		return AME_DEL_ERROR;
	}
}


int AM_OpenIndex (char *fileName) {
  int pos;
  int fileDesc;

  //Find a free position for the new file
  for(pos = 0; pos < MAXOPENFILES; pos++){
    if(open_files[pos].fd == -1){
      break;
    }
  }
  if(pos == MAXOPENFILES){
    return AME_OPEN_FULL;
  }

  //Open file and save info in the struct
  CALL_OR_ERROR(BF_OpenFile(fileName, &fileDesc));

  open_files[pos].fd = fileDesc;

  open_files[pos].fileName = malloc(sizeof(char) * (strlen(fileName) + 1));
  if(open_files[pos].fileName == NULL){
    return AME_MALLOC_ERROR;
  }
  strcpy(open_files[pos].fileName, fileName);

  return pos;
}


int AM_CloseIndex (int fileDesc) {
  int i;

  for(i = 0; i < MAXOPENFILES; i++){
    if(open_files[i].fd == fileDesc){
      //Close file, free the string and mark position as available
      CALL_OR_ERROR(BF_CloseFile(open_files[i].fd));
      open_files[i].fd = -1;
      free(open_files[i].fileName);
    }
  }
  return AME_OK;
}


int AM_InsertEntry(int fileDesc, void *value1, void *value2) {
	AM_Metadata metadata;
	char *data, *dataPtr;
	int i;

	BF_Block *indexBlock, *dataBlock;
	BF_Block_Init(&indexBlock);

	CALL_OR_ERROR(BF_GetBlock(fileDesc, 0, indexBlock));
	data = BF_Block_GetData(indexBlock);
	memcpy(&metadata, data, sizeof(AM_Metadata));
	CALL_OR_ERROR(BF_UnpinBlock(indexBlock));

	int nextBlock = metadata.rootIndex; //pairnoume to pou vrisketai i riza
	int depth = 1;
	int pointersUsed, pointer, value, recUsed, curKey, *key = value1;
    
    printf("metadata.depth = %d\n", metadata.depth);

	while(depth < metadata.depth){   //diatrexei to dentro mexri to datablock pou theloume
		CALL_OR_ERROR(BF_GetBlock(fileDesc, nextBlock, indexBlock));
		data = BF_Block_GetData(indexBlock);

		memcpy(&pointersUsed, data, sizeof(int));

        int *v1 = (int*)value1; 
		i = 0;
            
        do{    //diatrexoume to block eurethriou
			data += sizeof(int);
			memcpy(&pointer, data, sizeof(int));
			data += sizeof(*value1);
			memcpy(&value, data, sizeof(*value1));
			i++;
		}while(i < pointersUsed && *v1 >= value);


		CALL_OR_ERROR(BF_UnpinBlock(indexBlock));
		nextBlock = pointer;
		depth++;
	}

	CALL_OR_ERROR(BF_GetBlock(fileDesc, nextBlock, dataBlock));
	data = BF_Block_GetData(dataBlock);
	memcpy(&recUsed, data, sizeof(int));
    
    if (recUsed < metadata.recordCount) {   //an exei xwro to datablock
        i = recUsed;
        dataPtr = data;
        dataPtr += (recUsed-1)*(sizeof(*value1) + sizeof(*value2)) + sizeof(int);
        memcpy(&curKey, dataPtr, sizeof(*value1));

        while (i>0 && *key<curKey) { //anazitisi swstis thesis gia eggrafi se taksinomimeno block
            memcpy (dataPtr, dataPtr+sizeof(*value1)+sizeof(*value2), sizeof(*value1)+sizeof(*value2)); //metaferoume ta dedomena mia thesi meta
            dataPtr -= sizeof(*value1) + sizeof(*value2); //pame stin proigoumeni eggrafi
            i--;
            memcpy(&curKey, dataPtr, sizeof(*value1));
        }

        memcpy(&dataPtr, value1, sizeof(*value1));
        dataPtr += sizeof(*value1);
        memcpy (&dataPtr, value2, sizeof(*value2));
        memset(&data, recUsed+1, sizeof(int));
    }
    else {   //an den xwraei to spame
        //i = recUsed;
        dataPtr = data;
        newRecUsed = recUsed/2;
        dataPtr += newRecUsed*(sizeof(*value1)+sizeof(*value2));
        memset(data, newRecUsed, sizeof(int)); //grafoume to neo arithmo eggrafwn sto block pou idi yparxei
        
        //ftiaxnoume to neo block
        CALL_OR_ERROR(BF_AllocateBlock(fileDesc, childblock));
	    newdata = BF_Block_GetData(childblock);
        memset(newdata, recUsed-newRecUsed, sizeof(int)); //grafoume posa records yparxoun sto neo block
        memcpy(newdata, dataPtr, (recUsed-newRecUsed)*(sizeof(*value1)+sizeof(*value2))); //antigafoume ta dedomena

    }
    return AME_OK;
}



int AM_OpenIndexScan(int fileDesc, int op, void *value) {
  return AME_OK;
}
void *AM_FindNextEntry(int scanDesc) {
}
int AM_CloseIndexScan(int scanDesc) {
  return AME_OK;
}
void AM_PrintError(char *errString) {
	switch(AM_errno){
		case AME_OK:
			printf("%sEverything is OK.\n", errString);
			break;
		case AME_EOF:
		//TODO: Find out when this error code is used
			printf("%s\n", errString);
			break;
		case AME_BF_ERROR:
			printf("%sBlockFile level error!\n", errString);
			break;
		case AME_DEL_ERROR:
			printf("%sError trying to delete a file!\n", errString);
			break;
    case AME_DEL_OPEN:
			printf("%sTrying to delete an open file!\n", errString);
			break;
    case AME_OPEN_FULL:
			printf("%sCannot open more files!\n", errString);
			break;
	}
}

void AM_Close() {
  //Check for indexes not closed and free their strings?
}
