#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "AM.h"
#include "bf.h"
#include "AM_Helping.h"
#include "Stack.h"

//Macro for handling errors when call BF functions
#define CALL_OR_ERROR(call)   \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK){       \
      BF_PrintError(code);    \
			AM_errno = AME_BF_ERROR;\
      return AME_BF_ERROR;    \
    }                         \
  }


#define CALL_OR_NULL(call)   \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK){       \
      BF_PrintError(code);    \
			AM_errno = AME_BF_ERROR;\
      return NULL;    \
    }                         \
  }

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
    scan_files[i].fd = -1;
  }

	return;
}


int AM_CreateIndex(char *fileName, char attrType1, int attrLength1, char attrType2, int attrLength2) {
	AM_Metadata metadata;
	int recUsed, nextDataBlock;

	int fileDesc;
	char *data;

  if(attrType1 == 'i' || attrType1 == 'f'){
    if (attrLength1 != 4){
      return AME_LEN_ERROR;
    }
  }

  if(attrType2 == 'i' || attrType2 == 'f'){
    if (attrLength2 != 4){
      return AME_LEN_ERROR;
    }
  }

	BF_Block *block;
	BF_Block_Init(&block);

	CALL_OR_ERROR(BF_CreateFile(fileName));
	CALL_OR_ERROR(BF_OpenFile(fileName, &fileDesc));
	CALL_OR_ERROR(BF_AllocateBlock(fileDesc, block));
	data = BF_Block_GetData(block);

	//Write metadata to block
	metadata.identifier = AM_IDENTIFIER;
	metadata.rootIndex = 1;
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

	//Allocate and initialize the first data block
	CALL_OR_ERROR(BF_AllocateBlock(fileDesc, block));
	data  = BF_Block_GetData(block);
	recUsed = 0;
  nextDataBlock = -1;
	memcpy(data, &recUsed, sizeof(int));
  data += sizeof(int);
  memcpy(data, &nextDataBlock, sizeof(int));

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
      //TODO: Maybe this will try to close a closed file? (if a file is open more than once)
      open_files[i].fd = -1;
      free(open_files[i].fileName);
    }
  }
  return AME_OK;
}


int AM_InsertEntry(int fileDesc, void *value1, void *value2) {
  AM_Metadata metadata;
  Stack stack;
	char *data, *newData, *freePos;
	int i;
  int nextBlock, depth;

  void *key, *lkey, *rkey, *lDirKey, *rDirKey;
  int pointer, lpointer, rpointer;

  int pointersUsed;
  int newPointersUsed;
  int recUsed, nextDataBlock;
  int newRecUsed, newNextDataBlock;

  BF_Block *Block, *NewBlock;
	BF_Block_Init(&Block);
  BF_Block_Init(&NewBlock);

  //TODO: Is this the real fileDesc or should we look it up in the open_file table???
  fileDesc = open_files[fileDesc].fd;

	CALL_OR_ERROR(BF_GetBlock(fileDesc, 0, Block));
	data = BF_Block_GetData(Block);
	memcpy(&metadata, data, sizeof(AM_Metadata));
  CALL_OR_ERROR(BF_UnpinBlock(Block));

  //ALlocate memory for keys
  key = malloc(metadata.attrLength1);
  lkey = malloc(metadata.attrLength1);
  rkey = malloc(metadata.attrLength1);
  rDirKey = malloc(metadata.attrLength1);
  lDirKey = malloc(metadata.attrLength1);

  nextBlock = metadata.rootIndex; //pairnoume to pou vrisketai i riza
  depth = 1;
  //Katevasma mexri ta dedomena kratontas to monopati
  //Sto telos tou while to nextBlock tha deixnei sto katallilo data block
  while(depth < metadata.depth){
    //Keep our descend path in the stack
    push(&stack, nextBlock);

    CALL_OR_ERROR(BF_GetBlock(fileDesc, nextBlock, Block));
		data = BF_Block_GetData(Block);
		memcpy(&pointersUsed, data, sizeof(int));
    data += sizeof(int);

		i = 0;
    do{
      //diatrexoume to block eurethriou
			memcpy(&pointer, data, sizeof(int));
			data += sizeof(int);

			memcpy(key, data, metadata.attrLength1);
      data += metadata.attrLength1;

			i++;
		}while(i < pointersUsed && compareKeys(metadata.attrType1, value1, key, GREATER_THAN_OR_EQUAL));

    if(i == pointersUsed && compareKeys(metadata.attrType1, value1, key, GREATER_THAN_OR_EQUAL)){
      //If the value to be inserted is >= than the last key of the dir block
      //Then we must follow the last pointer of the block
      memcpy(&pointer, data, sizeof(int));
    }

    nextBlock = pointer;

    CALL_OR_ERROR(BF_UnpinBlock(Block));
		depth++;
	}

  //Load the data block
  CALL_OR_ERROR(BF_GetBlock(fileDesc, nextBlock, Block));
	data = BF_Block_GetData(Block);
  memcpy(&recUsed, data, sizeof(int));
  data += sizeof(int);
  memcpy(&nextDataBlock, data, sizeof(int));
  data += sizeof(int);

  //An xwraei sto block eisagwgi kai telos
  if(recUsed < metadata.dataBlockCap){
    insertData(Block, value1, value2, &metadata);

    BF_Block_SetDirty(Block);
    CALL_OR_ERROR(BF_UnpinBlock(Block));
  }
  //An de xwraei spasimo tou data block se 2 kai isomerismos
  else{
    //ftiaxnoume to neo block
    CALL_OR_ERROR(BF_AllocateBlock(fileDesc, NewBlock));
    metadata.blocksAllocated += 1;

    newRecUsed = recUsed/2;
    recUsed = recUsed - newRecUsed;

    //Syndesi twn block
    newNextDataBlock = nextDataBlock;
    nextDataBlock = metadata.blocksAllocated - 1;

    //Update data blocks' metadata
    data = BF_Block_GetData(Block);
    newData = BF_Block_GetData(NewBlock);

    memcpy(data, &recUsed, sizeof(int));
    data += sizeof(int);
    memcpy(newData, &newRecUsed, sizeof(int));
    newData += sizeof(int);

    memcpy(data, &nextDataBlock, sizeof(int));
    data += sizeof(int);
    memcpy(newData, &newNextDataBlock, sizeof(int));
    newData += sizeof(int);

    //Copy records to new block
    data += recUsed * (metadata.attrLength1 + metadata.attrLength2);
    memcpy(newData, data, newRecUsed * (metadata.attrLength1 + metadata.attrLength2));

    //Eisagwgi sto katallilo block

    //an to key einai mikrotero tou prwtou stoixeiou tou kainourgiou block
    //eisagoume tin eggrafi sto palio block
    getFirstDataKey(NewBlock, key, &metadata);

    if(compareKeys(metadata.attrType1, key, value1, GREATER_THAN)) {
      //pame ta data sto deksi block
      insertData(Block, value1, value2, &metadata);
    }
    else{
      insertData(NewBlock, value1, value2, &metadata);
    }

    //anevasma key+pointer tou aristerou stoixeiou twn 2 block
    //TODO: Make sure pointers are correct
    getFirstDataKey(Block, lkey, &metadata);
    lpointer = nextBlock;
    getFirstDataKey(NewBlock, rkey, &metadata);
    rpointer = nextDataBlock;

    depth = metadata.depth;
    //oso exoume key+pointer pou prepei na anebei
    while(rpointer != -1){
      //an yparxei dir block apo panw
      if(depth > 1){
        nextBlock = pop(&stack);
        CALL_OR_ERROR(BF_GetBlock(fileDesc, nextBlock, Block));
        data = BF_Block_GetData(Block);
        memcpy(&pointersUsed, data, sizeof(int));
        data += sizeof(int);

        //eisagwgi sto dir block an yparxei kai an xwraei (mono tou deksiou)
        if(pointersUsed < metadata.dirBlockCap){
          insertDir(Block, rkey, rpointer, &metadata);
          BF_Block_SetDirty(Block);
          CALL_OR_ERROR(BF_UnpinBlock(Block));
          lpointer = rpointer = -1;
        }
        //an einai gemato, spasimo sta 2 kai anebasma meseou key+pointer (mono deksi)
        else{
          CALL_OR_ERROR(BF_AllocateBlock(fileDesc, NewBlock));
          metadata.blocksAllocated += 1;
          newData = BF_Block_GetData(NewBlock);
          //Pass metadata
          newData += sizeof(int);

          newPointersUsed = pointersUsed/2 + 1;
          pointersUsed = pointersUsed - newPointersUsed + 1;

          //pass first pointer
          data += sizeof(int);
          //go to left block's last key
          data += (pointersUsed - 2) * (metadata.attrLength1 + sizeof(int));
          //load left block's last key
          memcpy(lDirKey, data, metadata.attrLength1);
          //go to right block's first key
          data += metadata.attrLength1 + sizeof(int);
          //load right block's first key
          memcpy(rDirKey, data, metadata.attrLength1);


          if(compareKeys(metadata.attrType1, rkey, lDirKey, LESS_THAN)){
            //rkey+rpointer will be inserted in the left block
            //lDirKey will be moved up with the pointers of the left + right block

            //Go to the pointer of lDirKey
            data -= sizeof(int);

            //Copy pointers and keys to new block
            memcpy(newData, data, newPointersUsed * (metadata.attrLength1 + sizeof(int)) - metadata.attrLength1);
            setMetaDir(NewBlock, newPointersUsed);

            //Remove last key+pointer from left block
            pointersUsed--;
            setMetaDir(Block, pointersUsed);

            //Insert rkey+rpointer to the left block
            insertDir(Block, rkey, rpointer, &metadata);

            //Move  lDirKey up
            memcpy(rkey, lDirKey, metadata.attrLength1);
          }
          else if(compareKeys(metadata.attrType1, rkey, rDirKey, GREATER_THAN)){
            //rkey+rpointer will be inserted in the right block
            //rDirKey will be moved up with the pointers of the left + right block

            //Go to the pointer of rDirKey
            data += metadata.attrLength1;

            //Copy pointers and keys to new block
            newPointersUsed--;
            memcpy(newData, data, newPointersUsed * (metadata.attrLength1 + sizeof(int)) - metadata.attrLength1);
            setMetaDir(NewBlock, newPointersUsed);

            setMetaDir(Block, pointersUsed);

            //Insert rkey+rpointer to the right block
            insertDir(NewBlock, rkey, rpointer, &metadata);

            //Move rDirKey up
            memcpy(rkey, rDirKey, metadata.attrLength1);
          }
          else{
            //rkey is exactly in the middle
            //rkey will be moved up with the pointers of the left + right block

            //the rpointer will become the first pointer of the new block
            memcpy(newData, &rpointer, sizeof(int));
            newData += sizeof(int);

            //Copy pointers and keys to new block
            memcpy(newData, data, (newPointersUsed - 1) * (metadata.attrLength1 + sizeof(int)));

            setMetaDir(Block, pointersUsed);
            setMetaDir(NewBlock, newPointersUsed);
          }

          lpointer = nextBlock;
          rpointer = metadata.blocksAllocated - 1;

          BF_Block_SetDirty(Block);
          BF_Block_SetDirty(NewBlock);

          BF_UnpinBlock(Block);
          BF_UnpinBlock(NewBlock);
        }
      }
      //an den yparxei, dhmiourgia kai eisagwgi (aristerou + deksiou)
      else{
        CALL_OR_ERROR(BF_AllocateBlock(fileDesc, Block));
        metadata.blocksAllocated += 1;
        data = BF_Block_GetData(Block);

        pointersUsed = 2;
        memcpy(data, &pointersUsed, sizeof(int));
        data += sizeof(int);

        memcpy(data, &lpointer, sizeof(int));
        data += sizeof(int);

        memcpy(data, rkey, metadata.attrLength1);
        data += metadata.attrLength2;

        memcpy(data, &rpointer, sizeof(int));
        // data += sizeof(int);

        BF_Block_SetDirty(Block);
        BF_UnpinBlock(Block);

        //Update the root
        metadata.rootIndex = metadata.blocksAllocated - 1;

        //Den exoume pointer+key na anevasoume
        lpointer = rpointer = -1;
      }
      depth--;
    }
  }


  //Update metadata block
  CALL_OR_ERROR(BF_GetBlock(fileDesc, 0, Block))
  data = BF_Block_GetData(Block);
  memcpy(data, &metadata, sizeof(AM_Metadata));
  BF_Block_SetDirty(Block);
  CALL_OR_ERROR(BF_UnpinBlock(Block));

  BF_Block_Destroy(&Block);
  BF_Block_Destroy(&NewBlock);

  free(key);
  free(lkey);
  free(rkey);
  free(lDirKey);
  free(rDirKey);

  return AME_OK;
}


int AM_OpenIndexScan(int fileDesc, int op, void *value) {
  AM_Metadata metadata;
  char *data;
  int i, pos;
  int nextBlock, depth;
  int pointersUsed;
  int pointer;
  void *key, *value1;
  int recUsed, nextDataBlock;

  BF_Block *Block;
  BF_Block_Init(&Block);

  for(pos = 0; pos < MAXSCANS; pos++) {
    if (scan_files[pos].fd == -1) break;
  }

  if(pos == MAXSCANS) {
    return AME_SCAN_FULL;
  }
/*
  if (!(pos>=0 && pos<MAXOPENFILES))
    return
*/
  
  scan_files[pos].fd = fileDesc;

  fileDesc = open_files[fileDesc].fd;

  CALL_OR_ERROR(BF_GetBlock(fileDesc, 0, Block));
  data = BF_Block_GetData(Block);
  memcpy(&metadata, data, sizeof(AM_Metadata));
  CALL_OR_ERROR(BF_UnpinBlock(Block));

  nextBlock = metadata.rootIndex;
  depth = 1;
  if (op == EQUAL || op == GREATER_THAN || op == GREATER_THAN_OR_EQUAL) {
        while(depth < metadata.depth){
            CALL_OR_ERROR(BF_GetBlock(fileDesc, nextBlock, Block));
            data = BF_Block_GetData(Block);
            memcpy(&pointersUsed, data, sizeof(int));
            data += sizeof(int);

            i = 0;
            do{ //diatrexoume to block eurethriou
                memcpy(&pointer, data, sizeof(int));
                data += sizeof(int);
                memcpy(&key, data, metadata.attrLength1);
                data += metadata.attrLength1;

                i++;
            }while(i < pointersUsed && compareKeys (metadata.attrType1, key, value, GREATER_THAN_OR_EQUAL));

            if(i == pointersUsed && compareKeys (metadata.attrType1, key, value, GREATER_THAN_OR_EQUAL))
                memcpy(&pointer, data, sizeof(int));

            nextBlock = pointer;

            CALL_OR_ERROR(BF_UnpinBlock(Block));
            depth++;
        }
        //Load the data block
        CALL_OR_ERROR(BF_GetBlock(fileDesc, nextBlock, Block));
        data = BF_Block_GetData(Block);
        //memcpy(&recUsed, data, sizeof(int));
        data += 2*sizeof(int); //pername tis 2 arxikes times tou datablock
        //memcpy(&nextDataBlock, data, sizeof(int));
        //data += sizeof(int);

        memcpy(value1, data, sizeof(int));
        data += metadata.attrLength1 + metadata.attrLength2;
        i = 1;

        while(i < recUsed && compareKeys (metadata.attrType1, value1, value, op)) {
            //diatrexoume to block dedomenwn
            memcpy(value1, data, sizeof(int));
            data += metadata.attrLength1 + metadata.attrLength2;
            i++;
        }
        i--;
        CALL_OR_ERROR(BF_UnpinBlock(Block));
  }
  else { //se autin tin periptwsi prepei na pame sto proto datablock
        while(depth < metadata.depth){
            CALL_OR_ERROR(BF_GetBlock(fileDesc, nextBlock, Block));
            data = BF_Block_GetData(Block);
            data += sizeof(int);

            memcpy(&pointer, data, sizeof(int));
            nextBlock = pointer;

            CALL_OR_ERROR(BF_UnpinBlock(Block));
            depth++;
        }
        i=0;
  }

    if (op == GREATER_THAN) i++;

    //apothikeuoume ston pinaka to anagnoristiko tis teleutaias eggrafis
    scan_files[pos].last_block = nextBlock;
    scan_files[pos].last_record_num = i;
    scan_files[pos].op = op;
    scan_files[pos].value = value;
    return pos;
}


void *AM_FindNextEntry(int scanDesc) {
    int nextBlock, recUsed, nextDataBlock, i, fd;
    char* data;
    void *value2, *value1;
    AM_Metadata metadata;
    BF_Block *Block;
    BF_Block_Init(&Block);

    fd = scan_files[scanDesc].fd;
    fd = open_files[fd].fd;
    nextBlock = scan_files[scanDesc].last_block;
    CALL_OR_NULL(BF_GetBlock(fd, nextBlock, Block));
    data = BF_Block_GetData(Block);
    memcpy(&recUsed, data, sizeof(int));
    data += sizeof(int);
    memcpy(&nextDataBlock, data, sizeof(int));
    data += sizeof(int);

    //to sigkekrimeno block den exei alles eggrafes
    if (scan_files[scanDesc].last_record_num == recUsed) {
        //pigainoume sto epomeno datablock
        CALL_OR_NULL(BF_UnpinBlock(Block));
        CALL_OR_NULL(BF_GetBlock(fd, nextDataBlock, Block));
        data = BF_Block_GetData(Block);
        memcpy(&recUsed, data, sizeof(int));
        data += sizeof(int);
        memcpy(&nextDataBlock, data, sizeof(int));
        data += sizeof(int);
    }
    else {
        //phgainoume sti thesi pou itan i proigoumeni eggrafi pou diavasame
        for (i=0; i<scan_files[scanDesc].last_record_num; i++) {
            data += metadata.attrLength1 + metadata.attrLength2;
        }
    }

    memcpy(value1, data, metadata.attrLength1);
    if (compareKeys(metadata.attrType1, value1, scan_files[scanDesc].value, scan_files[scanDesc].op)) {
        data+=sizeof(metadata.attrLength1);
        memcpy(value2, data, metadata.attrLength2);
        return value2;
        scan_files[scanDesc].last_record_num++;
    }
    else {
        AM_errno = AME_EOF;
        return NULL;
    }
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
    case AME_MALLOC_ERROR:
			printf("%sMemory allocation error!\n", errString);
			break;
    case AME_SCAN_FULL:
			printf("%sCannot open more scans!\n", errString);
			break;
    case AME_LEN_ERROR:
			printf("%sInvalid length for given data type!\n", errString);
			break;
	}
}

void AM_Close() {
  //TODO: Check for indexes not closed and free their strings?
}
