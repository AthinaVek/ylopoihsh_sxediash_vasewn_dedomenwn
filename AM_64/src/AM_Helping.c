#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "AM.h"
#include "bf.h"
#include "AM_Helping.h"

//TODO: Change definition to int compareKeys(char type, void* op1, void* op2, int operator)
int compareKeys(char type, void* current_key, void* value1, int operator) {
  int *key_int, *current_key_int;
  float *key_float, *current_key_float;
  char *key_string, *current_key_string;
  int temp;

  switch(type) {
    case 'i':
      current_key_int = (int*)current_key;
      key_int = (int*)value1;
      //printf("current_key_int = %d and key_int = %d\n", *current_key_int, *key_int);
      switch(operator) {
        case EQUAL:
            return (*current_key_int = *key_int);
        case NOT_EQUAL:
            return (current_key_int != *key_int);
        case LESS_THAN:
            return (*current_key_int < *key_int);
        case GREATER_THAN:
            return (*current_key_int > *key_int);
        case LESS_THAN_OR_EQUAL:
            return (*current_key_int <= *key_int);
        case GREATER_THAN_OR_EQUAL:
            return (*current_key_int >= *key_int);
        default:
            return -1;
      }
      break;
    case 'f':
      current_key_float = (float*)current_key;
      key_float = (float*)value1;
      // printf("current_key_string = %f and key_string = %f\n", *current_key_float, *key_float);
      switch(operator) {
        case EQUAL:
            return (*current_key_float = *key_float);
        case NOT_EQUAL:
            return (current_key_float != *key_float);
        case LESS_THAN:
            return (*current_key_float < *key_float);
        case GREATER_THAN:
            return (*current_key_float > *key_float);
        case LESS_THAN_OR_EQUAL:
            return (*current_key_float <= *key_float);
        case GREATER_THAN_OR_EQUAL:
            return (*current_key_float >= *key_float);
        default:
            return -1;
      }
      break;
    case 'c':
      current_key_string = (char*)current_key;
      key_string = (char*)value1;
      //printf("current_key_string = %s and key_string = %s\n", current_key_string, key_string);
        case EQUAL:
            return (strcmp(current_key_string, key_string) = 0);
        case NOT_EQUAL:
            return (strcmp(current_key_string, key_string) != 0);
        case LESS_THAN:
            return (strcmp(current_key_string, key_string) < 0);
        case GREATER_THAN:
            return (strcmp(current_key_string, key_string) > 0);
        case LESS_THAN_OR_EQUAL:
            return (strcmp(current_key_string, key_string) <= 0);
        case GREATER_THAN_OR_EQUAL:
            return (strcmp(current_key_string, key_string) >= 0);

      switch(operator) {
        case LESS_THAN:
          return (strcmp(current_key_string, value1) < 0);
        case GREATER_THAN_OR_EQUAL:
          return (strcmp(current_key_string, value1) >= 0);
        default:
          return -1;
      }
      break;
    default:
      printf("error in type\n");
  }
}


int insertData(BF_Block *Block, void *value1, void *value2, AM_Metadata *metadata){
  char *data, *freePos;
  int recUsed, nextDataBlock;
  void *key;
  int i;

  //Allocate memory for key
  key = malloc(metadata->attrLength1);

  data = BF_Block_GetData(Block);

  //Load block's metadata
  memcpy(&recUsed, data, sizeof(int));
  data += sizeof(int);
  memcpy(&nextDataBlock, data, sizeof(int));
  data += sizeof(int);

  printf("recUsed = %d \t dataBlockCap = %d\n", recUsed, metadata->dataBlockCap);

  //Set freePos to the first free record position
  freePos = data + recUsed * (metadata->attrLength1 + metadata->attrLength2);
  //Set data to the last record of the block
  data = freePos - (metadata->attrLength1 + metadata->attrLength2);

  if(recUsed > 0){
    i = recUsed - 1;
    memcpy(key, data, metadata->attrLength1);
    //anazitisi swstis thesis gia eggrafi se taksinomimeno block
    while(i >= 0 && compareKeys(metadata->attrType1, value1, key, LESS_THAN)){
      //metaferoume ta dedomena mia thesi meta
      memcpy (freePos, data, metadata->attrLength1 + metadata->attrLength2);

      //pame stin proigoumeni eggrafi
      freePos -= metadata->attrLength1 + metadata->attrLength2;
      if(i != 0){
        data -= metadata->attrLength1 + metadata->attrLength2;
        memcpy(key, data, metadata->attrLength1);
      }

      i--;
    }
  }

  //Insert new record
  memcpy(freePos, value1, metadata->attrLength1);
  freePos += metadata->attrLength1;
  memcpy (freePos, value2, metadata->attrLength2);
  freePos += metadata->attrLength2;

  metadata->recordCount += 1;

  //Go back to the start of the block and update recUsed
  recUsed++;
  data = BF_Block_GetData(Block);
  memcpy(data, &recUsed, sizeof(int));

  free(key);
  return AME_OK;
}


int insertDir(BF_Block *Block, void *newKey, int pointer, AM_Metadata *metadata){
  char *data, *freePos;
  int pointersUsed;
  void *key;
  int i;

  //Allocate memory for the key
  key = malloc(metadata->attrLength1);

  //Load block's metadata
  data = BF_Block_GetData(Block);
  memcpy(&pointersUsed, data, sizeof(int));
  data += sizeof(int);

  //Pass first pointer
  data += sizeof(int);
  //Set freePos to the first free key position
  freePos = data + (pointersUsed - 1) * (metadata->attrLength1 + sizeof(int));
  //Set data to the last key of the block
  data = freePos - (metadata->attrLength1 + sizeof(int));

  i = pointersUsed - 2;
  memcpy(key, data, metadata->attrLength1);
  //anazitisi swstis thesis gia eggrafi se taksinomimeno block
  while(i >= 0 && compareKeys(metadata->attrType1, newKey, key, LESS_THAN)){
    //metaferoume ta dedomena mia thesi meta
    memcpy (freePos, data, metadata->attrLength1 + sizeof(int));
    //pame stin proigoumeni eggrafi
    data -= metadata->attrLength1 + sizeof(int);
    freePos -= metadata->attrLength1 + sizeof(int);

    memcpy(key, data, metadata->attrLength1);
    i--;
  }

  //Insert new key + pointer
  memcpy(freePos, newKey, metadata->attrLength1);
  data += metadata->attrLength1;
  memcpy (freePos, &pointer, sizeof(int));
  data += sizeof(int);

  //Go back to the start of the block and update pointersUsed
  data = BF_Block_GetData(Block);
  pointersUsed++;
  memcpy(data, &pointersUsed, sizeof(int));

  free(key);
  return AME_OK;
}


int getFirstDataKey(BF_Block *Block, void *key, AM_Metadata *metadata){
  char *data = BF_Block_GetData(Block);

  //Pass metadata
  data += 2 * sizeof(int);

  //Load first key
  memcpy(key, data, metadata->attrLength1);

  return AME_OK;
}


int setMetaDir(BF_Block *Block, int pointersUsed){
  char *data;

  data = BF_Block_GetData(Block);
  memcpy(data, &pointersUsed, sizeof(int));

  return AME_OK;
}
