#ifndef AM_HELPING_H_
#define AM_HELPING_H_

//dexetai ton typo tou key, to current_key, to value1 kai
//enan telesti sygkrisis kai epistrefei tin praksi sygkrisis
int compareKeys(char type, void* current_key, void* value1, int operator);

// inserts the record <value1,value2> in the given block
//while keeping the records sorted. It expects an initialized block.
//it does not set the block dirty nor does it unpin it or destroy it.
int insertData(block *Block, void *value1, void *value2, AM_Metadata *metadata);

//loads the first key of a data block
int getFirstDataKey(block *Block, void *key, AM_Metadata *metadata);

#endif /* AM_HELPING_H_ */
