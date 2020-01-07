#include <stdio.h>

#define MAXSIZE 10000

typedef struct Stack
{
  int stack[MAXSIZE];
  int top;
}Stack;

void push(Stack*, int);
int pop(Stack*);
