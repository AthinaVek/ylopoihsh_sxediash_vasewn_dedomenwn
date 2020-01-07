#include "Stack.h"


void push(Stack *s, int num)
{
  if (s->top == MAXSIZE)
      printf ("ERROR: Stack is full.\n");
  else
  {
      s->top ++;
      s->stack[s->top] = num;
  }
}

int pop(Stack *s)
{
  int num;
  num = s->stack[s->top];
  s->top --;
  return(num);
}
