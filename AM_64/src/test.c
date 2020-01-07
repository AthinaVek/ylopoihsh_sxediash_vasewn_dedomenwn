#include <stdio.h>
#include "Stack.h"

int main(void)
{
    Stack s;
    int n;

    s.top = -1;
    push(&s, 1);
    push(&s, 2);
    n = pop(&s);
    printf("%d\n", n);
    return 0;
}
