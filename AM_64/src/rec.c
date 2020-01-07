#include <stdio.h>

int recursive(int depth, int maxdepth);

int main(void) {
    int depth = 1;
    printf ("%d\n",recursive(depth, 5));
}

int recursive(int depth, int maxdepth) {
    if (depth<maxdepth){
        printf("prin tin anadromi, depth = %d\n", depth);
        recursive(depth+1, maxdepth);
    }
    printf("meta tin anadromi, depth = %d\n", depth);
    return depth;
}
