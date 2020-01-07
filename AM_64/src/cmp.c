#include <stdio.h>
#include <string.h>

#define EQUAL 1
#define NOT_EQUAL 2
#define LESS_THAN 3
#define GREATER_THAN 4
#define LESS_THAN_OR_EQUAL 5
#define GREATER_THAN_OR_EQUAL 6

//dexetai ton typo tou key, to current_key, to value1 kai enan telesti sygkrisis kai epistrefei tin praksi sygkrisis

int type_translator (char type, void* current_key, void* value1, int operator) {
    int *key_int, *current_key_int;
    float *key_float, *current_key_float;
    char *key_string, *current_key_string;
    int temp;

    switch(type) {
        case 'i':
            current_key_int = (int*)current_key;
            key_int = (int*)value1;
            //printf("current_key_int = %d and key_int = %d\n", *current_key_int, *key_int);
            if (operator==6)
                return (*current_key_int >= *key_int);
            break;
        case 'f':
            current_key_float = (float*)current_key;
            key_float = (float*)value1;
            //printf("current_key_string = %f and key_string = %f\n", *current_key_float, *key_float);
            if (operator==6)
                return (*current_key_float >= *key_float);
            break;
        case 'c':
            current_key_string = (char*)current_key;
            key_string = (char*)value1;
            //printf("current_key_string = %s and key_string = %s\n", current_key_string, key_string);
            if (operator==6) {
                temp = strcmp(current_key_string, value1);
                //printf("temp = %d\n", temp);
                return temp>=0;
            }
            break;
        default:
            printf("error in type\n");
    }
}

int main (void) {
    int int_current_key = 10, int_value1 = 5;
    char int_type = 'i', string_type = 'c', float_type='f';
    char *s_current = "athina", *s_value1 = "markos";
    float float_current = 5.5, float_value1 = 10.1;
    if (type_translator(int_type, (void*)&int_current_key, (void*)&int_value1, 6))
        printf("int_type true\n");
    else
        printf("int_type false\n");

    if (type_translator(float_type, (void*)&float_current, (void*)&float_value1, 6))
	printf("float_type true\n");
    else
	printf("float_type false\n");

    if (type_translator(string_type, (void*)s_current, (void*)s_value1, 6))
        printf("string_type true\n");
    else
        printf("string_type false\n");


}
