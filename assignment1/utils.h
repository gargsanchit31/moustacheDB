/*
 * utils.h
 */

#ifndef UTILS_H__
#define UTILS_H_

#define DEBUG 1
#define IFDEBUG if(DEBUG){ 
#define ENDBUG	}


int display(char *filename);
int create(char *filename, unsigned int noofattr, int attributes[][2]);
int insert(char *filename, char* attr_values[]);
#endif /* UTILS_H_ */
