//
//  splitfile.c
//  C-MapReduce
//
//  Created by jeffrey on 1/12/15.
//  Copyright © 2015 jeffrey. All rights reserved.
//
#include <stdio.h>
#include <stdlib.h>
#include<string.h>

/*
 * Put all GLOBAL variables here
 */
const int MAX_WORDS_PER_FILE = 5; // maximum words for split

char* getSplitFilename(int count) {
    char *splitName = "split_";
    //printf("%lu\n", strlen(splitName));
    
    char splitCount = '0' + count;
    //printf("%lu\n", sizeof(splitCount));
    
    char *fExt = ".txt";
    //printf("%lu\n", strlen(fExt));
    
    char *newName = (char *) malloc(strlen(splitName) + 2 + strlen(fExt)); //one for extra char, one for trailing zero
    //printf("%lu\n", strlen(newName));
    
    strcpy(newName, splitName);
    newName[strlen(splitName)] = splitCount;
    newName[strlen(splitName)+1] = '\0';
    strcat(newName, fExt);
    //printf("%lu\n", strlen(newName));
    //printf("--> %s\n", newName);
    
    return newName;
    free(newName);
}

int splitfile() {
	FILE *fpIn;
    fpIn = fopen("input.txt", "r");
    
    // counter for number of split
    int splitCount = 1;
    // prepare the split file Handle
    FILE *fpOut;
    fpOut = fopen(getSplitFilename(splitCount), "w+");
    
    char temp;
    char *string;
    int charCount = 0;
    int wordCount = 0;
    
    while ((temp = fgetc(fpIn)) != EOF) {
        //printf("[%c]\n", temp);
        
        if (temp == '\n' || temp == ' ' || temp == '?' || temp == ',' || temp == '.' || temp == '\"' ||
            temp == '!' || temp == '@' || temp == '~' || temp == '#' || temp == '$' || temp == '%' ||
            temp == '%' || temp == '^' || temp == '&' || temp == '*' || temp == '(' || temp == ')' ||
            temp == '-' || temp == '_' || temp == '{' || temp == '}' || temp == '[' || temp == ']' ||
            temp == '<' || temp == '>' || temp == '\r' || temp == '/' || temp == '\'' || temp == '`'
        ){
            if (charCount > 0) {
                string[strlen(string)] = '\0'; //add the null-termination character '\0'
                printf("[%s]-->\n",string);
                wordCount ++;
                
                //printf("@@ write to split file: %s\n", getSplitFilename(splitCount));
                fprintf(fpOut, "%s\n", string);
                
                if (wordCount % MAX_WORDS_PER_FILE == 0) {
                    // need to split, increment the split counter by 1
                    splitCount++;
                    
                    // close the file handle and re-open to next split file
                    fclose(fpOut);
                    fpOut = fopen(getSplitFilename(splitCount), "w+");
                }
                
                charCount = 0;
                free(string);
                string = calloc(0, sizeof(char));
            }
        } else {
            if (charCount == 0) {
                string = malloc(sizeof(char));
            }
            string[charCount] = temp;
            string[strlen(string)] = '\0'; //add the null-termination character '\0'
            charCount ++;
        }
    }
    
    free(string);
    fclose(fpIn);
    fclose(fpOut);
    
    return(splitCount);
}
