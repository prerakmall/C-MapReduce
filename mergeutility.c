//
//  mergeutility.c
//  C-MapReduce
//
//  Created by jeffrey on 1/12/15.
//  Copyright © 2015 jeffrey. All rights reserved.
//
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>

/*
 * Put all GLOBAL variables here
 */
const int MAX_FILES_PER_MERGE = 2;
int totalMergeCount = 0;
int mergeCycle = 0;

FILE *fpOut;
FILE *fpIn;
int totalMergeWords = 0;
char* finalMergedOutputFile;

char* getMergeTask(int recordIndex) {
    printf("search for merge task: [%d]\n", recordIndex);
    fpIn = fopen("plan.txt", "r");
    
    char temp;
    char *string;
    int charCount = 0;
    int wordCount = 0;
    
    while ((temp = fgetc(fpIn)) != EOF) {
        //printf("[%c]\n", temp);
        
        if (temp == '\n') {
            //printf("[%s]-->\n",string);
            wordCount ++;
            
            if (wordCount == recordIndex) {
                break; // record is found, break the loop
            }
            
            charCount = 0;
            free(string);
            string = malloc(sizeof(char));
            
        } else {
            if (charCount == 0) {
                string = malloc(sizeof(char));
            }
            string[charCount] = temp;
            charCount ++;
        }
    }
    fclose(fpIn);
    
    string[strlen(string)] = '\0'; //add the null-termination character '\0'
    printf("merge task found: [%s]\n",string);
    return string;
    free(string);
}

char* generateMergePlan(char *inputArray) {
    char *outputArray;
    outputArray = (char *) malloc(sizeof(char));
    
    int inputFileCount;
    int tmpCounter;
    int mergedCount;
    
    inputFileCount = strlen(inputArray);
    //printf("--> %d\n", inputFileCount);
    tmpCounter = 0;
    mergedCount = 0;
    
    for (int i=0; i<inputFileCount; i++) {
        tmpCounter++;
        
        if (tmpCounter >= MAX_FILES_PER_MERGE) {
            totalMergeCount++; // increment the total merge operation count
            
            //printf("@ %d\n", mergedCount);
            outputArray [mergedCount] = '\0' + (totalMergeCount);
            mergedCount++;
            tmpCounter = 0;
            
            if (i < totalMergeCount) {
                fprintf(fpOut, "C%d ", inputArray[i]);
            } else {
                fprintf(fpOut, "B%d ", inputArray[i]);
            }
            
            fprintf(fpOut, "C%d \n", totalMergeCount);
            
        } else if (i == inputFileCount-1) {
            //printf("@@ %d\n", mergedCount);
            outputArray [mergedCount] = '\0' + inputArray[i];
            
        } else {
            if (i < totalMergeCount) {
                fprintf(fpOut, "C%d ", inputArray[i]);
            } else {
                fprintf(fpOut, "A%d ", inputArray[i]);
            }
        }
    }
    
    printf("output size: %lu\n", strlen(outputArray));
    for (int i=0; i<strlen(outputArray); i++) {
        printf("output: [%d]\n", outputArray[i]);
    }
    
    return outputArray;
    free(outputArray);
}

void preprocessingMerge(int inputSize) {
    printf("generate merge tasks planning:");
    
    char *inputArray;
    inputArray = (char *) malloc(sizeof(char));
    
    //debugger only, initialize the input array
    for (int i=0; i<inputSize; i++) {
        inputArray[i] = '\0' + (i+1);
    }
    printf("initial input size: %lu\n", strlen(inputArray));
    for (int i=0; i<strlen(inputArray); i++) {
        printf("input: [%d]\n", inputArray[i]);
    }
    
    while(strlen(inputArray) > 1) {
        mergeCycle++;
        printf("--> merge cycle: %d\n", mergeCycle);
        
        char *outputArray = (char *) calloc(0, sizeof(char));
        outputArray = generateMergePlan(inputArray);
        
        // re-create the input array using calloc which can reset the array length to 0
        free(inputArray);
        inputArray = (char *) calloc(0, sizeof(char));
        printf("input size: %lu\n", strlen(inputArray));
        
        // assign the output array to input array for next merge cycle
        for (int i=0; i<strlen(outputArray); i++) {
            inputArray[i] = outputArray[i];
        }
//        printf("input size: %lu\n", strlen(inputArray));
//        for (int i=0; i<strlen(inputArray); i++) {
//            printf("input: [%d]\n", inputArray[i]);
//        }
        
        free(outputArray);
    }
    printf("--> total merge operation planned(s): %d\n", totalMergeCount);
    
    free(inputArray);
}

int mergePlanner(int inputSize) {
    totalMergeWords = 0; // initialize the merged word count
    
    fpOut = fopen("plan.txt", "w+");
    preprocessingMerge(inputSize);
    fclose(fpOut);
    
    return totalMergeCount;
}

char* getFilename(char* param1, char* param2) {
    char *ext = ".txt";
    char *newName = (char *) malloc(strlen(param1) + strlen(param2) + strlen(ext));
    
    strcpy(newName, param1);
    strcat(newName, param2);
    strcat(newName, ext);
    
    //printf("%lu\n", strlen(newName));
    printf("--> %s\n", newName);
    
    return newName;
    free(newName);
}

void executeMerge(char *mergeTask) {
    printf("task: [%s]\n", mergeTask);
    printf("size: %lu\n", strlen(mergeTask));
    //printf("\n");
    
    // === decompose Input 1 and 2 and the Output ===
    int offset = 0; // initialize the offset
    char *temp; // temporary character array buffer
    
    // === grep the offset for input 1 ===
    offset = 0;
    temp = (char *) malloc(sizeof(char));
    temp = strstr(mergeTask, " ");
    if (temp != NULL) {
        offset = temp - mergeTask;
    }
    //printf("offset = %d\n", offset);
    
    // === parse input1 from the merge string using the offset ===
    char* input1 = (char *) malloc(sizeof(char));
    strncpy(input1, mergeTask, offset);
    input1[strlen(input1)] = '\0'; //add the null-termination character '\0'
    printf("input1 --> [%s]\n", input1);
    
    // === stripe off input1 from the merge string ===
    //printf("task: [%s]\n", mergeTask);
    temp = (char *) malloc(sizeof(char));
    strncpy(temp, mergeTask+offset+1, strlen(mergeTask));
    temp[strlen(temp)] = '\0'; //add the null-termination character '\0'
    mergeTask = temp; // assign temp back to mergeTask
    //printf("temp --> [%s]\n", temp);
    //printf("task: [%s]\n", mergeTask);
    //printf("\n");
    
    // === grep the offset for input 2 ===
    offset = 0;
    temp = (char *) malloc(sizeof(char));
    temp = strstr(mergeTask, " ");
    if (temp != NULL) {
        offset = temp - mergeTask;
    }
    //printf("offset = %d\n", offset);
    
    // === parse input1 from the merge string using the offset ===
    char* input2 = (char *) malloc(sizeof(char));
    strncpy(input2, mergeTask, offset);
    input2[strlen(input2)] = '\0'; //add the null-termination character '\0'
    printf("input2 --> [%s]\n", input2);
    
    // === stripe off input2 from the merge string ===
    //printf("task: [%s]\n", mergeTask);
    temp = (char *) malloc(sizeof(char));
    strncpy(temp, mergeTask+offset+1, strlen(mergeTask));
    temp[strlen(temp)] = '\0'; //add the null-termination character '\0'
    mergeTask = temp; // assign temp back to mergeTask
    //printf("temp --> [%s]\n", temp);
    //printf("task: [%s]\n", mergeTask);
    //printf("\n");
    
    // === grep the offset for output ===
    offset = 0;
    temp = (char *) malloc(sizeof(char));
    temp = strstr(mergeTask, " ");
    if (temp != NULL) {
        offset = temp - mergeTask;
    }
    //printf("offset = %d\n", offset);
    
    // === parse input1 from the merge string using the offset ===
    char* output = (char *) malloc(sizeof(char));
    strncpy(output, mergeTask, offset);
    output[strlen(output)] = '\0'; //add the null-termination character '\0'
    printf("output --> [%s]\n", output);
    
    // === stripe off input2 from the merge string ===
    //printf("task: [%s]\n", mergeTask);
    temp = (char *) malloc(sizeof(char));
    strncpy(temp, mergeTask+offset+1, strlen(mergeTask));
    temp[strlen(temp)] = '\0'; //add the null-termination character '\0'
    mergeTask = temp; // assign temp back to mergeTask
    //printf("temp --> [%s]\n", temp);
    //printf("task: [%s]\n", mergeTask);
    //printf("\n");
    
    // === retrieve the filenames ===
    char *fNameIn1;
    char *fNameIn2;
    char *fNameOut;
    bool isMerged;
    
    // === grep the offset of 1st char ===
    isMerged = false;
    offset = 0;
    temp = (char *) malloc(sizeof(char));
    temp = strstr(input1, "C");
    if (temp != NULL) {
        isMerged = true;
        offset = temp - input1;
    }
    //printf("offset = %d\n", offset);
    
    // === chop the 1st char from input 1 ===
    temp = (char *) malloc(sizeof(char));
    strncpy(temp, input1+1, strlen(input1));
    temp[strlen(input1)] = '\0'; //add the null-termination character '\0'
    input1 = temp; //assign temp back to input1
    //printf("input1 --> [%s]\n", input1);
    
    // === check the offset to determine the filename ===
    if (isMerged) {
        fNameIn1 = getFilename("reduced_", input1);
    } else {
        fNameIn1 = getFilename("sorted_output_split_", input1);
    }
    
    // === grep the offset of 1st char ===
    isMerged = false;
    offset = 0;
    temp = (char *) malloc(sizeof(char));
    temp = strstr(input2, "C");
    if (temp != NULL) {
        isMerged = true;
        offset = temp - input2;
    }
    //printf("offset = %d\n", offset);
    
    // === chop the 1st char from input 2 ===
    temp = (char *) malloc(sizeof(char));
    strncpy(temp, input2+1, strlen(input2));
    temp[strlen(input2)] = '\0'; //add the null-termination character '\0'
    input2 = temp; //assign temp back to input2
    //printf("input2 --> [%s]\n", input2);
    
    // === check the offset to determine the filename ===
    if (isMerged) {
        fNameIn2 = getFilename("reduced_", input2);
    } else {
        fNameIn2 = getFilename("sorted_output_split_", input2);
    }
    
    // === chop the 1st char from output ===
    temp = (char *) malloc(sizeof(char));
    strncpy(temp, output+1, strlen(output));
    temp[strlen(output)] = '\0'; //add the null-termination character '\0'
    output = temp; //assign temp back to input2
    //printf("output --> [%s]\n", output);
    
    // === output filename is always reduced, nil handling required ===
    fNameOut = getFilename("reduced_", output);
    // ==============================
    
    // === merge the 2 input files to output file ===
    FILE *fpOut = fopen(fNameOut, "w+");
    FILE *fpIn1 = fopen(fNameIn1, "r");
    FILE *fpIn2 = fopen(fNameIn2, "r");
    
    char tmp;
    char *string;
    int charCount = 0;
    int wordCount = 0;
    
    while ((tmp = fgetc(fpIn1)) != EOF) {
        //printf("[%c]\n", temp);
        
        if (tmp == '\n') {
            if (charCount > 0) {
                string[strlen(string)] = '\0'; //add the null-termination character '\0'
                //printf("[%s]-->\n",string);
                fprintf(fpOut, "%s\n", string);
                wordCount ++;
                charCount = 0;
                free(string);
                string = calloc(0, sizeof(char));
            }
        } else {
            if (charCount == 0) {
                string = malloc(sizeof(char));
            }
            string[charCount] = tmp;
            charCount ++;
        }
    }
    free(string);
    fclose(fpIn1);
    
    while ((tmp = fgetc(fpIn2)) != EOF) {
        //printf("[%c]\n", temp);
        
        if (tmp == '\n') {
            if (charCount > 0) {
                string[strlen(string)] = '\0'; //add the null-termination character '\0'
                //printf("[%s]-->\n",string);
                fprintf(fpOut, "%s\n", string);
                wordCount ++;
                charCount = 0;
                free(string);
                string = calloc(0, sizeof(char));
            }
        } else {
            if (charCount == 0) {
                string = malloc(sizeof(char));
            }
            string[charCount] = tmp;
            charCount ++;
        }
    }
    free(string);
    fclose(fpIn2);
    
    totalMergeWords = wordCount;
    printf("total words merged: %d\n", totalMergeWords);
    fclose(fpOut);
    // ==============================================
    
    //finalMergedOutputFile = (char *) malloc(sizeof(char));
    finalMergedOutputFile = fNameOut;
    
    free(input1);
    free(input2);
    free(output);
}

int getTotalMergeWords() {
    return totalMergeWords;
}

char* getFinalMergedOutputFile() {
    return finalMergedOutputFile;
}

int getNumberOfWordsInFile(char *fName) {
    FILE *fpIn;
    fpIn = fopen(fName, "r");
    
    char temp;
    char *string;
    int charCount = 0;
    int wordCount = 0;
    
    while ((temp = fgetc(fpIn)) != EOF) {
        //printf("[%c]\n", temp);
        
        if (temp == '\n') {
            //printf("[%s]-->\n",string);
            wordCount ++;
            charCount = 0;
            free(string);
            string = malloc(sizeof(char));
            
        } else {
            if (charCount == 0) {
                string = malloc(sizeof(char));
            }
            string[charCount] = temp;
            charCount ++;
        }
    }
    fclose(fpIn);
    free(string);
    
    printf("number of words: [%d]\n",wordCount);
    return wordCount;
}
