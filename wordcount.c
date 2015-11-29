#include <stdlib.h>
#include <stdio.h>
#include<string.h>

char* getCombinedFilename(char *fName) {
    char *sortedName = "sorted_";
    char *combinedName = "combined_";
    
    int chopLength = strlen(sortedName);
    char *choppedName = (char *) malloc(strlen(fName) - strlen(sortedName));
    
    int j = chopLength;
    for (int i=0; i<=strlen(choppedName); i++) {
        choppedName [i] = fName[j];
        j++;
    }
    //printf("chopped name: [%s]\n", choppedName);
    
    char *newName = (char *) malloc(strlen(combinedName) + strlen(choppedName));
    //printf("%lu\n", strlen(newName));
    
    strcpy(newName, combinedName);
    strcat(newName, choppedName);
    //printf("%lu\n", strlen(newName));
    //printf("--> %s\n", newName);
    
    return newName;
    free(newName);
}

char* getSortedFilename(char *fName) {
    char *sortedName = "sorted_";
    //printf("%lu\n", strlen(sortedName));
    
    char *newName = (char *) malloc(strlen(sortedName) + strlen(fName));
    //printf("%lu\n", strlen(newName));
    
    strcpy(newName, sortedName);
    strcat(newName, fName);
    //printf("%lu\n", strlen(newName));
    //printf("--> %s", newName);
    
    return newName;
    free(newName);
}

int compareString(char *string1, char *string2) {
    int result;
    result = strcmp(string1, string2);
    //printf("compare result: [%s] [%s] -> %d \n", string1, string2, result);
    return result;
}

char* copyString(char *inputString) {
    //printf("input: [%s] ", inputString);
    //printf("size %lu \n", strlen(inputString));
    
    // initialize the size of the output same as input
    char *outputString;
    outputString = malloc(strlen(inputString));
    
    
    // copy the characters from input to output
    for (int i=0; i<strlen(inputString); i++) {
        outputString[i] = inputString[i];
    }
    //printf("output: [%s] ", outputString);
    //printf("size %lu \n", strlen(outputString));
    
    return outputString;
}

void countWords(char *fName) {
    //printf("%s\n", fName);
    FILE *fpIn;
    fpIn = fopen(fName, "r");
    
    char temp;
    char *string;
    int charCount = 0;
    
    char *prevString = malloc(sizeof(char));
    //if (strlen(prevString) <= 0) {
    //    printf("prev String size: %lu \n", strlen(prevString));
    //}
    
    int matchCount = 1; //initialize the matchCount to 1 cause this is basic count value for each word exist in the input file
    
    FILE *fpOut;
    char *fNameOutput = getCombinedFilename(fName);
    fpOut = fopen(getCombinedFilename(fName), "w+");
    
    while ((temp = fgetc(fpIn)) != EOF) {
        //printf("[%c]\n", temp);
        
        if (temp == '\n') {
            if (charCount > 0) {
                //printf("[%s]-->\n",string);
                charCount = 0;
                
                // do the comparision here
                if (strlen(prevString) <= 0) { // the first record, prevString is empty
                    //printf("the first record [%s]-->\n",string);
                    
                    /* copy the current string to the previous string
                     * destroy current sting
                     */
                    prevString = copyString(string);
                    
                } else {
                    /* compare the current string with previous string 
                     * if matches, increment the match count by 1, then proceed next iteration
                     * if not matches, write the output associated match count to output file, then reset match count to 1
                     */
                    if (compareString(prevString, string) != 0) { // No match is found
                        //printf("no more match found for [%s] --> %d\n", prevString, matchCount);
                        fprintf(fpOut, "%s,%d\n", prevString, matchCount);
                        matchCount = 1;
                        prevString = copyString(string);
                    } else {
                        matchCount++;
                        //printf("a previous match is found for [%s]-->\n",string);
                    }
                }
                
                free(string);
                string = malloc(sizeof(char));
            }
        } else {
            if (charCount == 0) {
                string = malloc(sizeof(char));
            }
            string[charCount] = temp;
            charCount ++;
        }
    }
    
    /* Avoid the last record being omitted, just use the matchCount to determine a matching or not
     */
    if (matchCount <= 1) { // No match is found
        //printf("no more match found for [%s] --> %d\n", prevString, matchCount);
        fprintf(fpOut, "%s,%d\n", string, matchCount);
    } else {
        //printf("a previous match is found for [%s]-->\n",string);
        fprintf(fpOut, "%s,%d\n", string, matchCount);
    }
    
    free(string);
    free(prevString);
    fclose(fpIn);
    fclose(fpOut);
}

char* sortWords(char *fName, int wordCount) {
    //printf("%s\n", fName);
    FILE *fpIn;
    fpIn = fopen(fName, "r");
    
    char temp;
    char *string;
    int count = 0;
    
    
    // instantiate the array of Strings
    int nRows = wordCount;
    int recordCount = 0;
    char **arrayOfString = malloc(nRows * sizeof(char *)); // Allocate row pointers
    for(int i = 0; i < nRows; i++)
        arrayOfString[i] = malloc(sizeof(char));  // Allocate each row separately
    
    while ((temp = fgetc(fpIn)) != EOF) {
        if (temp == '\n' || temp == ' ') {
            if (count > 0) {
                arrayOfString [recordCount] = string;
                //printf("%s-->",string);
                recordCount ++;
                
                count = 0;
                free(string);
                string = malloc(sizeof(char));
            }
        } else {
            if (count == 0) {
                string = malloc(sizeof(char));
            }
            string[count] = temp;	
            count ++;
        }
    }
    fclose(fpIn);
    
    // competed fetching the file into memory, here is where the sorting happens
    char *tempString = malloc(sizeof(char));
    for(int i = 0; i < nRows; i++) {
        for (int j = 1; j< nRows; j++) {
            if (strcmp(arrayOfString[j-1], arrayOfString[j]) > 0) {
                strcpy(tempString, arrayOfString[j-1]);
                strcpy(arrayOfString[j-1], arrayOfString[j]);
                strcpy(arrayOfString[j], tempString);
            }
        }
    }
    
    FILE *fpOut;
    char *fNameOutput = getSortedFilename(fName);
    fpOut = fopen(getSortedFilename(fName), "w+");
    for(int i = 0; i < nRows; i++) {
        fprintf(fpOut, "%s\n", arrayOfString[i]);
    }
    fclose(fpOut);
    
    free(arrayOfString);
    
    return fNameOutput;
}

int main() {
	FILE *fpIn;
    FILE *fpOut;
	fpIn = fopen("input01.txt", "r");
    fpOut = fopen("output01.txt", "w+");
    
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
				//printf("[%s]-->\n",string);
                fprintf(fpOut, "%s\n", string);
                wordCount ++;
                charCount = 0;
				free(string);
                string = malloc(sizeof(char));
			}
		} else {
			if (charCount == 0) {
				string = malloc(sizeof(char));
			}
			string[charCount] = temp;
			charCount ++;
		}
	}
    free(string);
	fclose(fpIn);
    fclose(fpOut);
    
    char *fNameOutput = sortWords("output01.txt", wordCount);
    countWords(fNameOutput);
    
	return 0;
}

