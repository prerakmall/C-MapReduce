//
//  main.c
//  C-MapReduce
//
//  Created by jeffrey on 1/12/15.
//  Copyright © 2015 jeffrey. All rights reserved.
//
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "splitutility.c" // for splitting the input problem
#include "wordutility.c" // for word parsing and counting operation
#include "mergeutility.c" // for reduce tasks planning and execution

/*
 * Put all GLOBAL variables here
 */
int reducerCount = 0;
int mapperCount = 0;
int MAX_CHILDS = 0;

int childIndex = 1; // current child process index

int mapTasksCount = 0; // number of map tasks to be allocated across the mappers
int reduceTasksCount = 0; // number of reduce tasks to be allocated across the mappers

int masterToParentPipe[2]; // for Master -> Parent pipe

int masterToMapperPipe[2]; // for Master -> Mapper pipe
int mapperToMasterPipe[2]; // for Mapper -> Master pipe

int masterToReducerPipe[2]; // for Master -> Reducer pipe
int reducerToMasterPipe[2]; // for Reducer -> Master



void reduce(char *reduceTask) {
    // implement the reduce operation here ...
    mergeFiles(reduceTask);
}

void map(char *fName) {
    // implement the map operation here ...
    parseWords(fName);
}

void reducerInformMaster() {
    close(reducerToMasterPipe[0]); // avoid reducer to read output to master
    
    char *buf = "Y";
    printf("reducer [PID: %d] invoke --> reduce operation done!\n", getpid());
    write(reducerToMasterPipe[1], buf, strlen(buf));
    
    // the reducer process maybe re-used, do NOT close the output pipe to master
}

void reducerRoutine() {
    close(masterToReducerPipe[1]); // avoid reducer write to master
    
    // initilize a dynamic char array for fetching the input data
    char* buf = (char *) calloc(0, sizeof(char));
    
    int temp;
    
    /* - the read command is blocking
     * - subsequent lines after the read will be blocked until more data been fetched
     * - the while loop will only break if the other side close the output pipe
     * - set the fetch data size to a relative large number according to the real situatuin,
     *   this could effectively avoid the input data to be chunked and transmitted out of order
     *   which makes it impossible to ressemble the original data
     */
    while((temp = read(masterToReducerPipe[0], buf, 1000)) > 0) {
        printf("reducer [PID: %d] %lu char read from pipe: [%s]\n", getpid(), strlen(buf), buf);
        
        // execute the reduce operation
        reduce(buf);
        
        // inform master when reduce task finish
        reducerInformMaster();
    }
    printf("reducer [PID: %d] process finished\n", getpid());
    
    close(reducerToMasterPipe[1]); // avoid reducer further writing output to master
    close(masterToReducerPipe[0]); // avoid reducer to further read from master
}

void mapperInformMaster() {
    close(mapperToMasterPipe[0]); // avoid mapper to read output to master
    
    char *buf = "Y";
    printf("mapper [PID: %d] invoke --> map operation done!\n", getpid());
    write(mapperToMasterPipe[1], buf, strlen(buf));
    
    // the mapper process maybe re-used, do NOT close the output pipe to master
}

void mapperRoutine() {
    close(masterToMapperPipe[1]); // avoid mapper write to master
    
    // initilize a dynamic char array for fetching the input data
    char* buf = (char *) calloc(0, sizeof(char));
    
    int temp;
    
    /* - the read command is blocking
     * - subsequent lines after the read will be blocked until more data been fetched
     * - the while loop will only break if the other side close the output pipe
     * - set the fetch data size to a relative large number according to the real situatuin,
     *   this could effectively avoid the input data to be chunked and transmitted out of order
     *   which makes it impossible to ressemble the original data
     */
    while((temp = read(masterToMapperPipe[0], buf, 1000)) > 0) {
        printf("mapper [PID: %d] %lu char read from pipe: [%s]\n", getpid(), strlen(buf), buf);
        
        // execute the map operation
        map(buf);
        
        // inform master when map task finish
        mapperInformMaster();
    }
    
    printf("mapper [PID: %d] process finished\n", getpid());
    
    // do the clean-up here
    free(buf);
    close(mapperToMasterPipe[1]); // avoid mapper further writing output to master
    close(masterToMapperPipe[0]); // avoid mapper to further read from master
}

void masterWakeupUser() {
    // === sort the final reduced file ===
    char *tmp = calloc(0, sizeof(char));
    printf("@@ reduce tasks count: %d\n", reduceTasksCount);
    tmp[0] = (reduceTasksCount) + '0';
    printf("@@ tmp: %s\n", tmp);
    char *finalMergedFileName = getFilename("reduced_", tmp);
    printf("@@ %s:\n", finalMergedFileName);
    int numberOfWords = getNumberOfWordsInFile(finalMergedFileName);
    char *sortedMergeFile = sortWords(finalMergedFileName, numberOfWords);
    free(tmp);
    // ===================================
    
    // === combine the counts in final reduced file ===
    analyzeWordsCount(sortedMergeFile);
    // ================================================
    
    // === All MapReduce tasks finish, Master inform Parent to wake-up ===
    close(masterToParentPipe[0]); // avoid master read from output to parent
    
    char *buf = "wake-up user!";
    printf("master [PID: %d] invoke --> %s\n", getpid(), buf);
    write(masterToParentPipe[1], buf, strlen(buf));
    
    close(masterToParentPipe[1]); // avoid master further write output to parent
    // ===================================================================
}

void masterWaitForReducer() {
    // === Master wait for the callback from all Reducers ===
    close(reducerToMasterPipe[1]); // avoid master write input from reducer
    
    char* buf = (char *) calloc(0, sizeof(char)); // initilize a dynamic char array for fetching data from pipe
    int temp;
    
    int completedTaskCount = 0;
    
    /* - the read command is blocking
     * - subsequent lines after the read will be blocked until more data been fetched
     * - the while loop will only break if the other side close the output pipe
     * - set the fetch data size to a relative large number according to the real situatuin,
     *   this could effectively avoid the input data to be chunked and transmitted out of order
     *   which makes it impossible to ressemble the original data
     */
    while((temp = read(reducerToMasterPipe[0], buf, 1)) > 0) {
        printf("master [PID: %d] process %lu char read from reducer pipe: [%s]\n", getpid(), strlen(buf), buf);
        completedTaskCount++;
        
        printf("@@ completed task count: %d\n", completedTaskCount);
        printf("@@ reduce task count: %d\n", reduceTasksCount);
        
        if (completedTaskCount >= reduceTasksCount) {
            break; // break the read roop if ALL callback from reducer tasks received
        } else {
            sleep(1); // avoid reading too fast from pipe, wait for 1 second
        }
    }
    
    // do the clean-up here
    free(buf);
    close(reducerToMasterPipe[0]); // avoid master further read input from reducer
    // =======================================================
}

void masterWaitForMapper() {
    // === Master wait for the callback from all Mappers ===
    close(mapperToMasterPipe[1]); // avoid master write input from mapper
    
    char* buf = (char *) calloc(0, sizeof(char)); // initilize a dynamic char array for fetching data from pipe
    int temp;
    
    int completedTaskCount = 0;
    
    /* - the read command is blocking
     * - subsequent lines after the read will be blocked until more data been fetched
     * - the while loop will only break if the other side close the output pipe
     * - set the fetch data size to a relative large number according to the real situatuin,
     *   this could effectively avoid the input data to be chunked and transmitted out of order
     *   which makes it impossible to ressemble the original data
     */
    while((temp = read(mapperToMasterPipe[0], buf, 1)) > 0) {
        printf("master [PID: %d] process %lu char read from mapper pipe: [%s]\n", getpid(), strlen(buf), buf);
        completedTaskCount++;
        
        if (completedTaskCount >= mapTasksCount) {
            break; // break the read roop if ALL callback from mapper tasks received
        } else {
            sleep(1); // avoid reading too fast from pipe, wait for 1 second
        }
    }
    
    // do the clean-up here
    free(buf);
    close(mapperToMasterPipe[0]); // avoid master further read input from mapper
    // =====================================================
}

void initReduceTasks() {
    // implement the logic to decide no. of reduce tasks based on output file from mapper
    //reduceTasksCount = 2; // debugger
    reduceTasksCount = getTotalMergeTask(mapTasksCount); // pass the input problem size
    printf("number of reduce tasks planned: %d\n:", reduceTasksCount);
}

void masterAssignReducer() {
    // === Master picks idle reducer and assigns each one a map task ===
    close(masterToReducerPipe[0]); // avoid master read from output to reducer
    
    // debugger
    //char *buf = "this is a reduce test";
    //printf("output buf: %lu \n", strlen(buf));
    
    // initialize the reduce task
    initReduceTasks();
    int initialTaskCount = reduceTasksCount;
    
    while(1) {
        printf("assigning task to reducer...\n");
        
        /* assign the corresonding merge task to each
         * reducer base on the merge plan
         */
        char *buf = getMergeTaskName(reduceTasksCount - initialTaskCount + 1);
        //printf("--> %s\n", buf);
        
        /* this line will hit error and quit the program
         * if no more reader is accepting input from the pipe
         */
        write(masterToReducerPipe[1], buf, strlen(buf));
        
        // free the buf
        free(buf);
        
        initialTaskCount--;
        
        if (initialTaskCount <= 0) {
            break; // quit the while loop when all reducer tasks are assigned
        } else {
            sleep(1); // sleep 1 second before assigning next reducer tasks
        }
    }
    
    close(masterToReducerPipe[1]); // avoid master further write to output to reducer
    // ==================================================================
}

void masterAssignMapper() {
    // === Master picks idle mapper and assigns each one a map task ===
    close(masterToMapperPipe[0]); // avoid master read from output to mapper
    
    // debugger
    //char *buf = "this is a map test";
    //printf("output buf: %lu \n", strlen(buf));
    
    int initialTaskCount = mapTasksCount;
    
    while(1) {
        printf("assigning task to mapper...\n");
        
        // assign the splitted input filename to each mapper
        char *buf = getSplitFilename(initialTaskCount);
        
        /* this line will hit error and quit the program
         * if no more reader is accepting input from the pipe
         */
        write(masterToMapperPipe[1], buf, strlen(buf));
        
        initialTaskCount--;
        
        if (initialTaskCount <= 0) {
            break; // quit the while loop when all mapper tasks are assigned
        } else {
            sleep(1); // sleep 1 second before assigning next mapper tasks
        }
    }
    
    close(masterToMapperPipe[1]); // avoid master further write to output to mapper
    // =================================================================
}

void masterRoutine() {
    // assign tasks to mapper
    masterAssignMapper();
    
    // should be FIRED only when all map tasks been assigned to mapper
    masterWaitForMapper();
    
    // assign tasks to reducer
    masterAssignReducer();
    
    // should be FIRED only when all reduce tasks been assigned to reducer
    masterWaitForReducer();
    
    // should be FIRED only when ALL MapReduces tasks finished!!!
    masterWakeupUser();
    
    printf("master [PID: %d] process finished\n", getpid());
}

void userRoutine() {
    // === wait for the wake-up signal from Master to indicate the completion of MapReduce tasks ===
    close(masterToParentPipe[1]); // avoid user write to input from master
    
    char* buf = (char *) calloc(0, sizeof(char)); // initilize a dynamic char array for fetching data from pipe
    int temp;
    
    /* - the read command is blocking
     * - subsequent lines after the read will be blocked until more data been fetched
     * - the while loop will only break if the other side close the output pipe
     * - set the fetch data size to a relative large number according to the real situatuin,
     *   this could effectively avoid the input data to be chunked and transmitted out of order
     *   which makes it impossible to ressemble the original data
     */
    while((temp = read(masterToParentPipe[0], buf, 1000)) > 0) {
        printf("parent process %lu char read from pipe: [%s]\n", strlen(buf), buf);
        break;
    }
    
    close(masterToParentPipe[0]); // avoid user further read input from master
    // =============================================================================================
}

void forkChild() {
    int returnpid;
    returnpid = fork();
    
    if (returnpid < 0) {
        printf("Fork failed\n");
        exit(1);
        
    } else if (returnpid == 0) {
        int pid = getpid();
        
        if (childIndex == MAX_CHILDS) { // make sure to create the master after all the slaves
            printf("child [Master] process number %d forked with PID: %d\n", childIndex, pid);
            
            // invoke master routine here
            masterRoutine();
            
        } else {
            // this is the Slave worker, can be mapper / reducer
            
            if (childIndex <= mapperCount) {
                printf("child [Mapper] process number %d forked with PID: %d\n", childIndex, pid);
                
                // invoke mapper routine here
                mapperRoutine();
                
            } else {
                printf("child [Reducer] process number %d forked with PID: %d\n", childIndex, pid);
                // invoke reducer routine here
                reducerRoutine();
            }
        }
        
    } else {
        int pid = getpid();
        printf("parent [User] process with PID: %d\n", pid);
        
        if (childIndex < MAX_CHILDS) {
            childIndex++;
            forkChild();
            
        } else {
            /* wait for 1 second to give enough time for the childs forking before resuming to parent,
             * technically this is not required as the user routine will block until receiving signal
             * from master before it can proceed
             */
            sleep(1);
            
            // invoke user routine here
            userRoutine();
            
            /* if parent routine is finished, it should
             * terminate all the child process before it quit
             */
            printf("parent process finished\n");
            wait(NULL); // block parent(root) process to wait until all child processes finish their routine
            exit(0);
        }
    }
}

void createPipes() {
    if (pipe(masterToMapperPipe) < 0) {
        printf("Master to Mapper pipe creation error\n");
        exit(1);
    }
    if (pipe(mapperToMasterPipe) < 0) {
        printf("Mapper to Master pipe creation error\n");
        exit(1);
    }
    
    if (pipe(masterToReducerPipe) < 0) {
        printf("Master to Reducer pipe creation error\n");
        exit(1);
    }
    if (pipe(reducerToMasterPipe) < 0) {
        printf("Reducer to Master pipe creation error\n");
        exit(1);
    }
    
    if (pipe(masterToParentPipe) < 0) {
        printf("Master to Parent pipe creation error\n");
        exit(1);
    }
}

void initChildsCounter() {
    /*******************************************************************************
     * Ideally, number of mapper (M) and reducer (R) should be much larger than 
     * the number of worker machines 
     *
     * We tend to choose M so that each individual task is roughly with same size 
     * of input data. While R is just a small multiple of the number of worker 
     * machines in use
     *******************************************************************************/
    
    mapperCount = mapTasksCount; // mapper count is simply set to mapTasksCount
    reducerCount = mapperCount / 4; // reducer count is simply set to 4 times smaller than mapper
    MAX_CHILDS = 1 + mapperCount + reducerCount; //at least 1 is master, the rest can be mapper or reducer
    
    printf("Map Worker(s): %d\n", mapperCount);
    printf("Reduce Worker(s): %d\n", reducerCount);
    printf("Max Worker(s): %d\n", MAX_CHILDS);
}

void initMapTasks() {
    // implement the logic to decide no. of map tasks based on input file from user
    mapTasksCount = splitfile("input.txt");
    printf("Map Tasks Count: %d\n", mapTasksCount);
}

int main() {
    initMapTasks();
    initChildsCounter();
    createPipes();
    forkChild();
}

