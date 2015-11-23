#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
//#include <stdbool.h>
#include <string.h>



/*
 * Put all GLOBAL variables here
 */

const int MAX_CHILD = 3; //1 master, 1 mapper, 1 reducer
int childCount = 1;
int masterPID;

int downstreamPipe[2]; // for downstream pipe: Parent -> Child
int upstreamPipe[2]; // for upstream pipe: Chile -> Parent


void childRoutine() {
    close(downstreamPipe[1]); // avoid child write to downstream
    
    // initilize a dynamic char array for fetching the input data
    char* buf = (char *) malloc(sizeof(char));
    
    int temp;
    
    /* - the read command is blocking
     * - subsequent lines after the read will be blocked until more data been fetched
     * - the while loop will only break if the other side close the output pipe
     * - set the fetch data size to a relative large number according to the real situatuin,
     *   this could effectively avoid the input data to be chunked and transmitted out of order
     *   which makes it impossible to ressemble the original data
     */
    while((temp = read(downstreamPipe[0], buf, 1000)) > 0) {
        printf("%lu char read from pipe: [%s]\n", strlen(buf), buf);
        
        // child process do some work with the input data
        printf("@@@ 1\n");
        
        // child process finish the work and inform the master
        printf("@@@ 2\n");
    }
    
    printf("child process finished\n");
    
    // do the clean-up here
    free(buf);
    close(downstreamPipe[0]); // avoid child to further read from downstream
}

void parentRoutine() {
    close(downstreamPipe[0]); // avoid parent read from downstream
    
    char *buf = "this is a test";
    printf("parent buf: %lu \n", strlen(buf));
    
    while(1) {
        printf("send data...\n");
        
        /* this line will hit error and quit the program 
         * if no more reader is accepting input from the pipe
         */
        write(downstreamPipe[1], buf, strlen(buf));
        
        sleep(1);
    }
    
    printf("parent process finished\n");
    
    // do the clean-up here
    close(downstreamPipe[1]); // avoid parent further write to downstream
}

void forkChild() {
    int returnpid;
    returnpid = fork();
    
    if (returnpid < 0) {
        printf("Fork failed\n");
        exit(1);
        
    } else if (returnpid == 0) {
        if (childCount == 1) {
            // this is the Master worker
            masterPID = getpid();
        }
        
        int pid = getpid();
        printf("child process number %d forked with PID: %d\n", childCount, pid);
        
        // master/mapper/reducer routine here
        childRoutine();
        
    } else {
        if (childCount < MAX_CHILD) {
            childCount++;
            forkChild();
            
        } else {
            sleep(1); // wait for 1 second to make sure all the childs forking finish
            int pid = getpid();
            printf("parent process PID: %d\n", pid);
            
            //user routine here
            parentRoutine();
            
            /* if parent routine is finished, it should
             * terminate all the child process before it quit
             */
            wait(NULL);
            exit(0);
        }
    }
}

void createPipes() {
    if (pipe(downstreamPipe) < 0) {
        printf("Downstream pipe creation error\n");
        exit(1);
    }
    if (pipe(upstreamPipe) < 0) {
        printf("Upstream pipe creation error\n");
        exit(1);
    }
}

int main(int argc, char *argv[]) {
    createPipes();
    forkChild();
}
