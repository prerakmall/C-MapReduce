# C-MapReduce
A simple Map Reduce implementation in C programming language.

This program aims to illustrate the basic functioning of a MapReduce framework, it runs on local machine but forking the corresponding worker processes to simulate distributed processes in a cluster of machines. The Inter-Process Communication (IPC) among the workers (Mapper / Reducer / Master) processes and the parent process (the user) is simply achieved by using Uunamed Pipes.

We used an example of a hand-made ASCII word counting application to demonstrate the MapReduce computation, it merely counts the number of English words from an input source, and breaking down the input problem into numerous smaller chunks which will then be processed by the Map workers to generate intermediate output files, such output files will be further processed by the Reduce workers to generate the final combined output files.

The map() routine and reduce() routine (in main.c) can however be substituted by any other implementation for solving generic problem that fits the MapReduce programming model.

The input and output file samples has also been uploaded as well for illustration about how the data is being manipulated during the process.
[Get the output data](https://drive.google.com/file/d/0BwP5Ki5tO2LsTGhHVlBIYmVBUFk/view?usp=sharing)

This is initially developed by myself (Jeffrey K L Wong) and Bill K K Chan as the final project of the course - Fundamentals of Operating Systems during our Master Study in HK Polytechnic University. The program was written using Xcode7.0 and fully tested on the following environment:  

1. Mac OSX 10.10.4 with Apple LLVM compiler (clang-700.0.72)  

2. Suse Linux 11 (kernel 3.0.93-0.5) with GCC compiler (gcc 4.3.4)  


# Execution Overview
The Map invocations are distributed across multiple processes by automatically partitioning the input data into a set of M splits, the input splits are sent to the Map workers and be processed in parallel. While Reduce invocations are distributed by partitioning the intermediate key space into R pieces are simply specified by the user.

We tend to choose M so that each individual task is roughly with same size of input data, this can be easily achieved by limiting the number of words contained in each split. In our implementation, we consider M as the number of map tasks that need to be performed and for simplicity, we set the number of Map workers just equal to M, and R = M/4, with number of Reduce workers just equal to R.  

However in a real distributed environment M should be much larger than the total number of commodity machines in the cluster, and R is defined as a small multiple of the total machines in use.
[Read the paper published from Google](http://research.google.com/archive/mapreduce-osdi04.pdf)  

The overall flow of a MapReduce operation in our implementation is explained below.  

1. The main routine (user program) will first splits the input files into M pieces of smaller chunks containing equal number of words (controllable by the user via an optional parameter). It then use the fork() command to startup many copies of the program on the local machine.  

2. One of the copies of the program is special - the Master. The rest are workers that are assigned with work from the Master. There should be M map tasks and R reduce tasks to assign. The master picks idle worker process and assigns each one a map task or a reduce task.  

3. A worker who is assigned a map task reads the contents of the corresponding input split. It parses key/value pairs out of the input data and passes each pair to the user-defined Map function. The intermediate key/value pairs produced by the Map function are then written to temporary output files on local disks.  

4. The output files location are then passed back to the Master, who is responsible for forwarding these files to the reduce workers.  

5. When a reduce worker is notified by the master about these locations, it reads all the intermediate data and sorts it by the intermediate keys so that all occureneces of the same key are grouped together. This sorting is needed because typically many different keys map to the same reduce task.  

6. The reduce worker iterates over the sorted intermeidate data and for each unique intermediate key encountered, it passes the key and the corresponding set of intermediate values to the user's Reduce function. The output of the Reduce function is appended to a final output file for this reduce partition.  

7. When all Map and Reduce tasks have been completed, the Master wakes up the user program. At this point the MapReduce computation is finished and returns the program control back to user code.  

8. After successful comletion, the output of the MapReduce computation is available in R outout files (one per reduce task, with file names as specified by the user).   

9. The merge routine is then invoked to combine all the R output files into one file. Typically, users do not need to perform this combination as they can simply pass these files as input to another MapReduce call, or use them from another distributed application that is able to deal with input that is partitioned into multiple files.  


# Walkthrough
The WordCount application is quite straight-forward.

The Mapper implementation, via the map method, processes one line at a time. It splits the line into tokens separated by whitespaces or symbols, and emits a key-value pair of < _WORD_ , 1>.

For an sample input:  
Hello World Bye World  
Hello Map Reduce Goodbye Map Reduce  

The first map emits:  
< Hello, 1>  
< World, 1>  
< Bye, 1>  
< World, 1>   

The second map emits:  
< Hello, 1>  
< Map, 1>  
< Reduce, 1>  
< Goodbye, 1>  
< Map, 1>  
< Reduce, 1>  
  
Our program also specifies a combiner. Hence, the output of each map is passed through the local combiner (which is same as the Reducer as per the job configuration) for local aggregation, after being sorted on the keys.  

The output of the first map:  
< Bye, 1>  
< Hello, 1>  
< World, 2>  
  
The output of the second map:  
< Goodbye, 1>  
< Hello, 1>  
< Map, 2>  
< Reduce, 2>  
  
The Reducer implementation, via the reduce method just sums up the values, which are the occurence counts for each key (i.e. words in this example).
  
Thus the output of the job is:  
< Bye, 1>   
< Hello, 2>  
< Goodbye, 1>  
< Map, 2>  
< Reduce, 2>  
< World, 2>  
  
