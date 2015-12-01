# C-MapReduce
A simple Map Reduce implementation in C programming language.

This program aims to illustrate the basic functioning of a MapReduce framework, it runs on local machine but forking the corresponding worker processes to simulate distributed processes in a cluster of machines. The Inter-Process Communication (IPC) among the workers (Mapper / Reducer / Master) processes and the parent process (the user) is simply achieved by using Uunamed Pipes.

We used an example of a hand-made ASCII word counting application to demonstrate the MapReduce computation, it merely counts the number of English words from an input source, and breaking down the input problem into numerous smaller chunks which will then be processed by the Map workers to generate intermediate output files, such output files will be further processed by the Reduce workers to generate the final combined output files.

The map() routine and reduce() routine (in main.c) can however be substituted by any other implementation for solving generic problem that fits the MapReduce programming model.

The input and output file samples has also been uploaded as well for illustration about how the data is being manipulated during the process.
[Get the output data](https://drive.google.com/file/d/0BwP5Ki5tO2LsTGhHVlBIYmVBUFk/view?usp=sharing)

This is initially developed by myself (Jeffrey K L Wong) and Bill K K Chan as the final project of the course - Fundamentals of Operating Systems during our Master Study in HK Polytechnic University. 


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
  
