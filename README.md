# C-MapReduce
A simple Map Reduce implementation in C programming language

This program aims to illustrate the basic functioning of a MapReduce framework, it runs on local machine but forking the corresponding worker processes to simulate distributed processes in a cluster of machines. The Inter-Process Communication among themselves and with the parent process (the user) is simply achieved by using unamed pipes.

We used an example of ASCII Word Counting application to demonstrate the MapReduce computation, it merely counts the number of English words from an input source, and breaking down the input problem into numerous smaller chunks which will then processed by the Map workers to generate intermediate output files, such output files will be processed by the Reduce workers to generate the final output files.

The map() routine and reduce routine can however be substituted by another implementation for solving generic problem that fits the MapReduce programming model.

This is initially developed as the final project of the course - Operating Systems held by HK Polytechnic University. It's jointly developed by Jeffrey Garcia and Bill K K Chan.
