Instructions:
1. All the MapReduce jobs are written in a single class file named PageRank.

2. To run page rank program it must be provided with two arguments (input path which has input text files and output path for the generated output).

3. Transfer all the input files to the input folder and start the execution. 


For example, arguments and command must be like:
Command: Hadoop jar <Jar name> PageRank <arguments>
Arguments should be the source of input and output folder destination.
Arguments example: graph1/input graph1/output

3. All the Intermediate outputs and final output is created inside the given output location.

For example if the given output argument is graph1/output then all the output files is created inside graph1/output.
NOTE : 

1. To run the program again the output folder (given argument) must be deleted. All the intermediate outputs (LinkGraph outputs and PageRank outputs) are deleted after creating sorted output.
2. Name of the final output is FinalOutput inside the given output folder.