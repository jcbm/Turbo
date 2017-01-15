To test the application, compile and run the Driver file. The file sets up a network on localhost using a different port for each node.
By varying the numberOfWorkers and numberOfReducers, different configurations can be evaluated.
The TeskTask file is used as the task, but another implementation can be used.
By calling activateDebug on a node, logging to stdout is enabled. Note that the output may be interleaved across methods.

