# nmp
A module for networked multi-processing

nmp aims to be a drop-in  replacement for the multiprocessing module allowing worker threads to run on other machines accessible in the network.
Consider a machine in the network named 'mydesktop'. 
With nmp it is seamless to offload computation to that machine. 
A minimal example is the following,

    import nmp
    def xsq(x):
        return x**2
    a = nmp.Pool()
    a.add_host('mydesktop')
    print(a.map(xsq, range(10)))

which should print,

    >>> [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

in the background the following happened,
1. nmp checked that it can access 'mydesktop'
2. nmp checked that N computing cores are available at 'mydesktop'
3. nmp started N worker processes at 'mydesktop'
4. nmp sent the function and data to be evaluated to each worker and collected the results

nmp is intended to be:
- zero install. no installation required in any of the remote hosts. in any case please check the requirements
- zero setup, make use of available machines in the network, just type, e.g., a.add_host('hostname') or a.add_host('192.168.1.100') and the hosts can be used.
- scale an existing python code that uses map()-like construtions to an arbitrary number of heterogenous hosts in the network

nmp is NOT:
- a distributed computing framework
- it is not a job scheduler
- a tool to orchestrate machines in the network
- a tool to manage workflows and distribute them over networked machines

## requirements
For the boss host:
- python3 
- ssh client with key-based authentication enabled (password-less login) for the computation hosts 
- a few GNU/Linux utilities: ping, hostname
 
For the computation hosts:
- python3 installed
- a few GNU/Linux utilities: ping, 

At its core, nmp uses SSH to launch processes at the computation hosts then the functions and data for the computation are serialized and sent to the workers over a TCP connection. nmp is definitely not the 
first implementation of this. nmp stands out because it,
- does not require root or any installation in the computing nodes
- it handles the workers at the computing hosts

## caveats
Networked communication is much slower than communication in the same host. 
Therefore, depending on the type of workload, the network speed may be a bottleneck.
In that case nmp will not speed up the execution, it will make it slower. 
It is important to test your specific workload

## problems
nmp serializes (pickles) the function and sends it to the workers. 
The pickle protocol places many restrictions on what kind of things can be pickled. 
For example, lambda functions cannot be pickled.
Additionally, I have noticed that a simple unpickled function at the worker may execute several orders of magnitude slower than it should. 
At the moment the best way to solve these issues is to use the dill module. 
It is a pity, because this adds one dependency at the computing nodes. 
In any case it can be easily installed with,

    python3 -m pip install dill
    
## status
This module is an alpha release. at the moment it is a minimal working prototype. 
It has not been tested in any meaningful context.

## todo
- dynamically create and destroy workers to accommodate workload
- robust to workers failing
- expanded the use cases beyond Pool.map()
- check each thread performance and assign work accordingly
- integrate with cloud provides: Amazon, Google, DigitalOcean
- integrate with Docker(?)

## resources
A random list of resources that inspired nmp,
- examples of multiprocessing use: https://www.usenix.org/system/files/login/articles/login1210_beazley.pdf
- a list of projects for parallel processing with Python: https://wiki.python.org/moin/ParallelProcessing
- dispy
- Airflow
- Dask
