# nmp stands for networked multi-processing and it aims to be a drop-in 
# replacement for the multiprocessing module allowing for worker threads
# to run on other machines accessible in the network.
#
# consider a machine in the network named 'mydesktop'. with nmp it is 
# seamless to offload computation to that machine.  a minimal example is 
# the following
#   import nmp
#   def xsq(x):
#       return x**2
#   a = nmp.Pool()
#   a.add_host('mydesktop')
#   print(a.map(xsq, range(10)))
#
# which should print,
#   [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]
#
# in the background the following happened,
#   1. nmp checked that it can use 'mydesktop' 
#   2. nmp checked that N computing cores are available at 'mymachine'
#   3. nmp started N worker processes at 'mymachine'
#   4. nmp sent the function and data to be evaluated to each worker
#       and collected the results
#
# nmp is intended to be:
#   - zero install. no installation required in any of the remote
#     hosts. in any case please check the requirements
#   - zero setup, make use of available machines in the network,
#     just type, e.g., a.add_host('hostname') or a.add_host('192.168.1.100')
#     and the hosts can be used.
#   - scale an existing python code that uses map()-like construtions to
#     an arbitrary number of heterogenous hosts in the network
#
# nmp is NOT:
#   - a distributed computing framework
#   - it is not a job scheduler
#   - a tool to orchestrate machines in the network
#   - a tool to manage workflows and distribute them over networked 
#     machines
#
# requirements:
# for the boss host:
#   - python3 
#   - ssh client with key-based authentication enabled (password-less 
#     login) for the computation hosts 
#   - a few GNU/Linux utilities: ping, hostname
# 
# for the computation hosts:
#   - python3 installed
#   - a few GNU/Linux utilities: ping, 
#
# at its core, nmp uses SSH to launch processes at the computation hosts
# then the functions and data for the computation are serialized and 
# sent to the workers over a TCP connection. nmp is definitely not the 
# first implementation of this. nmp stands out because it
#   - does not require root or any installation in the computing nodes
#   - it handles the workers at the computing hosts
#
# caveats:
#   - networked communication is much slower than communication in the
#     same host. therefore, depending on the type of workload, the 
#     network speed may be a bottleneck; in that case nmp will not speed
#     up the execution, it will make it slower. it is important to test
#     your specific workload
#
# problems:
#   - nmp serializes (pickles) the function and sends it to the workers.
#     the pickle protocol places many restrictions on what kind of things
#     can be pickled. For example, lambda functions cannot be pickled.
#     additionally, i have noticed that a simple unpickled function at 
#     the worker may execute several orders of magnitude slower than
#     it should. at the moment the best way to solve this issue is to 
#     use the dill module. it is a pity, because this adds one dependency
#     at the computing nodes. in any case it can be easily installed with 
#       'python3 -m pip install dill'
# 
# development status:
# this module is an alpha release. at the moment it is a minimal working 
# prototype. it has not been tested in any meaningful context.
#
# improvement plans:
#   - dynamically create and destroy workers to accommodate workload
#   - robust to workers failing
#   - expanded the use cases beyond Pool.map()
#   - check each thread performance and assign work accordingly
#   - integrate with cloud provides: Amazon, Google, DigitalOcean
#   - integrate with Docker?
#
# resources:
# a random list of resources that inspired nmp
#   - examples of multiprocessing use: https://www.usenix.org/system/files/login/articles/login1210_beazley.pdf
#   - a list of projects for parallel processing with Python: https://wiki.python.org/moin/ParallelProcessing
#   - dispy
#   - Airflow
#   - Dask

from multiprocessing.connection import Listener, Client
import threading 
import subprocess
from queue import Queue
import argparse
from os import path
try:
    import dill as pkl
except ModuleNotFoundError:
    import pickle as pkl

# debug
import time     
import logging

## module variables
DEF_WORKDIR = '/tmp/nmp'
DEF_INITIAL_PORT = 16000

logger = logging.getLogger(__name__)
# logger.setLevel('DEBUG')

NMP_FILE = path.abspath(__file__)
logger.debug('nmp.py file is at %s' % (NMP_FILE,))

## low level functions
def _call(cmd, live=False, test=False):
    """
    Execute a command in the shell and returns stdout as string.
    Optional argument live directly prints stdout througt python's terminal.
    If the command fails, an exception is raised with stderr.

    if test is True, then call returns True if the return_code is 0 and False
    otherwise
    """

    if live:
        # call command
        return_code = subprocess.call(cmd, shell=True)

        # output already printed to the terminal
        output = ["", ""]
    else:
        # create call object
        callObj = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        # wait for execution to finish and get (stdout,stderr) tuple
        output = callObj.communicate()
        # logger.debug'DEBUG:call: stdout: %s' % (output[0],))
        # logger.debug'DEBUG:call: stderr: %s' % (output[1],))

        # collect return code
        return_code = callObj.returncode
        # logger.debug('DEBUG:call: retcode: %s' % (return_code,))

    # check return code
    if return_code != 0:
        # return stderr
        if test:
            return False
        else:
            raise RuntimeError(output[1].decode())
    else:
        if test:
            return True
        else:
            # return stdout
            return output[0].decode()


def _tcall(cmd):
    '''
    short alias for call with test=True
    returns True if command is successful and False otherwise
    '''

    return _call(cmd,test=True)

def _check_local_env():
    '''
    check the local system for the following: ping, ssh
    raises exception if any of them is not found
    '''

    for x in ['ping','ssh','scp','python3','hostname']:
        if not _env_has(x):
            raise RuntimeError('no %s found in the local system' %(x,))

def _env_has(prg):
    '''
    check local environment for executable prg
    return True if yes otherwise False
    only works on linux
    '''

    return tcall('type %s' % prg)

## computed module variables
# get my ip
BOSS_ADDRESS = _call('hostname -I').split()[0]
logger.debug('my ip is %s',BOSS_ADDRESS)

## classes
class Pool(object):
    """
    a class to handle a pool of workers at multiple remote hosts 
    """
    def __init__(self, no_workers=None, hosts=None):
        super(Pool, self).__init__()
        self.no_workers = no_workers
        self.hosts = []

        self.d = Queue()
        self.r = Queue()

    def add_host(self,*args,**kwargs):
        '''add a new host to the pool'''

        host = Host(*args,**kwargs)
        self.hosts.append(host)

    def start(self):

        # determine number of hosts 
        if self.no_workers is None:
            self.no_workers = sum([x.max_no_workers for x in self.hosts])

        # loop hosts and start workers
        for i in range(self.no_workers):
            wid = i
            port = DEF_INITIAL_PORT + i
            sorted(self.hosts,key=lambda x: x.get_load_factor())[0].start_worker(wid, port, self.d, self.r)

    def stop(self):
        '''
        terminate all threads which should stop all workers
        '''

        raise NotImplementedError()

    def map(self, fnc, itr):
        '''
        map function fnc the iterable itr and evaluate it in 
        the remote workers
        return a list with the results
        '''

        # populate the queue
        [self.d.put(pkl.dumps((fnc, x))) for x in itr]
        logger.debug('populated the queue with %s' %(str(itr)))

        # start workers
        self.start()
        logger.debug('started workers')

        # wait for all data to be consumed
        self.d.join()
        logger.debug('waiting for results')

        return list(self.r.queue)


class Host(object):
    """
    class to handle a host that will run workers

    TODO: generalize the base Host class, and use inheritance to define
    different types of hosts, e.g., Linux, Local, Windows, Amazon, 
    DigitalOcean, Slurm, etc.
    """

    def __init__(self, host, name=None, user=None, wdir=DEF_WORKDIR, max_no_workers=None):
        super(Host, self).__init__()    
        self.host = host
        self.name = name
        if self.name is None:
            self.name = self.host
        self.user = user
        self.wdir = wdir
        self.max_no_workers = max_no_workers

        # check host and prepare it for workers
        self.prepare()

        # if no max workers were specified get them from the system
        if self.max_no_workers is None:
            self.max_no_workers = self._get_no_cores()

        # list to store thread handles for the workers in this host
        self.secretaries = []
      
    def prepare(self):
        '''
        executes a number of checks and copies the nmp module to the 
        remote host to its working directory.
        check reachable
        check connect
        can ping localhost
        check env
        copy module to wdir
        '''

        if not self._is_reachable():
            raise RuntimeError('cannot reach host %s' % (self.host,))
        logger.debug('%s is reachable' % (self.host,))
        if not self._can_connect():
            raise RuntimeError('no passwordless ssh connection to host %s' %(self.host,))
            logger.debug('can login to %s' % (self.host,))
        if not self._can_reach_boss():
            raise RuntimeError('host %s cant reach localhost' % (self.host,))

        self._check_env()

        # copy file to remote host
        self._copy_file(NMP_FILE,path.join(self.wdir,'nmp.py'))

    def start_worker(self,wid,port,d,r):
        '''
        start a worker on this host what will connect to boss on the specified port

        TODO: start workers simultaneously
        '''

        # start a thread to listed on the port and add it to the pool
        logger.debug('host %s starting thread %s' % (self.name,wid))
        x = Secretary(wid,port,d,r)
        x.start()
        self.secretaries.append(x)

        # start a worker in the remote host which will connecte over TCP on the given port
        self._call('cd %s; nohup python3 nmp.py -a %s:%d %d &> %s.log </dev/null &' % (
            self.wdir,BOSS_ADDRESS,port,wid,x.name))
        logger.debug('started worker %s at host %s' % (wid, self.name))


    def stop_worker(self, n=1):
        '''
        stop n workers
        '''

        raise NotImplementedError()

    def get_load_factor(self):
        '''
        metric for the load in the host
        '''

        return (self.max_no_workers -len(self.secretaries))/float(self.max_no_workers)

    def _get_no_cores(self):
        '''
        get the number of cores available in the host
        only works on linux
        '''

        nc = self._call('nproc')
        logging.debug('%s has %s cores available' % (self.name,nc))
        nc = int(nc)

        return nc

    ## low level functions
    def _call(self,cmd,test=False):
        '''
        runs command on remote machine through ssh
        returns stdout if successful or raises an exception RuntimeError with stderr
        if it fails
        if test is True, then it returns True if the return code is 0 and False 
        otherwise
        '''

        host = self.host
        if self.user:
            host = self.user+'@'+self.host

        cmd = 'ssh -o PasswordAuthentication=no %s "%s"' % (self.host,cmd)
        
        return _call(cmd, test=test)

    def _tcall(self,cmd):
        '''
        alias for _call(cmd,test=True)
        return True if cmd succeeds and False otherwise
        '''

        return self._call(cmd,test=True)

    def _env_has(self, prg):
        '''
        check local environment for executable prg
        return True if yes otherwise False
        this only works on linux
        '''

        return _tcall('type %s' % prg)

    def _check_env(self):
        '''
        check the local system for some executables
        raises exception if any of them is not found
        '''

        for x in ['ping','python3','nproc']:
            if not self._env_has(x):
                raise RuntimeError('no %s found in host %s' %(x,self.host))

    def _is_reachable(self):
        '''
        ping host to see if it is reachable
        return True or False
        '''

        return _tcall('ping -c 1 %s' % (self.host,))

    def _can_reach_boss(self):

        return self._tcall('ping -c 1 %s' % (BOSS_ADDRESS,))

    def _can_connect(self):
        '''
        check connection to host
        returns true or false
        '''

        return self._tcall(':')

    def _copy_file(self, lf, rf):
        '''
        copy local file lf to host file rf
        will create directory if it doesn't exist
        returs none, will throw RuntimeError if command fails
        '''

        # build remote file identifier
        host = self.host
        if self.user:
            host = self.user+'@'+self.host

        # check local file
        if not path.isfile(lf):
            raise FileNotFoundError(lf)

        # check remote directory
        rd = path.dirname(rf)
        if not self._tcall('[[ -d "%s" ]]' % (rd,)):
            self._call('mkdir "%s"' % (rd,))

        # copy file    
        _call('scp -o PasswordAuthentication=no "%s" "%s:%s"' % (lf,host,rf))


class Secretary(threading.Thread):
    """
    a class for a thread that will handle the "phones", i.e. 
    communication with a remote worker over TCP 

    wid is an integer with the id of this worker
    port is the TCP port where the TCP connection will be established
    d is a FIFO queue to get data for the worker
    r is a FIFO queue to put results from the worker
    """
    def __init__(self, wid, port, d, r):
        super(Secretary, self).__init__(daemon=True)
        self.wid = wid
        self.name = 'w%d' % (wid,)
        self.port = port
        self.d = d
        self.r = r

        self.address = (BOSS_ADDRESS,port)

    def run(self):
        '''
        listed on speficied port and 
        '''

        logger.debug('%s: waiting for connection at %s' % (self.name,self.address))
        worker_conn = Listener(self.address)
        worker = worker_conn.accept()
        logger.debug('%s: connected to worker' % (self.name))

        # while the data queue is not empty
        while not self.d.empty():
            # send 
            tic = time.time()
            x = self.d.get()          
            toc = time.time() -tic
            logger.debug('%s: got another item' % (self.name,))
            logger.debug('%s: elapsed time %f\n' % (self.name,toc,))

            tic = time.time()
            worker.send(x)
            toc = time.time() -tic
            logger.debug('%s: sent data to worker' % (self.name,))
            logger.debug('%s: elapsed time %f\n' % (self.name,toc,))


            logger.debug('%s: waiting for result' % (self.name,))
            tic = time.time()
            z = worker.recv()
            toc = time.time() -tic
            logger.debug('%s: got %s' % (self.name,str(z)))
            logger.debug('%s: elapsed time %f\n' % (self.name,toc,))
            # try:
            #     z = worker.recv()
            # except (EOFError, ConnectionResetError):
            #     # something went wrong with this thread, put x back in the queue and terminate
            #     # other workers will deal with x, however order will not be correct
            #     logger.debug('%s: worker w%d failed' % (self.name,wid))
            #     logger.debug('%s: putting data back in the queue' % (self.name,))
            #     d.put(x)
            #     d.task_done()
            #     break

            tic = time.time()
            self.r.put(z)
            self.d.task_done()
            toc = time.time() -tic
            logger.debug('%s: put result in the queue' % (self.name,))
            logger.debug('%s: elapsed time %f\n' % (self.name,toc))


## remote host functions
def worker(wid, address):
    '''
    worker loop which performs cumputation at the remote host
    it communicates with a Secretary thread at host over TCP at port. 
    '''

    logging.debug('w%d: calling boss at %s...' % (wid, address))
    boss = connect_with_timeout(wid, address)

    while True:
        try:
            logger.debug('w%d: waiting for data from boss' % (wid,))
            tic = time.time()
            x = boss.recv()
            toc = time.time() - tic
            logger.debug('w%d: elapsed time %f' % (wid,toc,))

        except EOFError:
            print('w%d: connection closed by boss, terminating' %(wid,))
            return

        except ConnectionResetError:
            print('w%d: connection reset by boss, terminating' %(wid,))
            return

        # unpickle payload
        tic = time.time()
        fun, arg = pkl.loads(x)
        toc = time.time() - tic
        logger.debug('w%d: got %s' % (wid,str(arg)))
        logger.debug('w%d: elapsed time %f\n' % (wid,toc))

        # run payload
        tic = time.time()
        r = fun(arg)
        toc = time.time() - tic
        logger.debug('w%d: computed' % (wid,))
        logger.debug('w%d: elapsed time %f\n' % (wid,toc))

        # return payload
        tic = time.time()
        boss.send(r)
        toc = time.time() -tic
        logger.debug('w%d: sent %d' % (wid,r))
        logger.debug('w%d: elapsed time %f\n' % (wid,toc))

def connect_with_timeout(wid, address, timeout=60):
    '''
    establish connection with the boss at host and TCP port port
    if not succesful retry and raise timeout exception after one timeout 
    seconds
    return connection handle
    '''

    # 
    WAIT_SEC_BEFORE_RETRY=1

    # cast port to int
    host, port = address.split(':')
    port = int(port)

    stime = time.time()
    logger.debug('w%d: calling boss at %s:%d...' % (wid, host, port))
    while True:
        try:
            boss = Client((host,port))
            break
        except ConnectionRefusedError:
            # wait at most 1min until boss comes online
            if time.time() -stime > timeout:
                logger.debug('w%d: connection timeout' % (wid,))
                raise TimeoutError('could not connect to %s:%d' % (host,port))
        # wait a bit before trying again
        time.sleep(WAIT_SEC_BEFORE_RETRY)

    logger.debug('w%d: connected', wid)
    return boss

def parse_args():
    """
    parser when running this module from the command line to start 
    a worker at the remote host
    """

    desc = "start a nmp worker"

    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("wid",type=int,help="worker id")
    parser.add_argument("-a","--address",help="host:port to establish connection")

    return parser.parse_args()

# this gets executed when the module is called from the command line
if __name__ == '__main__':

    args = parse_args()
    worker(args.wid,args.address)
