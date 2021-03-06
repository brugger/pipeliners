#
# 
# 
##

from __future__ import print_function, unicode_literals
import inspect
import os
import pprint as pp
import time
from time import gmtime, strftime
import subprocess

from local import *
from slurm import *

class Job_status( object ):
    """ Enumerate class for job statuses, this is done differently in python 3

    """
    FINISHED    =    1
    FAILED      =    2
    NO_RESTART  =    3
    RUNNING     =    4
    QUEUEING    =    5
    RESUBMITTED =    6
    SUBMITTED   =    7
    CREATED     =   98
    KILLED      =   99
    UNKNOWN     =  100


class Job(object):
    """ This class is presenting a singular job and all information associated with it. 

    """


    def __init__(self,  cmd, step_name, output=None, limit=None, delete_file=None, thread_id=None):
        """ Create a job object
        
        Args:
          cmd (str): command to run
          step_name (str): name of the step that this command belongs to
          output (str): output information to pass on to the next job
          limit (str): paramters to pass on to the backend
          delete_file (str): File(s) to delete if the job is successful
          thread_id (int): id of the thread running this 

        Returns:
          job (obj)
        """

        self.status   = Job_status.CREATED
        self.active   = True
        self.command  = None
        self.backend  = None

        self.output       = output
        self.step_name    = None
        self.pre_task_ids = None
        self.delete_file  = False
        self.job_id       = None
        self.backend_id   = None
        self.nr_of_tries  = 0

        self.cmd = cmd
        self.step_name = step_name
        self.max_memory = None
        self.cputime    = None


        if ( limit is not None ):
            self.limit = limit

        if ( delete_file is not None ):
            self.delete_file = delete_file

        if ( thread_id is not None ):
            self.thread_id = thread_id


    def __getitem__(self, item):
        """ Generic getter function

        Raises:
          AttributeError is raised if trying to access value starting with _ or unknown value
        """
        
        if ( item.startswith("_")):
            raise AttributeError

        try:
            return getattr(self, item)
        except KeyError:
            raise AttributeError

    def __setitem__(self, item, value):
        """ Generic setter function

        Raises:
          AttributeError is raised if trying to access value starting with _ or unknown value
        """

        if ( item.startswith("_")):
            raise AttributeError
        
        try:
            return setattr(self, item, value)
        except KeyError:
            raise AttributeError

    def __repr__(self):
        return "{name} -> {status}".format( name=self.step_name, status=self.status )

    def __str__(self):
        return "{name}".format( name=self.step_name )

class Thread( object):

    def __init__(  self, name, thread_id ):
         self.name = name
         self.thread_id = thread_id
        

    def __getitem__(self, item):
        """ Generic getter function

        Raises:
          AttributeError is raised if trying to access value starting with _ or unknown value
        """
        
        if ( item.startswith("_")):
            raise AttributeError

        try:
            return getattr(self, item)
        except KeyError:
            raise AttributeError

    def __setitem__(self, item, value):
        """ Generic setter function

        Raises:
          AttributeError is raised if trying to access value starting with _ or unknown value
        """

        if ( item.startswith("_")):
            raise AttributeError
        
        try:
            return setattr(self, item, value)
        except KeyError:
            raise AttributeError


class Manager( object ):


    def __init__(self, pipeline):
        """ Creates a manager object

        """
        self._jobs = []
        self._active_jobs = []

        self._threads       = []
        self._thread_index  = {}
        self._thread_id = 1


        self.local_backend  = Local()
        self.backend        = None

        self.pipeline = pipeline


    def __getitem__(self, item):
        """ Generic getter function

        Raises:
          AttributeError is raised if trying to access value starting with _ or unknown value
        """
        
        if ( item.startswith("_")):
            raise AttributeError

        try:
            return getattr(self, item)
        except KeyError:
            raise AttributeError

    def __setitem__(self, item, value):
        """ Generic setter function

        Raises:
          AttributeError is raised if trying to access value starting with _ or unknown value
        """

        if ( item.startswith("_")):
            raise AttributeError
        
        try:
            return setattr(self, item, value)
        except KeyError:
            raise AttributeError

    def add_thread(self, name):
        """ Create a new thread object for the manager

        Args:
          name (str): name of the thread

        Returns:
          None

        """

        thread = Thread( name=name, thread_id=self._thread_id)
        self._threads.append( thread )
        self._thread_index[ name ] = self._thread_id 

        self._thread_id += 1


    def get_thread_by_name( self, name):
        """ gets a thread object based on name

        Args:
          name (str): name of the thread

        Returns:
          thread (obj)

        Raises:
          raises an assert error if the thead does not exist

        """

        assert name in self._thread_index, "No thread named {}".format( name )

        return self._threads[ self._thread_index[ name ]]


        
    def submit_job(self, cmd, step_name, output=None, limit=None, delete_file=None, thread_id=None, system_call=False):
        """ Submits a job  using the selected backend, setting up the tracking and all that jazz
    
        Args:
          cmd (str): command to run
          step_name (str): name of the step that this command belongs to
          output (str): output information to pass on to the next job
          limit (str): paramters to pass on to the backend
          delete_file (str): File(s) to delete if the job is successful
          thread_id (int): id of the thread running this 
          system_call (bool): run the job as a system job (default: false )

        Returns:
          None
          
       """

        job = Job(cmd, step_name, output, limit, delete_file, thread_id)
        self._jobs.append( job )
        job.job_id = len( self._jobs) - 1

#        print( "Working on: '{}' -> {}".format( job.step_name, job.cmd ))

        if ( system_call ) :
          job = self.local_backend.system_call( job )
        else:
          job = self.backend.submit( job )

#        print( job.status )
          

        
    def resubmit_job(self, job):
        """ resubmits a job
        
        Args:
          job (job): jobid to resubmit

        """

        job.nr_of_tries += 1
        job.status = Job_status.RESUBMITTED
        job = self.backend.submit( job )


    def killall(self):
        """kills all submitted/running jobs
        
        """

        for job_id, job in self.jobs:
            backend.kill( job )



    def job_outputs( self, step_name=None):
        """
        
        Args:
          step_name (str): name of the step to collect outputs from

        Returns:
          list of outputs

        """

        outputs = []
        prev_steps = self.pipeline._workflow.prev_steps( step_name )
#        print("{} :: Prev steps to collect outputs from: {}".format( step_name, prev_steps))
        for job in self._jobs:
            if job.step_name in prev_steps:
                outputs.append( job.output )


#        print("{}".format( outputs))
        return outputs


    def format_memory(self, memory):
        """ Format memory into a more readable format

        Args:
          memory (int): will be cast to float anyway

        Returns
          Readable memory (str)
        """

        memory = float( memory) 

        if memory is None or memory == 0:
            return "N/A"
        elif ( memory > 1000000000):
            return "{:.2f}GB".format(memory / 1000000000)
        elif ( memory > 1000000):
            return "{:.2f}MB".format(memory / 1000000) 
        elif ( memory > 1000):
            return "{:.2f}KB".format(memory / 1000) 
        else:
            return "{:}".format(int(memory))



    def format_time( self, seconds):
        """ Markes seconds into a more readable format eg: 10:03:01

        Args
          Seconds (int): seconds to convert into hours:mins:seconds

        returns:
          time (str)
        """

        if seconds is None:
            return "N/A"

        seconds = int( seconds )

        hours = int(seconds / 3600)
        seconds -= hours * 3600

        minutes = int(seconds / 60)
        seconds -= minutes * 60
        seconds = int(seconds )


        return "{:02}:{:02}:{:02}".format( hours, minutes, seconds)



    def report(self):
        """ print the current progress
        Args:
          None

        Returns:
          None

        """

        job_summary = {}
        for job in self._jobs:
            
            if job.step_name not in job_summary:
                job_summary[ job.step_name ] = {}
                job_summary[ job.step_name ][ 'DONE' ] = 0
                job_summary[ job.step_name ][ 'RUNNING' ] = 0
                job_summary[ job.step_name ][ 'QUEUING' ] = 0
                job_summary[ job.step_name ][ 'FAILED' ] = 0
                job_summary[ job.step_name ][ 'UNKNOWN' ] = 0
                job_summary[ job.step_name ][ 'max_mem' ] = 0
                job_summary[ job.step_name ][ 'cputime' ] = 0

            if job.status == Job_status.FINISHED:
                job_summary[ job.step_name ][ 'DONE' ] += 1
                if job.cputime is not None:
                    job_summary[ job.step_name ]['cputime'] += int(job.cputime)

                if job.max_memory is not None and job.max_memory  > job_summary[ job.step_name ][ 'max_mem']:
                    job_summary[ job.step_name ][ 'max_mem'] = int(job.max_memory)

            elif job.status == Job_status.RUNNING:
                job_summary[ job.step_name ][ 'RUNNING' ] += 1
            elif job.status == Job_status.QUEUEING or job.status == Job_status.SUBMITTED:
                job_summary[ job.step_name ][ 'QUEUING' ] += 1
            elif job.status == Job_status.FAILED or job.status == Job_status.NO_RESTART:
                job_summary[ job.step_name ][ 'FAILED' ] += 1
            else:
                job_summary[ job.step_name ][ 'UNKNOWN' ] += 1



        local_time = strftime("%d/%m/%Y %H:%M", time.localtime())
        

        pickle_file = "{}.{}".format(self.pipeline.project_name, self.pipeline._pid)

        print("[{} @{} {}]".format( local_time,self.pipeline._hostname , pickle_file))

        print("{:20} || {:12} || {:12} || {:2s} {:2s} {:2s} {:2s} {:2s}".format("Run stats", "Runtime", "Max Mem", "D","R","Q","F","U"))

        for step in sorted(self.pipeline._workflow._analysis_order, key=self.pipeline._workflow._analysis_order.__getitem__):
            if step not in job_summary:
                continue

            print("{:20} || {:12} || {:12} || {:02d}/{:02d}/{:02d}/{:02d}/{:02d}".format(step, 
                                                                                          self.format_time(job_summary[ step ]['cputime']),
                                                                                          self.format_memory(job_summary[ step ]['max_mem']),
                                                                                          job_summary[ step ][ 'DONE' ],
                                                                                          job_summary[ step ][ 'RUNNING' ],
                                                                                          job_summary[ step ][ 'QUEUING' ],
                                                                                          job_summary[ step ][ 'FAILED' ],
                                                                                          job_summary[ step ][ 'UNKNOWN' ]))
                                                                                          

            



    def active_jobs(self):
        """ updates the status of and returns all active jobs 

        Args:
          None

        Returns:
          list of jobs (obj)
          
        """
        
        active_jobs = []
        for job in self._jobs:
            if job.active:
                job.backend.status( job )
                active_jobs.append( job )

        self._active_jobs = active_jobs[:]

        return active_jobs


    def waiting_for_job(self,  depends_on ):
        """ check if any of the running jobs are in the depends list 

        Args:
          depends_on (list obj): list of steps to check again

        Returns:
          boolean, True if outstanding dependencies

        """

        # This code is aweful, but I don't have to time and brain
        # power to fix it right now

        for depend_on in depends_on:
            for active_job in self._active_jobs:
                if (active_job.active and  
                    depend_on.name == active_job.step_name ):
#                    print("waiting on {}".format(active_job.step_name))
                    return True



        for depend_on in depends_on:
            job_found = False
            for job in self._jobs:
                if (depend_on.name == job.step_name ):
                    job_found = True

            if not job_found:
                print("{} is waiting to start and finish {}".format( job.step_name, depend_on.name ))
                return  True


        # We are not waiting for any active or steps yet to be performed
        return False



    def failed_dependency_jobs(self,  depends_on ):
        """ check if any of the running jobs this one depends on have failed.

        Args:
          depends_on (list obj): list of steps to check again

        Returns:
          boolean, True if one or more job has failed and cannot be restarted

        """

        for depend_on in depends_on:
            for active_job in self._active_jobs:
                if (active_job.status == Job_status.NO_RESTART):
                    print("dependecy {} failed".format(active_job.step_name))
                    return True

        return False
          



    def _next_id():
	'''  generates and returns the next job id from the class

	Returns:
          Next available job id (int)

	'''
        self.job_id += 1

        return self.job_id




