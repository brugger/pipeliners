#
# 
# 
##

from __future__ import print_function, unicode_literals
import inspect
import os
import pprint as pp
import time
import subprocess

from local import *



class Job_status( object ):
    """ Enumerate class for job statuses, this is done differently in python 3

    """
    FINISHED    =    1
    FAILED      =    2
    RUNNING     =    3
    QUEUEING    =    4
    RESUBMITTED =    5
    SUBMITTED   =    6
    KILLED      =   99
    UNKNOWN     =  100


class Job(object):
    """ This class is presenting a singular job and all information associated with it. 

    """


    def __init__(self,  cmd, step_name, limit=None, delete_file=None, thread_id=None):
        """ Create a job object
        
        Args:
          cmd (str): command to run
          step_name (str): name of the step that this command belongs to
          limit (str): paramters to pass on to the backend
          delete_file (str): File(s) to delete if the job is successful
          thread_id (int): id of the thread running this 

        Returns:
          job (obj)
        """


        self.status   = Job_status.SUBMITTED
        self.active   = True
        self.command  = None
        self.backend  = None

        self.output_file  = None
        self.step_name    = None
        self.pre_task_ids = None
        self.delete_file  = False
        self.job_id       = None
        self.backend_id   = None
        self.nr_of_tries  = 0

        self.cmd = cmd
        self.step_name = step_name

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
        return "{name}".format( name=self.step_name )

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

        assert  name in self._thread_index, "No thread named {}".format( name )

        return self._threads[ self._thread_index[ name ]]



    def submit_job(self, cmd, step_name, limit=None, delete_file=None, thread_id=None, system_call=False):
        """ Submits a job  using the selected backend, setting up the tracking and all that jazz
    
        Args:
          cmd (str): command to run
          step_name (str): name of the step that this command belongs to
          limit (str): paramters to pass on to the backend
          delete_file (str): File(s) to delete if the job is successful
          thread_id (int): id of the thread running this 
          system_call (bool): run the job as a system job (default: false )

        Returns:
          None
          
       """


        job = Job(cmd, step_name, limit, delete_file, thread_id)
        self._jobs.append( job )
        job.job_id = len( self._jobs) - 1

#        print( "Working on: '{}' -> {}".format( job.step_name, job.cmd ))

#        print(type( job ))
#        print(type( self.backend ))

        if ( system_call ) :
          job = local.system_call( job )
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
        job = self._backend.submit_job( job )


    def killall(self):
        """kills all submitted/running jobs
        
        """

        for job_id, job in self.jobs:
            backend.kill( job )




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

        for depend_on in depends_on:
            for active_job in self._active_jobs:
                if (active_job.active and  
                    depend_on.name == active_job.step_name ):
                    print("waiting on {}".format(active_job.step_name))
                    return True

        return False
          



    def _next_id():
	'''  generates and returns the next job id from the class

	Returns:
          Next available job id (int)

	'''
        self.job_id += 1

        return self.job_id




