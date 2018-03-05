#
# 
# 
##

from __future__ import print_function, unicode_literals
import inspect
import os
import socket
import pprint as pp
import time

from workflow import *
from manager  import *

DEBUG = 0


class Pipeline( object ):
    """ The main pipeline class that the user will interact with """


    def __init__(self):
        """ Create a pipeline """

        self.project_name = "CCBG" 
        self.queue_name   = ""
        self.project       = ""

    
        # For housekeeping to see how long the processing took
        self._start_time = None
        self._end_time   = None

        # when was the run information last saved
        self._last_save      =   None
        # How often to save, in secs
        self.save_interval  = 300

        self.max_retry      =   3
        self._failed_steps  =   0 # failed jobs that cannot be restarted. 

        self.sleep_time     =   30
        self.max_sleep_time =  300
        self.sleep_start    =  self.sleep_time
        self.sleep_increase =   30

        if ( 1 ):
            self.sleep_time     =   3
            self.max_sleep_time =  3
            self.sleep_start    =  self.sleep_time
            self.sleep_increase =   1

        # to control that we do not flood the hpc with jobs, or if local block server machine.
        # -1 is no limit
        self.max_jobs       =  -1 

        self._use_storing    =   1 # debugging purposes
        self._freeze_file = None
        
        self._delete_files = []
        
        self._cwd      = os.getcwd()

        # Setup helper classes, step manager tracks the steps in the
        # pipeline and the job-manager the running of actual executation
        # of steps as jobs
        self._workflow = Workflow()
        self._manager = Manager(pipeline=self)

        self._step_name = None
        self._thread_id = None

        self._pid       = os.getpid()
        self._hostname  = socket.gethostname()


    # generic ge
    def __getitem__(self, item):
        """ generic geter function, variable starting with _ are ignored as are private """
        
        if ( item.startswith("_")):
            raise AttributeError

        try:
            return getattr(self, item)
        except KeyError:
            raise AttributeError


    def __setitem__(self, item, value):
        """ generic setter function, variable starting with _ are ignored as are private """

        if ( item.startswith("_")):
            raise AttributeError
        
        try:
            return setattr(self, item, value)
        except KeyError:
            raise AttributeError


    def backend(self, backend):
        self._manager.backend = backend

    def backend_name(self):
        print("Backend name: {}".format(self._manager.backend.name()))

    def start_step(self, function, name=None):
        return self._workflow.start_step(function, name)

    # Generic step adder, wrapped in the few functions below it
    def add_step( self, prev_step, function, name=None, step_type=None):
        return self._workflow.add_step(prev_step, function, name, step_type)

    # Simple wrapper functions for the generic add_step function.
    def next_step(self, prev_step, function, name=None):
        return self._workflow.add_step( prev_step, function, name);

    def global_merge_step(self, prev_step, function, name=None):
        return self._workflow.add_step( prev_step, function, name);

    def thread_merge_step(self, prev_step, function, name=None):
        return self._workflow.add_step( prev_step, function, name);

    def print_workflow(self, starts=None):
        self._workflow.print_flow( starts )


    def system_job(self, cmd, output=None, delete_file=None):
        return self.submit_job(cmd, output=output, limit=None, delete_file=delete_file, system_call=True)


    def submit_job(self, cmd, output=None, limit=None, delete_file=None, system_call=False):
        self._manager.submit_job( cmd, self._step_name, output, limit, delete_file, thread_id=self._thread_id, system_call=system_call )


    def _sleep(self, active_jobs=None):
        """ sleeps the loop, if there are no active jobs (eg running or started) increase sleep time 

        Args:
          active_jobs (int): any active jobs, resets the sleep time

        Returns:
          none

        """

        if active_jobs is not None and active_jobs > 0:
            self._sleep_time = self.sleep_start
        elif ( self.max_sleep_time < self.sleep_time):
            self.sleep_time += self.sleep_increase


        time.sleep( self.sleep_time)



    def run( self, starts=None, args=None ):
        """ Run the tasks and track everything

        Args:
          Start tasks (list of str): default it pull from the workflow
          args (list of str): input(s) for the start(s)

        Returns:
          Nr of failed jobs (int)

        """

        # if no start states selected, pull from the workflow, if
        # steps have been provided translate names to states.
        if starts is None:
            starts = self._workflow.start_steps()
        else:
            starts = self._workflow.states_by_name( starts )
            

        # Kick off the start jobs before starting to spend some quality tom in the main loop...
        for start in starts:
            self._step_name  = start.name
            start.function( args )
            
        
        while ( True ):

#            print( "Pulling job ...")

            # Pull the satus on all jobs, and return the active  ones. Active being non-finished
            active_jobs = self._manager.active_jobs();

            started_jobs = 0
            queued_jobs  = 0
            running_jobs = 0
            
            for job in active_jobs:

#                print( "Checking in on job {}/{}".format( job.step_name, job.status ))

                self._step_name = job.step_name
#                self._thread_id = job.thread_id

                if job.status == Job_status.FINISHED:
                    job.active = False
                    
                    next_steps = self._workflow.next_steps( job.step_name )

                    # Nothing after this step, looppon to the next job
                    if next_steps is None:
                        continue

                    for next_step in next_steps:
#                        print( "Next step is {}".format( next_step.name))
                        # The next step is either a global sync or a
                        # thread sync, so things are slightly
                        # complicated and we need to check the states
                        # of a ton of jobs
                        if next_step.step_type == 'sync' or next_step.step_type == 'thread_sync':
#                            print("Sync step ")
                            # A global sync is equal to thread_id being 0 (top level)
                            # Threading is really not tobe working for this version.
#                            if ( next_step.step_type == 'sync' ):
#                                self._active_thread_id = 0
#                            else:
#                                self._active_thread_id = next_step.thread_id
                    
                            # Check if the next step is depending on
                            # something running or queuing
                            step_depends_on = self._workflow.get_step_dependencies( next_step )
#                            print("Dependencies {}".format(step_depends_on))
                            if self._manager.waiting_for_job( step_depends_on ):
#                                print( "step is waiting for something...")
                                continue
                            
                            if self._manager.failed_dependency_jobs( step_depends_on):
                                break


#                        print( "kicking of next step...")

                        self._step_name  = next_step.name
                        
                        if next_step.step_type == 'sync' or next_step.step_type == 'thread_sync':
                            job_outputs = self._manager.job_outputs( next_step.name )
                        else:
                            job_outputs = job.output

                        next_step.function( job_outputs )
                        started_jobs += 1

                elif job.status == Job_status.QUEUEING:
                    queued_jobs += 1

                elif job.status == Job_status.RUNNING:
                    running_jobs += 1

                elif job.status == Job_status.FAILED:
                    if job.nr_of_tries < self.max_retry:
                        self._manager.resubmit_job( job )
                        started_jobs += 1
                    else:
                        job.active = False
                        job.status = Job_status.NO_RESTART
                        self._failed_steps += 1

                elif  job.status == Job_status.FAILED or job.status == Job_status.KILLED:
                    job.active = False

            if (started_jobs  + running_jobs + queued_jobs == 0):
                break

            self._manager.report()
            self._sleep( started_jobs  + running_jobs )


        self._manager.report();
        print("The pipeline finished with {} job(s) failing\n".format(self._failed_steps));

        if ( 0 ):
            import pickle
            output = open("{}.{}".format(self.project_name, self._pid), 'wb')
            pickle.dump(self, output, -1)
            output.close()
  
        return self._failed_steps



