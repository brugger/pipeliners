#
# 
# 
##

from __future__ import print_function, unicode_literals
import inspect
import os
import pprint as pp
import time

from workflow import *
from manager  import *
from local    import *

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
        self._sleep_start   =  sleep_time
        self.sleep_increase =   30


        self.backend        = Local()


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
        self._job_manager = Manager()


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
        self._workflow.print( starts )


    def run(self, starts=None):
        """ Run the workflow, it is possible to override the start step(s)
        
        Args:
          starts (list of str): list of start step(s)
        Returns:
          Nr of jobs that failed completing (int)

        """

        pp.pprint( starts)

        
        # If no specific start point is provided use what was used when the pipeline was defined.
        # The program expects these as steps  so translate the step names to step objects
        if starts is None:
            starts = self._workflow.start_steps()
        else:
            starts = self.steps_by_name( starts )


        # start all the start steps, and start spending some quality time in the main loop
        for start in starts:
            self._run_job( start );
            queued += 1

        print( "Running the pipeline loop")
        while ( True ):

            # to check what the number of job statuses have changed 
            # Mainly used for increasing the sleep time if needed.
            (started, queued, running ) = (0,0,0)
            
            # Fetch all the active jobs from the job_manager
            active_jobs = self._job_manager.active_jobs();

            if ( active_jobs == [] and not self._restarted_run ):

                continue
            
            for active_job in active_jobs:
                # The job is no longer being tracked either due to crashing or finishing.
                # This should not happen as we are looping through the active jobs only.
                if ( not active_job.tracking ):
                    continue
                

                step_name = active_job.step
                tread_id  = active_job.thread_id

                if ( active_job.status == Job_status.FINISHED ):
                    # disable tracking of the job
                    active_job.tracking = 0
                    # fetch the steps that depended on this step
                    next_steps = self._workflow.next( step_name )

                    # If none, go to the next active job
                    if ( next_steps is None or len(next_steps) == 0):
                        continue

                    # Looping though the step dependencies
                    for next_step in next_steps:
                        # if the next steps is a sync or thread-sync
                        if ( next_step.sync == 'sync' or next_step.sync == 't_sync'):
                            if ( next_step.sync == 'sync'):
                                thread_id = 0

                            if ( self._thread_manager.no_restart( thread_id )):
                                continue

                            if ( retained_jobs > 0 ):
                                continue

                            if ( self._job_manager.depends_on_active_jobs( next_step )):
                                 continue

                            depends_on = []
                            for step in self._task_manager.flow.keys():
                                for analysis in self._task_manager.flow( step ):
                                    depends_on.append( analysis )
                                

                            depends_jobs = fetch_jobs( depends_on )
                            all_treads_done = 0

                            for job in  depends_jobs :
                                if ( job.status != Job_status.FINISHED ):
                                    all_threads_done = 0
                                    break
                                
                                     # active_thread_id aware...

                            if ( all_threads_done ):

                                inputs  = []
                                job_ids = []
                                
                                for job in depends_jobs:
                                    job.tracking = 0
                                    inputs.append( job.output )
                                    job_ids.append( jobs )
		
		

                            self.run_analysis( next_step, job_ids, inputs);
                            started += 1
                            
                    else:
                            run_analysis( next_step, job, job.output)
                            started += 1

                elif (job.status == Job_status.FAILED or job.status == Job_status.KILLED):
                    job.tracking = 0
                elif ( job.status == Job_status.RUNNING):
                    queued += 1
                    running += 1
                else:
                    queued += 1

                    

            while ( self.max_jobs > 0 and self._job_submitted < self.max_jobs and len( self.retained_jobs )):

                params = retained_jobs.pop()
                started += 1

                
                check_n_store_state()
                    
            if ( len( queued ) == 0 and started == 0 and len( retained_jobs ) == 0):
                break

            if ( not queued and not started and len(retained_jobs) == 0):
                break


            if ( running == 0 and self.sleep_time < self.max_sleep_time):
                self.sleep_time += self.sleep_increase

            if ( running != 0 ):
                self.sleep_time = self._sleep_start
    

            time.sleep ( self.sleep_time )
            self._job_manager.check_jobs()

#  print report()
#  report2tracker() if ($database_tracking)
        print( self.total_runtime())
        print( self.real_runtime())

        if ( no_restart ):
            print("The pipeline was unsucessful with $no_restart job(s) not being able to finish\n")
        
  

        if ( len(retained_jobs) > 0):
            print("Retaineded jobs: " +  retained_jobs +  " (should be 0)\n")


        return no_restart



