import traceback
import subprocess
import shlex
import time

from backend import *
from manager import * 

import manager

class Local ( Backend ):

    
    def __init__(self):
        self._processes  = {}
        self._start_time = {}

    def submit(self, job):

#        print( "Trying to run command: '{}'".format( job.cmd ))
        
        cmd = shlex.split( job.cmd )
        job.backend = self

        try:
            # shell to be true for more complex oneliners. Just for testing
            # and dont need to split the cmd
            cmd = job.cmd
            p = subprocess.Popen( cmd, shell=True )
            job.status = manager.Job_status.SUBMITTED

            job.backend_id = p.pid
            
            self._processes[ job.job_id ]  = p
            self._start_time[ job.job_id ] = time.time()
            job.status = self.status( job )

        except:
            job.status = manager.Job_status.FAILED
            traceback.print_exc()

        return job
        

    def wait(self, job ):
        """ Waits for a job to finish 
        Updates status at the same time

        Args:
          job (obj): job to wait for

        Returns:
          job (obj)
          
        """
        p = self._processes[ job.job_id  ];
        p.wait()
        job.status = self.status( job )
        return job
        

    def status(self, job ):
        
        p = self._processes[ job.job_id  ];

        status = p.poll()
        if status is None:
            job.status = manager.Job_status.RUNNING
        elif status == 0:
            job.status = manager.Job_status.FINISHED
            job.cputime = time.time() - self._start_time[ job.job_id ]
        else:
            job.status = manager.Job_status.FAILED

        return job.status

    def kill(self, job):
        
        p = self._processes[ job.job_id  ];
        p.kill( )
        p.status = manager.Job_status.KILLED

        return job


    def available(self, job):
        return True


    def system_call(self, job):
        """ executes a system call and wait for it to finish

        Args:
          job (obj): job to wait for

        Returns:
          job (obj)
          
        """

        job = self.submit(job)
        job = self.wait(job)
        return job
