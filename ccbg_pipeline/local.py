import traceback
import subprocess
import shlex

import backend
from manager import * 



class Local (backend.Backend ):

    
    def __init__(self):
        self._processes = {}

    def submit(self, job):
        
        cmd = shlex.split( job.cmd )
        job.backend = self

        try:
            p = subprocess.Popen( cmd )
            job.status = Job_status.SUBMITTED
            
            self._processes[ job.job_id ] = p
            job.status = self.status( job )

        except:
            job.status = Job_status.FAILED
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
            job.status = Job_status.RUNNING
        elif status == 0:
            job.status = Job_status.FINISHED
        else:
            job.status = Job_status.FAILED

        return job.status

    def kill(self, job):
        
        p = self._processes[ job.job_id  ];
        p.kill( )
        p.status = Job_status.KILLED

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
        
        cmd = shlex.split( job.cmd )
        try:
            status = subprocess.check_call( cmd )
            if status == 0:
                job.status = Job_status.FINISHED
            else:
                job.status = Job_status.FAILED
        except:
            job.status = Job_status.FAILED
