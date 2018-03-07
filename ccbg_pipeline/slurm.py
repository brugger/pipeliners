import subprocess
import shlex
import os
import re
import pprint as pp

import backend

import manager
import traceback 


class Slurm ( backend.Backend ):

    def name(self):
        """ returns name of the backend
        Args: 
          None
        
        Returns:
          backend name (str)
        """

        return "SLURM backend class"


    def submit(self, job ):

        job.backend = self

        SLURM_cmd  = " sbatch -J {} ".format( "CCBG")
        if ( job.limit is not None):
            SLURM_cmd += " {} ".format(job.limit)


        print("{} -> {}".format( SLURM_cmd, job.cmd))

        p = subprocess.Popen(shlex.split(SLURM_cmd), shell=False, 
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE)

        (stdout, stderr) = p.communicate("#!/bin/bash \n{}".format( job.cmd));
        status = p.wait()

        if status is None:
            # This should not happen!
            job.status = manager.Job_status.UNKNOWN
        elif status == 0:
            job.status = manager.Job_status.SUBMITTED
            job_id = re.match('Submitted batch job (\d+)', stdout)
            job_id = job_id.group( 1 )
            job.backend_id = job_id
        else:
            job.status = manager.Job_status.FAILED

        return job

    def status(self, job ):

        p = subprocess.Popen(shlex.split("sacct --format JobIDRaw,ExitCode,State,Elapsed,CPUTime,MaxRss -np -j {}".format(job.backend_id)), 
                             shell=False,
                             stdout=subprocess.PIPE)
        status = p.wait()
        (stdout, stderr) = p.communicate()

        if status is None:
            # This should not happen!
            job.status = manager.Job_status.UNKNOWN
        elif status == 0:

            # If the job has finised it does a batch line as well, so
            # take this into account and return the second last line,
            # The last one is empty!

            if ( stdout.count("\n") > 1):
#                pp.pprint( stdout)
                lines = stdout.split("\n")
#                pp.pprint( lines )
                stdout = lines[-2]

            fields = stdout.split("|")
#            pp.pprint( fields )
            (job_id, exit_code, status, elapsed, cputime, max_mem, undef) = fields

            if ( status == 'FAILED'    or 
                 status == "NODE_FAIL" or
                 status == "CANCELLED" or
                 status == "TIMEOUT"   or
                 status == "PREEMPTED" ):
                job.status = manager.Job_status.FAILED
            elif (status == 'RUNNING'):
                job.status = manager.Job_status.RUNNING

            elif (status == 'PENDING' or
                  status == 'SUSPENDED'):
                job.status = manager.Job_status.QUEUEING
            elif (status == 'COMPLETED' ):
                job.status = manager.Job_status.FINISHED

                print("Max mem: {}".format( max_mem))

                if ( re.match('(\d+)K', max_mem)):
                    m = re.match('(\d+)K', max_mem)
                    job.max_memory = int(m.group(1))* 1000
                     
                elif ( re.match('(\d+)M', max_mem)):
                    m = re.match('(\d+)M', max_mem)
                    job.max_memory = int(m.group(1))* 1000000
                else:
                    job.max_memory = max_mem

                print("Max mem: {}".format(job.max_memory))


                if (re.match('(\d+):(\d+):(\d+)', elapsed)):
                    m = re.match('(\d+):(\d+):(\d+)', elapsed)
                    job.cputime = 3600 * int(m.group(1)) + 60*int(m.group(2)) + int(m.group(3))
                else:
                    print("Unknown cputime format {}".format( cputime))


                print("Removing file 'slurm-{}.out'".format(job.backend_id))
                os.unlink("slurm-{}.out".format(job.backend_id))

            else:
                job.status = manager.Job_status.UNKOWN

        return job.status

    def kill(self, job):

        p = subprocess.Popen("scancel {}".format(job.backend_id), shell=False)
        status = p.wait()

        if status is None:
            # This should not happen!
            job.status = manager.Job_status.UNKNOWN
        elif status == 0:
            job.status = manager.Job_status.KILLED
        else:
            job.status = manager.Job_status.UNKOWN


    def available(self, job):
        pass
