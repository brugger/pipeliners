import subprocess
import shlex
import os
import re
import pprint as pp

import backend
#from manager import * 

import manager
import traceback 


class Slurm ( backend.Backend ):


    def submit_job(self, job ):

        SLURM_cmd  = " sbatch -J {} ".format( "CCBG")

        p = subprocess.Popen(shlex.split(SLURM_cmd), shell=False, 
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE)

        output = p.communicate("#!/bin/bash \n{}".format( job.cmd));
        status = p.wait()

        if status is None:
            # This should not happen!
            job.status = manager.Job_status.UNKNOWN
        elif status == 0:
            job.status = manager.Job_status.SUBMITTED
            job_id = re.match('Submitted batch job (\d+)', str(output[0]))
            job.backend_id = job_id
        else:
            job.status = manager.Job_status.FAILED


        return job

    def status(self, job ):

    def kill(self, job):

        p = subprocess.Popen("scancel {}".format(job.backend_id), shell=False)
        p.wait()

        if status is None:
            # This should not happen!
            job.status = manager.Job_status.UNKNOWN
        elif status == 0:
            job.status = manager.Job_status.KILLED
        else:
            job.status = manager.Job_status.UNKOWN


    def available(self, job):


    def job_finished(jobid):

        qstat_cmd = "qacct -j "+ jobid  
        
        args = shlex.split(qstat_cmd)
        p = subprocess.Popen(args, stdout=subprocess.PIPE)

        output = p.communicate()
        data = dict()
        for line in ( output[0].split("\n")):
            if re.match("====", line):
                continue
                    
            line = line.rstrip("\n")
            line = line.rstrip(" ")
            values = shlex.split(line,2)
            if ( not values ):
                continue
            
        key = values[0]
        value = " ".join(values[1:])
        data[ key ] = value

        if ( exit_status in data ):
            return 1
        else:
            return 0


    def job_successful(jobid):

        qstat_cmd = "qacct -j "+ jobid  
            
        args = shlex.split(qstat_cmd)
        p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        output = p.communicate()
        data = dict()
        for line in ( output[0].split("\n")):
            if re.match("====", line):
                continue

            line = line.rstrip("\n")
            line = line.rstrip(" ")
            values = shlex.split(line,2)
            if ( not values ):
                continue
        
            key = values[0]
            value = " ".join(values[1:])

            data[ key ] = value

            if ( "exit_status" in data and data[  "exit_status" ]  == '0' ):
                return 1
            elif ( "exit_status" in data and data[  "exit_status" ]  != '0' ):
                return 0
            else:
                #for unfinished...
                return -1





    
