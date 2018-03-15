import subprocess
import shlex
import os
import re
import xml.etree.ElementTree as ET


class Local (backend.Backend ):


    def submit_job(self, job ):
    
    qsub_cmd = "qsub -cwd -S /bin/sh " + job.limit


    args = shlex.split(qsub_cmd)


    p = subprocess.Popen(args, shell=True, 
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE)

    output = p.communicate(cmd)
    job_id = re.match('Your job (\d+)', str(output[0]))
    job_id = jobid.groups(0)

    job.job_id = job_id

    return str(jobid[0])


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

#        print key + " -- " + value + "]"
        data[ key ] = value

    if ( "exit_status" in data and data[  "exit_status" ]  == '0' ):
        return 1
    elif ( "exit_status" in data and data[  "exit_status" ]  != '0' ):
        return 0
    else:
        #for unfinished...
        return -1





    
