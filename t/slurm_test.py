#!/usr/bin/python
# 
# 
# 
# 
# Kim Brugger (07 Nov 2017), contact: kim@brugger.dk

import sys
import pprint
pp = pprint.PrettyPrinter(indent=4)

sys.path.append('./ccbg_pipeline/')
import pytest

from local import *
from slurm import *
from manager import *


def test_submission():
    
    S = Slurm()
    J = Job('sleep 60; echo "Hello good old system world"', "echo_test")

    S.submit_job( J )
    print( J.status )


    assert J.status == Job_status.SUBMITTED



def test_submission_pending():
    
    S = Slurm()
    J = Job('sleep 60; echo "Hello good old system world"', "echo_test")

    S.submit_job( J )
    print( J.status )

    print(J.backend_id)
    S.status( J )

    assert J.status == Job_status.QUEUEING
