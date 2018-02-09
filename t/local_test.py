#!/usr/bin/python
# 
# 
# 
# 
# Kim Brugger (07 Nov 2017), contact: kim@brugger.dk

import sys
import pprint
pp = pprint.PrettyPrinter(indent=4)
from time import sleep

#
sys.path.append('./ccbg_pipeline/')
import pytest
from local import *
from manager import *



def test_system_call_success():
    
    L = Local()
    J = Job('echo "Hello good old system world"', "echo_test")

    L.system_call( J )
    print( J.status )

    assert J.status == Job_status.FINISHED


def test_system_call_failure():
    
    L = Local()
    J = Job('echoss "Hello good old world"', "echo_test2")

    L.system_call( J )

    assert J.status == Job_status.FAILED


def test_submit_success_running():
    
    L = Local()
    J = Job('echo "Hello good old world"', "echo_test")

    L.submit( J )
    assert L.status( J ) == Job_status.RUNNING

def test_submit_success_sleep():
    
    L = Local()
    J = Job('sleep 40', "echo_test")

    L.submit( J )
    assert L.status( J ) == Job_status.RUNNING


def test_submit_success():
    
    L = Local()
    J = Job('echo "Hello good old world"', "echo_test")

    L.submit( J )
    sleep( 0.5 )
    L.status( J )
    assert L.status( J ) == Job_status.FINISHED


def test_submit_success_wait():
    
    L = Local()
    J = Job('echo "Hello good old world"', "echo_test")

    L.submit( J )
    L.wait( J )

    assert J.status == Job_status.FINISHED

def test_submit_success_wait2():
    
    L = Local()
    J = Job('echo "Hello good old world"', "echo_test")

    L.submit( J )
    J.backend.wait( J )

    assert J.status == Job_status.FINISHED


def test_submit_failure():
    
    L = Local()
    J = Job('echoss "Hello good old world"', "echo_test2")

    L.submit( J )

    assert J.status == Job_status.FAILED
