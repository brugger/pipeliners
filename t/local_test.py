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
from pipeliners import *



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
    J.backend.wait( J )

    assert J.status == Job_status.FAILED



def test_delete_tmp_file():
    
    L = Local()
    # Create a tmp file
    J1 = Job("touch tmp_file.txt", "create_file")
    L.system_call( J1 )
    assert os.path.isfile("tmp_file.txt") == True

    J2 = Job('echo "Lets delete a file"', "delete_file", delete_file="tmp_file.txt")
    

    L.submit( J2 )
    J2.backend.wait( J2 )
    J2.delete_tmp_files()
    assert os.path.isfile("tmp_file.txt") == False


def test_delete_tmp_files_and_systemcall():

    P = Pipeline()

    P.system_call("touch tmp_file_1.txt")
    P.system_call("touch tmp_file_2.txt")

    assert os.path.isfile("tmp_file_1.txt") == True
    assert os.path.isfile("tmp_file_2.txt") == True

    L = Local()
    J2 = Job('echo "Lets delete a file"', "delete_file", delete_file=["tmp_file_1.txt", "tmp_file_2.txt"])
    

    L.submit( J2 )
    J2.backend.wait( J2 )
    J2.delete_tmp_files()
    assert os.path.isfile("tmp_file.txt") == False
