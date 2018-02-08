#!/usr/bin/python
# 
# 
# 
# 
# Kim Brugger (07 Nov 2017), contact: kim@brugger.dk

import sys
import pprint
pp = pprint.PrettyPrinter(indent=4)


#
#import sqlalchemy
sys.path.append('./ccbg_pipeline/')
import pytest
import workflow as w


def a():
    print('A')


def test_step_init_wrong_type():

    with pytest.raises( AssertionError ):
        step = w.Step( workflow = None,
                       name = 'step1', 
                       function = a,
                       step_type='startss')

#    assert step.name   == 'step1'

def test_step_init_illigal_variable():

    step = w.Step( workflow = None,
                   name = 'step1', 
                   function = a,
                   step_type='start')

    
    with pytest.raises( AttributeError ):
        step._name

def test_step_init_unknown_variable():

    step = w.Step( workflow = None,
                   name = 'step1', 
                   function = a,
                   step_type='start')

    
    with pytest.raises( AttributeError ):
        step._names == 'tyt'


def test_step_init():
    # Tests the creation and the generic getter function

    step = w.Step( workflow = None,
                   name = 'step1', 
                   function = a,
                   step_type='start')

    step.name ='step2'

    assert step.name   == 'step2'
    assert step.workflow   == None
    assert step.function   == a
    assert step.step_type   == 'start'


def test_workflow_start_step():
    # Tests the creation and the generic getter function
    
    W = w.Workflow()
    step = W.start_step( a, "start_a")

    step.name ='step2'

    assert step.name       == 'step2'
    assert step.function   == a
    assert step.step_type  == 'start'
    assert W.start_steps() == [ step ]
    assert W.steps() == [ step ]




