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

def b():
    print('B')

def c():
    print('C')


def test_step_init_wrong_type():

    with pytest.raises( AssertionError ):
        step = w.Step( workflow = None,
                       function = a,
                       name = 'step1', 
                       step_type='startss')

#    assert step.name   == 'step1'

def test_step_init_illigal_variable():

    step = w.Step( workflow = None,
                   function = a,
                   name = 'step1', 
                   step_type='start')

    
    with pytest.raises( AttributeError ):
        step._name

def test_step_init_unknown_variable():

    step = w.Step( workflow = None,
                   function = a,
                   name = 'step1', 
                   step_type='start')

    
    with pytest.raises( AttributeError ):
        step._names == 'tyt'


def test_step_init():
    # Tests the creation and the generic getter function

    step = w.Step( workflow = None,
                   function = a,
                   name = 'step1', 
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

def test_workflow_step():
    # Tests the creation and the generic getter function
    
    W = w.Workflow()
    start_step = W.start_step( a, "start_a")
    step = W.add_step(start_step,  b, "step_b")

    assert step.name       == 'step_b'
    assert step.function   == b
    assert step.step_type  == None
    assert W.start_steps() == [ start_step ]
    assert W.steps()       == [ start_step, step ]

def test_workflow_step():
    # Tests the creation and the generic getter function
    
    W = w.Workflow()
    w_st = W.start_step( a, "start_a").next( b, 'step_b')

    start_step = w.Step( W, a, "start_a", step_type='start')
    step = w.Step(W, function=b, name="step_b")

    assert step.name       == 'step_b'
    assert step.function   == b
    assert step.step_type  == None
    assert W.start_steps() == [ start_step ]
    assert W.steps()       == [ start_step, step ]

def test_workflow_step_merge():
    # Tests the creation and the generic getter function
    
    W = w.Workflow()
    w_st = W.start_step( a, "start_a").next( b, 'step_b').merge(c)

    step_a = w.Step( W, a, "start_a", step_type='start')
    step_b = w.Step(W, function=b, name="step_b")
    step_c = w.Step(W, function=c, name="workflow_test.c", step_type='sync')

    assert step_c.function == c
    assert W.start_steps() == [ step_a ]
    assert W.steps()       == [ step_a, step_b, step_c ]

def test_workflow_step_thread_merge():
    # Tests the creation and the generic getter function
    
    W = w.Workflow()
    w_st = W.start_step( a, "start_a").next( b, 'step_b').thread_merge(c)

    step_a = w.Step( W, a, "start_a", step_type='start')
    step_b = w.Step(W, function=b, name="step_b")
    step_c = w.Step(W, function=c, name="workflow_test.c", step_type='thread_sync')

    assert step_c.function == c
    assert W.start_steps() == [ step_a ]
    assert W.steps()       == [ step_a, step_b, step_c ]
    assert W.step_by_name( 'step_b' ) == step_b 
    assert W.steps_by_name( ['step_b'] ) == [ step_b ]
    assert W.steps_by_name( ['start_a', 'step_b'] ) == [step_a, step_b ]
    assert W.next_steps( step_b ) == [ step_c ]
    assert W.next_steps( step_b ) == [ step_c ]

    assert W.get_step_dependencies( step_a ) == None
    assert W.get_step_dependencies( step_c ) == sorted([ step_a, step_b ])


