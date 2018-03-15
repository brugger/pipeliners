#!/usr/bin/python
# 
# 
# 
# 
# Kim Brugger (06 Jun 2017), contact: kim@brugger.dk

import sys
import pprint
pp = pprint.PrettyPrinter(indent=4)

#import  ccbg_pipeline.task

from ccbg_pipeline import *

#print task_status.SUBMITTED

#pp.pprint( P )
#print ("Max retry: {}".format( P.max_retry ) )
#P.max_retry = 5 
#print ("Max retry: {}".format( P.max_retry ) )

def a( inputs=None ):
    print( "generating some jobs from: A {}".format( inputs ))
    P.submit_job('sleep 10; echo "hello A:10 "', output='a1')
    P.submit_job('sleep 20; echo "hello A:20 "', output='a2')

def b(inputs=None):
    print( "B {}".format( inputs ))

    P.submit_job('sleep 10; echo "hello B:10 "', output='b1')



def c(inputs=None):
    print( "C {}".format( inputs ))
    P.submit_job('sleep 10; echo "hello C:10 "', output='c1')


def d(inputs=None):
    print( "D {}".format( inputs ))
    P.submit_job('sleep 10; echo "hello D:10 "', output='d1')


def e(inputs=None):
    print( "E {}".format( inputs ))
    P.submit_job('sleep 3; echo "hello E:3 "',   output='e1')
    P.submit_job('sleep 3; echo "hello E:3 "',   output='e2')
    P.submit_job('sleep 3; echo "hello E:3 "',   output='e3')
    P.submit_job('sleep 10; echo "hello E:10 "', output='e4')


def f(inputs=None):
    print( "F {}".format( inputs ))
    P.submit_job('sleep 10; echo "hello F:10 "', output='f1')


def q(inputs=None):
    print( "Q {}".format( inputs ))
    P.submit_job('sleep 10; echo "hello Q:10 "', output='q1')



P = Pipeline()
#P.backend( Local() )
P.backend( Slurm() )




prelim_steps = P.start_step( a ).merge( b ).next( c )
prelim_steps.next( q ).merge( f )
post_steps = P.add_step( b, d ).next( e ).merge( f )

#P.print_workflow( )

#P.run(['a'])
P.run()
