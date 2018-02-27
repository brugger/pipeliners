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
    print "generating some jobs from: A"
    P.submit_job('sleep 10; echo "hello A:10 "', 'sleep_10 ')
    P.submit_job('sleep 20; echo "hello A:20 "', 'sleep_20 ')

def b(inputs=None):
    print "B"

    P.submit_job('sleep 10; echo "hello B:10 "', 'sleep_10 ')



def c(inputs=None):
    print "C"
    P.submit_job('sleep 10; echo "hello C:10 "', 'sleep_10 ')


def d(inputs=None):
    print "D"
    P.submit_job('sleep 10; echo "hello D:10 "', 'sleep_10 ')


def e(inputs=None):
    print "E"
    P.submit_job('sleep 3; echo "hello E:3 "', 'sleep_3 ')
    P.submit_job('sleep 3; echo "hello E:3 "', 'sleep_3 ')
    P.submit_job('sleep 3; echo "hello E:3 "', 'sleep_3 ')
    P.submit_job('sleep 10; echo "hello E:10 "', 'sleep_10 ')


def q(inputs=None):
    print "Q"
    P.submit_job('sleep 10; echo "hello Q:10 "', 'sleep_10 ')

P = Pipeline()
P.backend( Local() )

prelim_steps = P.start_step( a ).merge( b ).next( c )
#prelim_steps.next( q )
post_steps = P.add_step( b, d ).next( e ).merge( q )

P.print_workflow( )

#P.run(['a'])
P.run()
