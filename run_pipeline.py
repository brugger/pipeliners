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

def a(prev_step):
    print "generating some jobs from: A"
    P.submit_job('sleep 10; echo "hello 10 "', 'sleep_10 ')
    P.submit_job('sleep 20; echo "hello 20 "', 'sleep_20 ')


def b():
    print "B"


def c():
    print "C"

def d():
    print "C"


def e():
    print "E"

def q():
    print "Q"

P = Pipeline()
P.backend( Local() )

prelim_steps = P.start_step( a ).merge( b ).next( c ).next( q )
#prelim_steps.next( q )
post_steps = P.add_step( b, d ).next( e )

P.print_workflow( )

#P.run(['a'])
P.run()
