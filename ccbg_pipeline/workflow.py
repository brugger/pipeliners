#
# 
# 
##

from __future__ import print_function, unicode_literals
import inspect
import os
import pprint as pp
import time

class Step ( object ):

    name      = None
    function  = None
    step_type = None
    workflow  = None

    def __init__( self, workflow, name, function, step_type=None):
        self.workflow  = workflow
        self.name      = name
        self.function  = function
        self.step_type = step_type

    def __getitem__(self, item):
        
        if ( item.startswith("_")):
            raise AttributeError

        try:
            return getattr(self, item)
        except KeyError:
            raise AttributeError

    def __setitem__(self, item, value):

        if ( item.startswith("_")):
            raise AttributeError
        
        try:
            return setattr(self, item, value)
        except KeyError:
            raise AttributeError

    def __repr__(self):
        return "{name}".format( name=self.name )

    def __str__(self):
        return "{name}".format( name=self.name )


    # Call back functions so we can do nice syntax stuff later on
    def next(self, function, name=None):
        return self.workflow.add_step( self, function, name);

    def merge(self, function, name=None):
        return self.workflow.add_step( self, function, name, step_type='sync');

    def thread_merge(self, function, name=None):
        return self.workflow.add_step( self, function, name, step_type='thread_sync');



class Workflow( object ):

    _start_steps = []
    _steps       = []
    _step_flow   = {}
    _step_index  = {}

    _step_dependencies = {}

    _analysis_order = {}

    def __repr__(self):

        """ printing function """

        res  = "\n\nStep manager dump::\n"
        res += "-----------------------\n\n"

        res += "Start steps:\n"
        res += pp.pformat(self._start_steps )+"\n"

        res += "Steps:\n"
        res += pp.pformat(self._steps )+"\n"


        res += "flow:\n"
        res += pp.pformat(self._step_flow )+"\n"

        res += "index:\n"
        res += pp.pformat(self._step_index )+"\n"

        res += "dependencies:\n"
        res += pp.pformat(self._step_dependencies )+"\n"

        res += "analysis-order:\n"
        res += pp.pformat(self._analysis_order )+"\n"

        return res

    def start_step(self, function, name=None, cluster_params = None):
        """ Set a start step

        It is possible to have multiple start steps in a workflow

        Args:
          Function(str): function that the step is to call
          name (str): logical name of step, default function name
          cluster_param (str): running paramters to pass on to the cluster

        Returns:
          created step (obj)

        """

        # If no name was provided use the name of the function
        # instead. If the function comes from a module add that to the
        # name as well
        if name is None:
            name = self._function_to_name( function )


        start_step = Step( workflow = self,
                           name = name, 
                           function = function,
                           step_type='start')        
        
        self._start_steps.append( start_step )
        self._steps.append( start_step )

        return start_step

    def add_step( self, prev_step, function, name=None, step_type=None):
        """ Generic step adding, this functionality that is wrapped within other simpler functions

        Args:
          prev_step(obj/function): previous step
          Function(str): function that the step is to call
          name (str): logical name of step, default function name
          step_type(str): either None, 'sync' or 'thread_sync'

        Returns:
          Created step

        """

        assert step_type in [None, 'sync', 'thread_sync'], "Illegal step type {}".format(step_type )



        # If no name was provided use the name of the function
        # instead. If the function comes from a module add that to the
        # name as well
        if name is None:
            name = self._function_to_name( function )

        print(" Prev step type: {}".format( type(prev_step) ))


        if (callable(prev_step)):
            prev_step = self._function_to_name( prev_step  )


        if (isinstance(prev_step, basestring)):
            prev_step = self.step_by_name( prev_step )

        print(" Prev step type: {}".format( type(prev_step) ))

        step = Step( workflow = self,
                     name = name, 
                     function = function, 
                     step_type = step_type)

        self._steps.append( step )
        self._link_steps( prev_step, step )

        return step



    def _function_to_name(self, func ):
        
        if ( not callable( func )):
            print( "{}:: parameter is not a function, it is a {}".format( '_function_to_name', type( func )))
            exit()
            
        if ( func.__module__ is None or func.__module__ == "__main__"):
            name = "{}".format( func.__name__)
        else:
            name = "{}.{}".format( func.__module__, func.__name__)
                
        return name

    def _link_steps(self, step1, step2 ):
        """ Links two steps in the workflow 
        
        step1 -> step 2

        args:
          step1 (Step): Step 
          step1 (Step): Step 
        """

        if ( step1 not in self._step_flow):
            self._step_flow[ step1 ] = []

        self._step_flow[ step1 ].append( step2 )


    def start_steps(self):
        """ Returns all start steps for the workflow """

        return self._start_steps[:]


    def next_steps( self, step):
        """ Return the next logical step(s) following the submitted step """

#        print ("Nxt step for: ", type(step), step )

        if (isinstance(step, basestring)):
            step = self._step_index[ step ]

        if step not in self._step_flow:
            return None

#        pp.pprint( self._step_flow[ step ] ) 
        # Return a copy of the list, otherwise it will return a
        # pointer to the list, and as I pop from this list later on it
        # ruins everything
        return self._step_flow[ step ][:]

    def step_by_name( self, name):
        print( "Looking for {}".format(name))
        if name not in self._step_index:
            return None

        return self._steps[ self._step_index[ name ]]


    def steps_by_name( self, names=None):

        pp.pprint( self )
        res = []

        if names is None:
            return res

        for name in names:

            if( callable( name )):
                name = _function_to_name( name )
                print( "Object is: {}".format( name ))


            if (isinstance(name, basestring) and name not in self._step_index):
                print( "Unknown step name: {}".format( name ))               
                exit()
            else:
                print("{}".format( self._step_index[ 'a' ]))

                res.append( self._steps[ self._step_index[ str(name) ]] )

        return res


    def find_analysis_order( self, steps ):

        pp.pprint( steps )

        steps = steps[:]
          
        self._analysis_order[ steps[ 0 ] ] = 1;


        while len(steps):
            step = steps.pop()

            next_steps = self.next_steps( step )
            
            if ( next_steps is None ):
                break


            for next_step in next_steps:
                if (next_step not in self._analysis_order or 
                    self._analysis_order[ next_step ] <= self._analysis_order[ step ] + 1):

                    self._analysis_order[ next_step ] = self._analysis_order[ step ] + 1 

            steps += self.next_steps( step )



    def waiting_for_analysis(self, step, steps_done):

        dependencies = self.get_step_dependencies( step )
             
        if dependencies is None:
            return 0

        pp.pprint( dependencies )

        done = {}
        for step_done in steps_done:
            done[ step_done ] = 1

        for dependency in dependencies:
            if dependency not in done:
                print("{} is waiting for {}".format(step, dependency));
                return 1

        return 0



    def print(self, starts = None ):

        pp.pprint(self)

        pp.pprint( starts )

        if starts is None:
            starts = self._start_steps
        else:
            starts = self.steps_by_name( starts )

            
        pp.pprint( starts )

        for start in starts:
            self.calc_analysis_dependencies( start )

        self.find_analysis_order( starts )

        print( self )


        print("")
        print( "Starting with: {} ".format( starts ))
        print( "--------------------------------------------------\n")

        steps = starts[:]
        
        steps_done = []
        
        while steps:
            step = steps.pop()
            next_steps = self.next_steps( step )

            steps_done.append( step )


            print( "{} queue: {}".format( step.name, next_steps))
  
            if next_steps is not None:
                for next_step in next_steps:

                    if step.step_type is None:
                        print( "{} --> {}\n".format( step.name, next_step.name))
                    else:
                        print( "{} --> {} {}\n".format( step.name, next_step.name, step.step_type))


                    if ( self.waiting_for_analysis(next_step, steps_done)):
                        pass
                    else:
                        steps += next_steps

        print( "--------------------------------------------------\n")


    def validate_flow(self, starts ):

        if starts is None:
            starts = self._start_steps
        else:
            starts = self.steps_by_name( starts )
        
        for start in starts:
            self.calc_analysis_dependencies( start )

        steps = starts[:]
        
        steps_done = []
        
        while steps:
            step = steps.pop()
            next_steps = self.next_steps( step )

            steps_done.append( step )


            print( "{} queue: {}".format( step.name, next_steps))
  
            if next_steps is not None:
                for next_step in next_steps:

                    if ( self.waiting_for_analysis(next_step, steps_done)):
                        pass
                    else:
                        steps += next_steps


        print( "run flow validated")

    def set_step_dependency(self, step, dependency):

        if step not in self._step_dependencies:
            self._step_dependencies[ step ] = []

        self._step_dependencies[ step ].append( dependency )

    def get_step_dependencies( self, step ):
        if step not in self._step_dependencies:
            return None

        return self._step_dependencies[ step ]


    def calc_analysis_dependencies(self,  start_step ):

        next_steps = self.next_steps( start_step )

        for next_step in next_steps:
            self.set_step_dependency(  next_step, start_step )

        while ( next_steps ):
            next_step = next_steps.pop()
            
            new_steps = self.next_steps( next_step );

            if ( new_steps is None or new_steps == []):
                continue

            next_steps += new_steps

            for new_step in new_steps:

                self.set_step_dependency( new_step,  next_step )

                for dependency in self.get_step_dependencies( next_step ):
                    self.set_step_dependency( new_step, dependency )
