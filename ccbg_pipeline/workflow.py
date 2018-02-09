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

    def __init__( self, workflow, function, name, step_type=None, thread_id=0):
        """ Create a step object

        Args:
          workflow (obj): pointer to workflow object
          function (func): pointer to function to run
          name (str): name of step
          step_type (str): one of either: None, 'sync', 'thread_sync', 'start'

        Returns:
          Created object

        Raises:
          Raises an assert error on illigal step_type

        """

        assert step_type in [None, 'sync', 'thread_sync', 'start'], "Illegal step type {}".format(step_type )

        self.workflow  = workflow
        self.function  = function
        self.name      = name
        self.step_type = step_type
        self.thread_id = thread_id

    def __getitem__(self, item):
        """ Generic getter function

        Raises:
          AttributeError is raised if trying to access value starting with _ or unknown value
        """
        
        if ( item.startswith("_")):
            raise AttributeError

        try:
            return getattr(self, item)
        except KeyError:
            raise AttributeError

    def __setitem__(self, item, value):
        """ Generic setter function

        Raises:
          AttributeError is raised if trying to access value starting with _ or unknown value
        """

        if ( item.startswith("_")):
            raise AttributeError
        
        try:
            return setattr(self, item, value)
        except KeyError:
            raise AttributeError

    def __repr__(self):
        """ str version of object, currently name only """
#        return "{} -> {} type:{}".format(self.name, self.function, self.step_type )
        return "{name}".format( name=self.name )

    def __str__(self):
        """ str version of object, currently name only """
        return "{name}".format( name=self.name )

    def __eq__(self, other):
        """ comparison function, needed for the unit testing """
        return( self.function  == other.function and 
                self.name      == other.name     and 
                self.step_type == other.step_type)  
        
        
    # Call back functions so we can do nice syntax stuff later on
    # These are not unit tested here but through the workflow tests.
    # self in the method calls are the current step that we are
    # extending on.
    def next(self, function, name=None):
        return self.workflow.add_step( self, function, name);

    def merge(self, function, name=None):
        return self.workflow.add_step( self, function, name, step_type='sync');

    def thread_merge(self, function, name=None):
        return self.workflow.add_step( self, function, name, step_type='thread_sync');



class Workflow( object ):


    def __init__( self):
        """ Creates a workflow object

        """

        self._start_steps = []
        self._steps       = []
        self._step_flow   = {}
        self._step_index  = {}

        self._step_dependencies = {} # stored as step names
        self._analysis_order = {}  # stored as step names



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

    def start_step(self, function, name=None, thread_id=0):
        """ Set a start step

        It is possible to have multiple start steps in a workflow

        Args:
          Function(str): function that the step is to call
          name (str): logical name of step, default function name

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
                           step_type='start',
                           thread_id=thread_id)        
        
        self._add_step( start_step )
        self._analysis_order[ start_step.name ] = 1;

        return start_step

    def add_step( self, prev_step, function, name=None, step_type=None, thread_id=None):
        """ Generic step adding, this functionality that is wrapped within other simpler functions

        Args:
          prev_step(obj/function): previous step
          Function(str): function that the step is to call
          name (str): logical name of step, default function name
          step_type(str): either None, 'sync' or 'thread_sync', 'start'

        Returns:
          Created step

        """

        assert step_type in [None, 'sync', 'thread_sync', 'start'], "Illegal step type {}".format(step_type )



        # If no name was provided use the name of the function
        # instead. If the function comes from a module add that to the
        # name as well
        if name is None:
            name = self._function_to_name( function )


        if (callable(prev_step)):
            prev_step = self._function_to_name( prev_step  )


        if (isinstance(prev_step, basestring)):
            prev_step = self.step_by_name( prev_step )


        step = Step( workflow = self,
                     name = name, 
                     function = function, 
                     step_type = step_type,
                     thread_id = prev_step.thread_id)

        if thread_id is not None:
            step.thread_id = thread_id

        self._add_step( step )
        self._link_steps( prev_step, step )


        return step



    def _add_step(self,  step ):
        """ Add step information to the workflow
        
        Args:
          step (obj): step object to add

        """

        # append step to the list
        self._steps.append( step )
        # set up the reverse indexm not sure we need this?
        self._step_index[ step.name ] = len(self._steps) - 1

        # if the type of the step is start append it to the start_steps list
        if step.step_type == 'start':
            self._start_steps.append( step )


    def _function_to_name(self, function ):
        """ get the function name for a function

        If the fuction is in a module add the module prefix to the name

        Args:
          function (func): function to get name from
          
        Returns:
          name of function (str)

        """

        
        # check the function is callable, otherwise raise an assert exception
        assert callable( function ), print( "{}:: parameter is not a function, it is a {}".format( '_function_to_name', type( function )))
            
        if ( function.__module__ is None or function.__module__ == "__main__"):
            return "{}".format( function.__name__)
        else:
            return "{}.{}".format( function.__module__, function.__name__)
                

    def _link_steps(self, step1, step2 ):
        """ Links two steps in the workflow 
        
        step1 -> step 2

        args:
          step1 (Step): Step 
          step1 (Step): Step 
        """

        if ( step1 not in self._step_flow):
            self._step_flow[ step1.name ] = []

        # step1 -> step2
        self._step_flow[ step1.name ].append( step2.name )

        # step2 is done after step1
        self._analysis_order[ step2.name ] = self._analysis_order[ step1.name ] + 1 

        #setup some dependencies:
        if step2 not in self._step_dependencies:
            self._step_dependencies[ step2.name ] = []

        if step1.name in self._step_dependencies:
            self._step_dependencies[ step2.name ] = [step1.name]  + self._step_dependencies[ step1.name ]
        else:
            self._step_dependencies[ step2.name ].append( step1.name )

    def start_steps(self):
        """ Returns all start steps for the workflow """
        return self._start_steps[:]

    def steps(self):
        """ Returns all start steps for the workflow """
        return self._steps[:]


    def next_steps( self, step):
        """ Return the next logical step(s) following the submitted step """

        # Nothing depends on this step
        if step.name not in self._step_flow:
            return None

        # Return a copy of the list, otherwise it will return a
        # pointer to the list, and as I pop from this list later on it
        # ruins everything
        res = []
        for next_step in self._step_flow[ step.name ]:
            res.append( self.step_by_name( next_step ))

        return res

    def step_by_name( self, name):
        """ returns a step by its name 
        Args:
          name (str): name of step to find

        Returns:
          step (obj)

        Raises:
          AssertError if a step by that names does not exist
        """

#        print( "Looking for {}".format( name ))
        assert  name in self._step_index, "No step named '{}'".format( name )

        return self._steps[ self._step_index[ name ]]


    def steps_by_name( self, names=None):
        """ Get steps by their name

        if names are empty return an empty list(?)

        Args:
          names(list of str): name of steps

        Returns:
          list of steps (obj)

        Raises:
          raises an assertion error if an unknown name is requested
        """

        res = []

        for name in names:
            assert  name in self._step_index, "No step named {}".format( name )

            res.append( self._steps[ self._step_index[ name ]])

        return res



    def get_step_dependencies( self, step ):
        """ get dependencies for step 

        Args:
         step (obj): step to get dependencies for

        Returns:
          list of steps (obj)
        """

        if step.name not in self._step_dependencies:
            return None

        pp.pprint( self._step_dependencies )

        return sorted(self.steps_by_name( self._step_dependencies[ step.name ]))



    def _waiting_for_analysis(self, step, steps_done):

        dependencies = self.get_step_dependencies( step )
             
        if dependencies is None:
            return False

        done = {}
        for step_done in steps_done:
            done[ step_done ] = 1

        for dependency in dependencies:
            if dependency not in done:
                print("{} is waiting for {}".format(step, dependency));
                return True

        return False

    def print_flow(self, starts = None ):
        """ prints out the workflow

        Args:
          starts (list of str): if provided starts from these start names

        """

        if starts is None:
            starts = self._start_steps
        else:
            starts = self.steps_by_name( starts )

        print("start_steps")
        pp.pprint( self._start_steps )
        print("steps")
        pp.pprint( self._steps )
        print("steps-flow")
        pp.pprint( self._step_flow  )
        print("steps-index")
        pp.pprint( self._step_index )
        print("steps-dep")
        pp.pprint( self._step_dependencies )
        print("steps-order")
        pp.pprint( self._analysis_order )

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


                    if ( self._waiting_for_analysis(next_step, steps_done)):
                        pass
                    else:
                        steps += next_steps

        print( "--------------------------------------------------\n")


