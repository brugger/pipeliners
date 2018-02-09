

class Backend(object):

    def submit(self, job):
        """ submits/starts a job

        Args:
          Job (obj): job object

        Returns:
          job  (obj)
        """
        
        print("submit_job is not implemented for this Backend\n")

    def status(self, job ):
        """ Updates the status of a job
        
        See executer.Job_status for possible statuses

        Args:
          Job (obj): job object


        Returns:
          job status (Job_status )
        """

        print("status is not implemented for this Backend\n")

    def kill(self, job):
        """ Kills/terminate/cancels a job

        job.status is set to Job_status.KILLED

        Args:
          Job (obj): job object

        Returns:
          job  (obj)
        """

        print("kill is not implemented for this Backend\n")

    def available(self):
        """ See if the backend is available for use by the framework

        Returns:
          boolean
        """
        print("check is not implemented for this Backend\n")

    def stats(self, job):
        """ submits/starts a job

        Args:
          Job (obj): job object

        Returns:
          job  (obj)
        """
        print("stats is not implemented for this Backend\n")

    def runtime(self, job):
        """ returns the time spend executing a task

        Args:
          Job (obj): job object

        Returns:
          runtime (?)
        """
        print("runtime is not implemented for this Backend\n")

    def memory(self, job):
        """ returns the maximum memory used executing a task

        Args:
          Job (obj): job object

        Returns:
          memory usage (?)
        """

        print("memory is not implemented for this Backend\n")

        


