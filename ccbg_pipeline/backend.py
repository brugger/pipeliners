

class Backend(object):

    def name(self):
       """ returns name of the backend
       Args: 
         None

       Returns:
         backend name (str)
       """

       return "base backend class"

    def submit(self, job):
        """ submits/starts a job

        Args:
          Job (obj): job object

        Returns:
          job  (obj)
        """
        
        print("submit_job is not implemented for this backend\n")

    def status(self, job ):
        """ Updates the status of a job
        
        See executer.Job_status for possible statuses

        Args:
          Job (obj): job object


        Returns:
          job status (Job_status )
        """

        print("status is not implemented for this backend\n")

    def kill(self, job):
        """ Kills/terminate/cancels a job

        job.status is set to Job_status.KILLED

        Args:
          Job (obj): job object

        Returns:
          job  (obj)
        """

        print("kill is not implemented for this backend\n")

    def available(self):
        """ See if the backend is available for use by the framework

        Returns:
          boolean
        """
        print("available is not implemented for this backend\n")
        return False

