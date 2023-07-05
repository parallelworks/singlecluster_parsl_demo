from parsl.app.app import python_app
import parsl_utils


# PARSL APPS:
def hello_executor(greeting):
    """
    This is a function that is wrapped in the main.py script to run on all the executors defined in 
    the Parsl config.
    """
    import socket
    return '{} from {}'.format(greeting, socket.gethostname())