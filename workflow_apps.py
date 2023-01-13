from parsl.app.app import python_app, bash_app
import parsl_utils


# PARSL APPS:

@parsl_utils.parsl_wrappers.log_app
@python_app(executors=['myexecutor_1'])
def hello_python_app(name = '', stdout='std.out', stderr = 'std.err'):
    """
    Sample python app that runs in myexecutor_1 (defined in the Parsl config)
    """
    import socket
    if not name:
        name = 'python_app_1'
    return 'Hello ' + name + ' from ' + socket.gethostname()

@parsl_utils.parsl_wrappers.log_app
@bash_app(executors=['myexecutor_1'])
def hello_bash_app(fut, run_dir, inputs = [], outputs = [], stdout='std.out', stderr = 'std.err'):
    """
    Sample bash app that runs in myexecutor_1 (defined in the Parsl config). The argument fut is a future
    from a different app and is only used to create a dependency (run this app after the other app).
    """
    return '''
    sleep 30
        cd {run_dir}
        cat {hello_in} > {hello_out}
        date >> {hello_out}
        echo $SLURM_JOB_NODELIST >> {hello_out}
    '''.format(
        run_dir = run_dir,
        hello_in = inputs[0].local_path,
        hello_out = outputs[0].local_path,
    )


def hello_executor(user):
    """
    This is a function that is wrapped in the main.py script to run on all the executors defined in 
    the Parsl config.
    """
    import socket
    return 'Hello {} from {}'.format(user,socket.gethostname())
