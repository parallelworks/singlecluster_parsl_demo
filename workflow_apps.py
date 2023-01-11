from parsl.app.app import python_app, bash_app
import parsl_utils


# PARSL APPS:
@parsl_utils.parsl_wrappers.log_app
@python_app(executors=['myexecutor_1'])
def hello_python_app_1(name = '', stdout='std.out', stderr = 'std.err'):
    import socket
    if not name:
        name = 'python_app_1'
    return 'Hello ' + name + ' from ' + socket.gethostname()

@parsl_utils.parsl_wrappers.log_app
@bash_app(executors=['myexecutor_1'])
def hello_bash_app_1(fut,run_dir, inputs = [], outputs = [], stdout='std.out', stderr = 'std.err'):
    return '''
        cd {run_dir}
        cat {hello_in} > {hello_out}
        date >> {hello_out}
        echo $SLURM_JOB_NODELIST >> {hello_out}
    '''.format(
        run_dir = run_dir,
        hello_in = inputs[0].local_path,
        hello_out = outputs[0].local_path,
    )
