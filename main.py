import sys, os, json, time
from random import randint
import argparse

import parsl
print(parsl.__version__, flush = True)
from parsl.app.app import python_app, bash_app
from parsl.data_provider.files import File


import parsl_utils
from parsl_utils.config import config,exec_conf


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
def hello_bash_app_1(run_dir, inputs = [], outputs = [], stdout='std.out', stderr = 'std.err'):
    return '''
        cd {run_dir}
        cat {hello_in} > {hello_out}
        echo $SLURM_JOB_NODELIST >> {hello_out}
    '''.format(
        run_dir = run_dir,
        hello_in = inputs[0].local_path,
        hello_out = outputs[0].local_path,
    )

def read_args():
    parser=argparse.ArgumentParser()
    parsed, unknown = parser.parse_known_args()
    for arg in unknown:
        if arg.startswith(("-", "--")):
            parser.add_argument(arg)
    pwargs=vars(parser.parse_args())
    print(pwargs)
    return pwargs

if __name__ == '__main__':
    args = read_args()
    job_number = args['job_number']

    print('Loading Parsl Config', flush = True)
    parsl.load(config)

    print('\n\n\nHELLO PYTHON APP:', flush = True)
    fut_1 = hello_python_app_1(name = args['name'])

    print(fut_1.result())

    print('\n\n\nHELLO BASH APP:', flush = True)
    print('\n\nmyexecutor_1:', flush = True)
    input_file = File('hello_srun.in')
    fut_1 = hello_bash_app_1(
        run_dir = exec_conf['myexecutor_1']['RUN_DIR'],
        inputs = [ 
            parsl_utils.staging.PWFile(
                path = '{cwd}/hello_srun.in'.format(cwd = os.getcwd()),
                local_path = '{remote_dir}/hello_srun.in'.format(remote_dir =  exec_conf['myexecutor_1']['RUN_DIR']),
                scheme = 'file'
            )
        ],
        outputs = [
            parsl_utils.staging.PWFile(
                path = '{cwd}/hello_srun-1.out'.format(cwd = os.getcwd()),
                local_path = '{remote_dir}/hello_srun-1.out'.format(remote_dir =  exec_conf['myexecutor_1']['RUN_DIR']),
                scheme = 'file'
            )
        ],
        stdout = os.path.join(exec_conf['myexecutor_1']['RUN_DIR'], 'std.out'),
        stderr = os.path.join(exec_conf['myexecutor_1']['RUN_DIR'], 'std.err')
    )

    print(fut_1.result())
