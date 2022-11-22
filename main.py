import sys, os, json, time
from random import randint
import argparse

import parsl
print(parsl.__version__, flush = True)
from parsl.app.app import python_app, bash_app

from config import config,exec_conf

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
@parsl_utils.parsl_wrappers.stage_app(exec_conf['myexecutor_1']['HOST_USER'] + '@' + exec_conf['myexecutor_1']['HOST_IP'])
@bash_app(executors=['myexecutor_1'])
def hello_bash_app_1(run_dir, inputs_dict = {}, outputs_dict = {}, stdout='std.out', stderr = 'std.err'):
    return '''
        cd {run_dir}
        cat {hello_in} > {hello_out}
        echo $SLURM_JOB_NODELIST >> {hello_out}
    '''.format(
        run_dir = run_dir,
        hello_in = inputs_dict["test-in-file"]["worker_path"],
        hello_out = outputs_dict["test-out-file"]["worker_path"],
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
    fut_1 = hello_bash_app_1(
        run_dir = exec_conf['myexecutor_1']['RUN_DIR'],
        inputs_dict = {
            "test-in-file": {
                "type": "file",
                "global_path": "pw://{cwd}/hello_srun.in",
                "worker_path": "{remote_dir}/hello_srun.in".format(remote_dir =  exec_conf['myexecutor_1']['RUN_DIR'])
            }
        },
        outputs_dict = {
            "test-out-file": {
                "type": "file",
                "global_path": "pw://{cwd}/hello_srun-1.out",
                "worker_path": "{remote_dir}/hello_srun-1.out".format(remote_dir =  exec_conf['myexecutor_1']['RUN_DIR'])
            }
        },
        stdout = os.path.join(exec_conf['myexecutor_1']['RUN_DIR'], 'std.out'),
        stderr = os.path.join(exec_conf['myexecutor_1']['RUN_DIR'], 'std.err')
    )

    print(fut_1.result())
