import sys, os, json, time
from random import randint
import argparse

import parsl
from parsl.app.app import python_app
print(parsl.__version__, flush = True)

import parsl_utils
from parsl_utils.config import config, exec_conf
from parsl_utils.data_provider import PWFile

from workflow_apps import hello_python_app, hello_bash_app, hello_executor

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

    # List to store the futures of the hello_executor app
    hello_executor_futs = []

    for exec_label, exec_conf_i in exec_conf.items():
        print('\n\n\nRUNNING APPS IN EXECUTOR {}'.format(exec_label), flush = True)

        if exec_label == 'myexecutor_1':
            print('\n\nApp hello_python_app', flush = True)
            fut_1 = hello_python_app(name = args['name'])
            
            print('\n\nApp hello_bash_app', flush = True)
            fut_2 = hello_bash_app(
                fut_1,
                run_dir = exec_conf[exec_label]['RUN_DIR'],
                inputs = [ 
                    PWFile(
                        url = 'file://usercontainer/{cwd}/hello_srun.in'.format(cwd = os.getcwd()),
                        local_path = '{remote_dir}/inputs/hello_srun.in'.format(remote_dir =  exec_conf[exec_label]['RUN_DIR'])
                    )
                ],
                outputs = [
                    PWFile(
                        url = 'file://usercontainer/{cwd}/outputs/hello_srun-1.out'.format(cwd = os.getcwd()),
                        local_path = '{remote_dir}/hello_srun-1.out'.format(remote_dir =  exec_conf[exec_label]['RUN_DIR'])
                    )
                ],
                stdout = os.path.join(exec_conf[exec_label]['RUN_DIR'], 'std.out'),
                stderr = os.path.join(exec_conf[exec_label]['RUN_DIR'], 'std.err')
            )
            
            if args['test_gsutil'] == 'True':
                # Testing gsutil data provider
                print('\n\nApp hello_bash_app with gsutil data provider', flush = True)
                fut_3 = hello_bash_app(
                    fut_2,
                    run_dir = exec_conf[exec_label]['RUN_DIR'],
                    inputs = [ 
                        PWFile(
                            url = 'gs://demoworkflows/parsl_demo/hello.in',
                            local_path = '{remote_dir}/hello.in'.format(remote_dir =  exec_conf[exec_label]['RUN_DIR'])
                        )
                    ],
                    outputs = [
                        PWFile(
                            url = 'gs://demoworkflows/parsl_demo/hello.out',
                            local_path = '{remote_dir}/hello.out'.format(remote_dir =  exec_conf[exec_label]['RUN_DIR'])
                        )
                    ],
                    stdout = os.path.join(exec_conf[exec_label]['RUN_DIR'], 'std-gs.out'),
                    stderr = os.path.join(exec_conf[exec_label]['RUN_DIR'], 'std-gs.err')
                )
                
                fut_3.result()
            else:
                fut_2.result()

        print('\n\nApp hello_executor', flush = True)
        hello_executor_futs.append(
            python_app(hello_executor, executors=[exec_label]) (os.environ['PW_USER'])
        )
    
    for fut in hello_executor_futs:
        print(fut.result())
