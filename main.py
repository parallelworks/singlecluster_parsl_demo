import os, json

import parsl
from parsl.app.app import python_app
print(parsl.__version__, flush = True)

import parsl_utils
from parsl_utils.config import config, resource_labels, inputs_dict

from workflow_apps import hello_executor


if __name__ == '__main__':
    print('Loading Parsl Config', flush = True)
    parsl.load(config)

    # List to store the futures of the hello_executor app
    hello_executor_futs = []

    for label in resource_labels:
        print(f'\nRunning job in executor {label}')
        hello_executor_futs.append(
            python_app(hello_executor, executors=[label])(inputs_dict['greeting'])
        )
    
    for li,label in enumerate(resource_labels):
        print(f'\nWaiting for job in executor {label}')
        print(hello_executor_futs[li].result())