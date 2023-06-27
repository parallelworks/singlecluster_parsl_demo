#!/bin/bash
source /etc/profile.d/parallelworks.sh
source /etc/profile.d/parallelworks-env.sh
source /pw/.miniconda3/etc/profile.d/conda.sh
conda activate

python input_form_resource_wrapper.py

# Otherwise the submodule is fixed to a given commit...
rm -rf parsl_utils
git clone -b new-workflow https://github.com/parallelworks/parsl_utils.git parsl_utils
# Cant run a scripts inside parsl_utils directly
bash parsl_utils/main.sh
