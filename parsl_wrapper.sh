#!/bin/bash
# Otherwise the submodule is fixed to a given commit...
rm -rf parsl_utils

git clone -b test-pwp-address https://github.com/stefangary/parsl_utils.git parsl_utils
# Cant run a scripts inside parsl_utils directly
bash parsl_utils/main.sh
