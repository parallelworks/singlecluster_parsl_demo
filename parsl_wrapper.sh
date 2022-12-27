#!/bin/bash
set -x

# Otherwise the submodule is fixed to a given commit...
git submodule update --remote 
# Cant run a scripts inside parsl_utils directly
bash parsl_utils/main.sh $@
