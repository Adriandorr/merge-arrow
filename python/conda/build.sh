#!/bin/bash
unset MACOSX_DEPLOYMENT_TARGET
pushd python
${PYTHON} setup.py install;
popd
