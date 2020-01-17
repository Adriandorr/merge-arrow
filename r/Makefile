.PHONEY: conda

SHELL=/bin/bash

MAKEFILE_PATH := $(realpath $(lastword $(MAKEFILE_LIST)))
CURRENT_DIR := $(dir $(MAKEFILE_PATH) )
CONDA_DEPS=r-arrow
CONDA_ENV_NAME=rmarrow-dev

include $(CURRENT_DIR)../conda-env.mk

conda_dev: $(CONDA_ENV)
	echo $(CONDA_ENV)
	source $(CONDA_ROOT)/bin/activate $(CONDA_ENV_NAME) && \
	R CMD INSTALL --build

conda_package: $(CONDA_ENV)
	echo TODO

test: conda_dev
	source $(CONDA_ROOT)/bin/activate $(CONDA_ENV_NAME) && \
	R CMD INSTALL --install-tests --no-test-load --no-docs --no-help --no-byte-compile .
	source $(CONDA_ROOT)/bin/activate $(CONDA_ENV_NAME) && \
	R --slave -e 'library(testthat); setwd(file.path(.libPaths()[1], "tests")); system.time(test_check("marrow", filter="${file}", reporter=ifelse(nchar("${r}"), "${r}", "summary")))'

all: test conda_package

clean:
	rm -rf "$(CONDA_FILE)" "$(CONDA_ROOT)"
