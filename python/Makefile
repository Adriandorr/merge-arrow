.PHONEY: conda

SHELL=/bin/bash

MAKEFILE_PATH := $(realpath $(lastword $(MAKEFILE_LIST)))
CURRENT_DIR := $(dir $(MAKEFILE_PATH) )

CONDA_FILENAME=Miniconda3-latest-Linux-x86_64.sh
CONDA_URL="https://repo.anaconda.com/miniconda/$(CONDA_FILENAME)"
CONDA_FILE=$(CURRENT_DIR)/$(CONDA_FILENAME)

CONDA_ROOT=$(CURRENT_DIR)../../env
CONDA_ENV_NAME=pymarrow-dev
CONDA_ENV=${CONDA_ROOT}/envs/$(CONDA_ENV_NAME)

echo:
	echo  $(CONDA_FILE)
	echo $(CURRENT_DIR)

$(CONDA_FILE):
	curl "$(CONDA_URL)" > "$(CONDA_FILE)"
	ls -l "$(CONDA_FILE)"

$(CONDA_ROOT): $(CONDA_FILE)
	bash "$(CONDA_FILE)" -b -p "$(CONDA_ROOT)"
	source $(CONDA_ROOT)/bin/activate && \
	conda install -y conda-build -c conda-forge


$(CONDA_ENV): $(CONDA_ROOT)
	source $(CONDA_ROOT)/bin/activate && \
	conda create --name $(CONDA_ENV_NAME) -y pyarrow pybind11 -c conda-forge

conda_dev: $(CONDA_ENV)
	rm -rf $(CURRENT_DIR)/build $(CURRENT_DIR)/pymarrow.egg-info
	source $(CONDA_ROOT)/bin/activate $(CONDA_ENV_NAME) && \
	python setup.py develop

test: conda_dev
	source $(CONDA_ROOT)/bin/activate $(CONDA_ENV_NAME) && \
	python setup.py test

package: $(CONDA_ENV)
	$(CONDA_ROOT)/bin/conda build --no-anaconda-upload --override-channels -c conda-forge $(CURRENT_DIR)conda

all: conda_dev

clean:
	rm -rf "$(CONDA_FILE)" "$(CONDA_ROOT)"
