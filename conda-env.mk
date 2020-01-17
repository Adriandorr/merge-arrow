
CONDA_FILENAME=Miniconda3-latest-Linux-x86_64.sh
CONDA_URL="https://repo.anaconda.com/miniconda/$(CONDA_FILENAME)"
CONDA_FILE=$(CURRENT_DIR)/../$(CONDA_FILENAME)

CONDA_ROOT=$(CURRENT_DIR)../../env
CONDA_ENV=${CONDA_ROOT}/envs/$(CONDA_ENV_NAME)

$(CONDA_FILE):
	curl "$(CONDA_URL)" > "$(CONDA_FILE)"
	ls -l "$(CONDA_FILE)"

$(CONDA_ROOT): $(CONDA_FILE)
	bash "$(CONDA_FILE)" -b -p "$(CONDA_ROOT)"
	source $(CONDA_ROOT)/bin/activate && \
	conda install -y conda-build -c conda-forge


$(CONDA_ENV): $(CONDA_ROOT)
	source $(CONDA_ROOT)/bin/activate && \
	conda create --name $(CONDA_ENV_NAME) -y $(CONDA_DEPS) -c conda-forge
