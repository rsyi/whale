base_dir:=~/.metaframe
config_dir:=${base_dir}/config
bin_dir:=${base_dir}/bin
env_dir:=${base_dir}/env
dirs=('${base_dir}', '${config_dir}', '${bin_dir}')

_python:=$(command -v python3.8 || command -v python3.7 || command -v python3.6 || command -v python3 || command -v python)

.PHONY: all
all:
	# Set up all subdirectories.
	rm -rf ${base_dir}/env
	for i in ${dirs}; \
	do \
		mkdir -p $i \
	done

	# Set up fzf.
	$(MAKE) install -C ./fzf/
	cp ./fzf/bin/fzf ~/.metaframe/bin

	# Determine if python is available, and save the alias.
	rm -f ${config_dir}/python_client
	echo $_python >> ${config_dir}/python_client

	# Track currently open virtual env.
	saved_virtual_env=${VIRTUAL_ENV}

	# Make a virtual environment for python scripts
	_python -m venv ${env_dir}
	pip install -r requirements.txt
	pip install .

	# Restore virtual env.
	source ${saved_virtual_env}/bin/activate
