base_dir:=~/.whale
install_dir:=${base_dir}/bin
dependency_binary_dir:=${base_dir}/libexec
python_directory:=pipelines
python3_alias:=python3


.PHONY: rust
rust:
	cargo build --release --manifest-path cli/Cargo.toml

.PHONY: python
python:
	mkdir -p ${dependency_binary_dir}
	${python3_alias} -m venv ${dependency_binary_dir}/env
	. ${dependency_binary_dir}/env/bin/activate && \
	pip3 install -U pip && \
	pip3 install -r ${python_directory}/requirements.txt && \
	pip3 install ${python_directory}/. && \
	python3 -m pip install --upgrade setuptools
	cp ${python_directory}/build_script.py ${dependency_binary_dir}
	cp ${python_directory}/run_script.py ${dependency_binary_dir}

.PHONY: rust
install_rust:
	mkdir -p ${install_dir}
	cp ./cli/target/release/whale ${install_dir}

.PHONY: install
install: python rust

.PHONY: test_python
test_python:
	${python3_alias} -b -m pytest -o log_cli=true ${python_directory}/tests --cov=whale --cov-report=xml
	flake8 ${python_directory}/whale/ --exit-zero

.PHONY: test
test: test_python
