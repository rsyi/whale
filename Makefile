base_dir:=~/.whale
install_dir:=${base_dir}/bin
dependency_binary_dir:=${base_dir}/libexec
python_directory:=databuilder
python3_alias:=python3


.PHONY: rust
rust:
	cargo build --release --manifest-path cli/Cargo.toml

.PHONY: python
python:
	mkdir -p ${dependency_binary_dir}
	${python3_alias} -m venv ${dependency_binary_dir}/env
	. ${dependency_binary_dir}/env/bin/activate && pip3 install -r ${python_directory}/requirements.txt && pip3 install ${python_directory}/.
	cp ${python_directory}/build_script.py ${dependency_binary_dir}
	cp ${python_directory}/run_script.py ${dependency_binary_dir}

.PHONY: install
install: python
	mkdir -p ${install_dir}
	cp ./cli/target/release/whale ${install_dir}

.PHONY: test_python
test_python:
	${python3_alias} -b -m pytest ${python_directory}/tests
	pytest --cov=whalebuilder --cov-report=xml ${python_directory}/tests/
	flake8 ${python_directory}/whalebuilder/. --exit-zero

.PHONY: test
test: test_python
