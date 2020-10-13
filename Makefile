base_dir:=~/.whale
install_dir:=${base_dir}/bin
dependency_binary_dir:=${base_dir}/libexec
python_directory:=databuilder

.PHONY: all
all: install

# dist:
# 	pip3 install pyinstaller
# 	pyinstaller ${python_directory}/build_script.py --clean --hidden-import='pkg_resources.py2_warn' --additional-hooks-dir='./databuilder/hooks/'

.PHONY: install
install:
	cargo build --release --manifest-path cli/Cargo.toml
	# Rust binary
	cp ./cli/target/release/whale ${install_dir}

	mkdir -p ${install_dir}
	# chmod +x ${dependency_binary_dir}
	mkdir -p ${dependency_binary_dir}
	python3 -m venv ${dependency_binary_dir}/env
	. ${dependency_binary_dir}/env/bin/activate && pip3 install -r ${python_directory}/requirements.txt && pip3 install ${python_directory}/.
	cp ${python_directory}/build_script.py ${dependency_binary_dir}

.PHONY: test_python
test_python:
	python3 -b -m pytest ${python_directory}/tests
	pytest --cov=whalebuilder --cov-report=xml ${python_directory}/tests/
	flake8 ${python_directory}/whalebuilder/. --exit-zero

.PHONY: test
test: test_python
