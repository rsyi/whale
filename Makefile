base_dir:=~/.whale
install_dir:=${base_dir}/bin
dependency_binary_dir:=${base_dir}/libexec

.PHONY: all
all: dist
	cargo build --release --manifest-path cli/Cargo.toml

dist:
	pip3 install pyinstaller
	pyinstaller databuilder/build_script.py --clean --hidden-import='pkg_resources.py2_warn' --additional-hooks-dir='./databuilder/hooks/'

.PHONY: install
install:
	mkdir -p ${dependency_binary_dir}
	mkdir -p ${install_dir}
	chmod +x ${dependency_binary_dir}
	# Rust binary
	cp ./cli/target/release/whale ${dependency_binary_dir}
	# Python "binary"
	cp -r ./dist ${dependency_binary_dir}

.PHONY: test_python
test_python:
	python3 -b -m pytest tests
	pytest --cov=metaframe --cov-report=xml tests/
	flake8 . --exit-zero

.PHONY: test
test: test_python
