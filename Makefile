base_dir:=~/.metaframe
install_dir:=${base_dir}/bin
dependency_binary_dir:=${base_dir}/libexec

.PHONY: all
all: dist
	$(MAKE) install -C ./fzf/

dist:
	pip3 install -r requirements.txt --user
	pyinstaller build_script.py --clean --hidden-import='pkg_resources.py2_warn' --additional-hooks-dir='./hooks/'

.PHONY: install
install:
	mkdir -p ${dependency_binary_dir}
	mkdir -p ${install_dir}
	chmod +x ${dependency_binary_dir}
	# Fzf.
	cp ./fzf/bin/fzf ${dependency_binary_dir}
	# Databuilder.
	cp -r ./dist ${dependency_binary_dir}
	# Install shell commands.
	cp ./shell/mf ${install_dir}/mf

.PHONY: test_python
test_python:
	python3 -b -m pytest tests
	coverage run-m pytest
	flake8 . --exit-zero

.PHONY: test
test: test_python
