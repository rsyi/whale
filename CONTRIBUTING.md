# How to contribute
👋 **Hello future whale committer!** We are so excited that you're interested in contributing to whale.

Whale welcomes all contributions and corrections. Before contributing, please make sure you have read the guidelines below.

## 🌳 Setting up your environment
### Python
The python client is accessed by the rust client through `./pipelines/build_script.py` and `./pipelines/run_script.py`.  For development purposes, therefore, it is generally sufficient to install only the python client by creating a virtual environment, activating it, then running the following command in the repository root:
```
pip install -e .
```
This command installs a symlink to the repository root in site-packages for the current virtual environment. Any tests can then be run within this environment. 

If you'd like to QA some new functionality against a real warehouse connection, you can run `python build_script.py` or `python run_script.py`, within the `./pipelines/` directory. (You'll need to define a connection to a warehouse within `~/.whale/config/connections.yaml` either manually or using `wh init` for this to work.)

### Rust
The rust side of things handles all things related to the local CLI, but also provides an interface to the python client. In general, for pure terminal user interface changes, it is reasonable to simply run the typical `cargo` checks within the `./cli/` directory.

If your changes involve adjustments to the python interface, we recommend manually installing the full stack by running `make && make install`.

## 📘 General instructions
Pull requests are the easiest and preferred method to contribute to any repo at Github.

1. Fork this repository, or update it if you've already forked it before.
2. Create a new branch specific to the issue you are working on with `git checkout -b feature_name`. This action will separate the new feature from any other changes you may do and will make it easier to edit or amend (read more [here](https://guides.github.com/introduction/flow/)).
3. If necessary (use your best judgment), test the feature with a local installation of whale (see [here](https://docs.whale.cx/#all-others) for installation instructions).
4. Add your files on the new branch.
5. Commit locally and push to the new branch with `git push -u origin feature_name`.
6. Go to Github, make a pull request, and ensure all tests pass.
7. Request a review from @rsyi.

## 📖 Working on existent issues
Please check the issues tab to find something to contribute with. Before starting to work on the selected issue make sure to check the comment thread to check that no one else has already started to work on it. If no one is working on it then make a comment that you have the intention to do so to avoid duplicate work.

If someone has already commented about their intentions to fix the issue but nothing has been submitted after 2 or 3 weeks, then it's acceptable to still pick up the issue but make sure to leave a comment.

## 📝 Working on new issues
If you have detected a new issue that needs to be fixed or you would like to add more functionalities please make sure to create an issue.
Before you submit a new issue please make sure that:
- Is a request that has not been done yet. Check closed issues as well.
- Isn't related to anything that provides an illegal service (e.g. piracy, malware, threatening material, spam, etc.).

If you are in doubt, feel free to submit it and we'll have a look.

Thanks for reading through to the end, and welcome to the team! :whale:

*To stay up to date on discussions with other contributors, join the [**#contributors** channel](https://talk-whale.slack.com/archives/C01DHUR40U9) on slack ([**join our community first**](https://join.slack.com/t/talk-whale/shared_invite/zt-i2rayu1u-fljCh7reVstTBOtaH1n1xA) if you haven't already done so).*
