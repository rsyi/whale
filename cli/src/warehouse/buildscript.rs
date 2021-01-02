use crate::serialization::{Deserialize, Serialize, YamlWriter};
use crate::utils;
use colored::*;

#[derive(Serialize, Deserialize)]
pub struct BuildScript {
    pub build_script_path: String,
    pub venv_path: String,
    pub python_binary_path: Option<String>,
}

impl BuildScript {
    pub fn prompt_add_details() {
        let header = "If you have a python script that generates (a) a manifest and (b) files in the ./metadata directory, whale allows you to fully integrate this script through association with `wh pull`. You'll need to simply specify a python3 virtual environment, the path the script, and [optionally] a python3 binary (by default, whale will use whatever binary is aliased to `python3`. For a sample build script, see the documentation.";
        println!("{}", header);

        let build_script_message = "Enter the path to your build script.";
        let build_script_path = utils::get_input_with_message(build_script_message);

        let venv_message =
            "Enter the path to the virtual environment you want to use to run this script.";
        let venv_path = utils::get_input_with_message(venv_message);

        let python_binary_path: Option<String>;
        let binary_message = format!(
            "Specify a python3 binary path [Default: {}]",
            "python3".green()
        );
        let binary_path_tmp = utils::get_input_with_message(&binary_message);
        if binary_path_tmp.is_empty() {
            python_binary_path = None;
        } else {
            python_binary_path = Some(binary_path_tmp);
        }

        let compiled_config = BuildScript {
            build_script_path,
            venv_path,
            python_binary_path,
        };

        compiled_config
            .register_connection()
            .expect("Failed to register warehouse configuration");
    }
}
