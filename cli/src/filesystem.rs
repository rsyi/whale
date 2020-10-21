use std::path::Path;
use std::{
    collections::BTreeSet,
    env,
    fs::{self, OpenOptions},
    io::{BufWriter, Write},
};

pub fn create_file_structure() {
    let subdirs = [
        get_bin_dirname(),
        get_config_dirname(),
        get_libexec_dirname(),
        get_logs_dirname(),
        get_metadata_dirname(),
        get_manifest_dirname(),
        get_metrics_dirname(),
    ];

    for subdir in subdirs.iter() {
        fs::create_dir_all(Path::new(&*subdir)).expect("Directory was not created succesfully.");
    }
}

pub fn deduplicate_file(file_path_string: &str) {
    let file_path = Path::new(file_path_string);
    let contents = fs::read_to_string(file_path).expect("Can't read file.");
    let lines: BTreeSet<_> = contents.lines().collect();
    match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&file_path_string)
    {
        Ok(file) => {
            let mut writer = BufWriter::new(&file);
            for line in lines {
                //writer.write(format!(b"{}", line));
                writeln!(writer, "{}", line).unwrap();
            }
            writer.flush().expect("Unable to write to file.");
        }
        Err(err) => panic!("Failed to open file: {}", err),
    }
}

pub fn append_to_file(contents: String, path: &str) {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(path)
        .expect("Failed to open file.");
    writeln!(file, "{}", contents).expect("Failed to write to file.");
}

pub fn get_activate_filename() -> std::string::String {
    let path = format!("{}/{}", get_libexec_dirname(), "env/bin/activate");
    path
}

pub fn get_base_dirname() -> std::string::String {
    shellexpand::tilde("~/.whale").into_owned()
}

pub fn get_bin_dirname() -> std::string::String {
    let path = format!("{}/{}", get_base_dirname(), "bin");
    path
}

pub fn get_build_script_filename() -> std::string::String {
    let path = format!("{}/{}", get_libexec_dirname(), "build_script.py");
    path
}

pub fn get_config_dirname() -> std::string::String {
    let path = format!("{}/{}", get_base_dirname(), "config");
    path
}

pub fn get_config_filename() -> std::string::String {
    let path = format!("{}/{}", get_config_dirname(), "config.yaml");
    path
}

pub fn get_connections_filename() -> std::string::String {
    let path = format!("{}/{}", get_config_dirname(), "connections.yaml");
    path
}

pub fn get_cron_log_filename() -> std::string::String {
    let path = format!("{}/{}", get_logs_dirname(), "cron.log");
    path
}

pub fn get_etl_command() -> std::string::String {
    let path = format!("{}/{}/{}", get_base_dirname(), "bin", "whale");
    if Path::new(&*path).exists() {
        format!("{} pull", path)
    } else {
        "wh pull".to_string()
    }
}

pub fn get_libexec_dirname() -> std::string::String {
    let executable = env::current_exe().unwrap();
    let executable = std::fs::canonicalize(executable).unwrap();
    let path = match executable.parent() {
        Some(name) => name,
        _ => panic!(),
    };
    let path = format!("{}/../{}", path.display(), "libexec");
    path
}

pub fn get_logs_dirname() -> std::string::String {
    let path = format!("{}/{}", get_base_dirname(), "logs");
    path
}

pub fn get_manifest_dirname() -> std::string::String {
    let path = format!("{}/{}", get_base_dirname(), "manifests");
    path
}

pub fn get_manifest_filename() -> std::string::String {
    let path = format!("{}/{}", get_manifest_dirname(), "manifest.txt");
    path
}

pub fn get_metadata_dirname() -> std::string::String {
    let path = format!("{}/{}", get_base_dirname(), "metadata");
    path
}

pub fn get_metrics_dirname() -> std::string::String {
    let path = format!("{}/{}", get_base_dirname(), "metrics");
    path
}

pub fn get_tmp_manifest_filename() -> std::string::String {
    let path = format!("{}/{}", get_manifest_dirname(), "tmp_manifest.txt");
    path
}

pub fn get_open_command() -> std::string::String {
    let editor = match std::env::var("EDITOR") {
        Ok(val) => val,
        Err(_e) => "open".to_string(),
    };
    editor
}

pub fn get_recently_used_filename() -> std::string::String {
    let path = format!("{}/{}", get_logs_dirname(), "recently_used.txt");
    path
}

pub fn get_run_script_filename() -> std::string::String {
    let path = format!("{}/{}", get_libexec_dirname(), "run_script.py");
    path
}

pub fn get_usage_filename() -> std::string::String {
    let path = format!("{}/{}", get_logs_dirname(), "usage.csv");
    path
}
