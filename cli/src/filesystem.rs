use std::path::{Path};
use std::{
    collections::BTreeSet,
    fs::{self, OpenOptions},
    io::{Write, BufWriter},
};


pub fn create_file_structure() {
    let base_path = shellexpand::tilde("~");
    let base_path = Path::new(&*base_path);
    let whale_base_path = base_path.join(".whale");
    let bin_path = whale_base_path.join("bin");
    let config_path = whale_base_path.join("config");
    let libexec_path = whale_base_path.join("libexec");
    let logs_path = whale_base_path.join("logs");
    let metadata_path = whale_base_path.join("metadata");
    let manifests_path = whale_base_path.join("manifests");

    let subpaths = [
        bin_path,
        config_path,
        metadata_path,
        libexec_path,
        logs_path,
        manifests_path
    ];

    for subpath in subpaths.iter() {
        fs::create_dir_all(subpath)
            .expect("Directory was not created succesfully.");
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
        .open(&file_path_string) {
        Ok(file) => {
            let mut writer = BufWriter::new(&file);
            for line in lines {
                //writer.write(format!(b"{}", line));
                writeln!(writer, "{}", line).unwrap();
            }
            writer.flush().expect("Unable to write to file.");
        },
        Err(err) => {panic!("Failed to open file: {}", err)}
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


pub fn get_base_dirname() -> std::string::String {
    shellexpand::tilde("~/.whale").into_owned()
}


pub fn get_build_script_filename() -> std::string::String {
    let path = format!("{}/{}/{}/{}/{}",
                       get_base_dirname(),
                       "libexec",
                       "dist",
                       "build_script",
                       "build_script");
    path
}


pub fn get_config_dirname() -> std::string::String {
    let path = format!("{}/{}",
                       get_base_dirname(),
                       "config");
    path
}


pub fn get_connections_filename() -> std::string::String {
    let path = format!("{}/{}",
                       get_config_dirname(),
                       "connections.yaml");
    path
}


pub fn get_cron_log_filename() -> std::string::String {
    let path = format!("{}/{}",
                       get_logs_dirname(),
                       "cron.log");
    path
}


pub fn get_etl_command() -> std::string::String {
    let path = format!("{}/{}/{}",
                       get_base_dirname(),
                       "bin",
                       "whale");
    if Path::new(&*path).exists() {
        format!("{} etl", path)
    }
    else {
        "wh etl".to_string()
    }
}


pub fn get_logs_dirname() -> std::string::String {
    let path = format!("{}/{}",
                       get_base_dirname(),
                       "logs");
    path
}


pub fn get_manifest_dirname() -> std::string::String {
    let path = format!("{}/{}",
                       get_base_dirname(),
                       "manifests");
    path
}


pub fn get_manifest_filename() -> std::string::String {
    let path = format!("{}/{}",
                       get_manifest_dirname(),
                       "manifest.txt");
    path
}


pub fn get_metadata_dirname() -> std::string::String {
    let path = format!("{}/{}",
                       get_base_dirname(),
                       "metadata");
    path
}


pub fn get_tmp_manifest_filename() -> std::string::String {
    let path = format!("{}/{}",
                       get_manifest_dirname(),
                       "tmp_manifest.txt");
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
    let path = format!("{}/{}",
                       get_logs_dirname(),
                       "recenty_used.txt");
    path
}


pub fn get_usage_filename() -> std::string::String {
    let path = format!("{}/{}",
                       get_logs_dirname(),
                       "usage.csv");
    path
}
