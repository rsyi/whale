pub use serde::{Serialize, Deserialize};
use std::path::Path;
use std::{
    collections::HashMap,
    io::{self, Write},
    fs::{OpenOptions, self},
};
use crate::filesystem;
use yaml_rust::{Yaml, YamlLoader};


pub trait YamlWriter {
    fn register_connection(&self) -> Result<(), io::Error>{

        let connections_filename = filesystem::get_connections_filename();
        let connections_path = Path::new(&*connections_filename);

        // Format yaml docs
        let mut new_docs = self.generate_yaml();

        // Open yaml file
        let mut file = OpenOptions::new().write(true)
                                 .create(true)
                                 .append(true)
                                 .open(connections_path)?;

        new_docs.push_str("\n");
        file.write_all(new_docs.as_bytes())?;
        Ok(())

    }

    fn generate_yaml(&self) -> String;
}

impl<T> YamlWriter for T
    where T: Serialize {

    /// Creates yaml out of struct.
    ///
    /// serde_yaml requires the Serialize trait to be implemented, so rather than implementing
    /// this within YamlWriter, we implement YamlWriter for Serialize then bind the Serialize
    /// trait.
    fn generate_yaml(&self) -> String {
        // Serialize struct as yaml string
        serde_yaml::to_string(&self)
            .unwrap()
    }
}


/// Update config/config.yaml with values from new_values.
///
/// This deserializes the existing config yaml, loops through all of the k/v pairs of the hashmap
/// new_values, and adds them to the deserialized yaml object.
pub fn update_config(new_values: HashMap<&str, &str>) -> Result<(), io::Error> {
    let config_filename = filesystem::get_config_filename();
    let config_path = Path::new(&*config_filename);

    let _file: fs::File = OpenOptions::new().write(true)
        .create(true)
        .open(config_path)?;

    let old_config_string = fs::read_to_string(config_path)?;
    let mut config;
    if let Ok(old_config) = serde_yaml::from_str::<serde_yaml::Value>(&old_config_string) {
        config = old_config;
    }
    else {
        config = serde_yaml::Value::Mapping(
            serde_yaml::Mapping::new()
        );
    }

    for (key, value) in new_values {
        config[key] = value.into();
    }

    let file: fs::File = OpenOptions::new().write(true)
        .create(true)
        .truncate(true)
        .open(config_path)?;
    serde_yaml::to_writer(&file, &config).expect("Failed to write.");

    Ok(())
}


pub fn read_config(key: &str, default: &str) -> Result<String, io::Error> {
    let config_filename = filesystem::get_config_filename();
    let config_path = Path::new(&*config_filename);
    let config_string = fs::read_to_string(config_path)?;

    let docs = YamlLoader::load_from_str(&config_string).unwrap();

    let value;
    if docs.len() > 0 {
        let doc = &docs[0];
        value = match &doc[key] {
            Yaml::String(value) => value.to_string(),
            Yaml::Boolean(value) => value.to_string(),
            Yaml::Integer(value) => value.to_string(),
            _ => default.to_string(),
        };
    }
    else {
        value = default.to_string()
    }

    Ok(value)
}
