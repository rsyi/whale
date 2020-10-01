
pub trait YamlWriter {
    fn register_config(&self) -> Result<(), io::Error>{

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

