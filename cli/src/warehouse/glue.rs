use super::metadatasource::MetadataSource;
use super::whutils::get_name;
use crate::serialization::{Deserialize, Serialize, YamlWriter};

#[derive(Serialize, Deserialize)]
pub struct Glue {
    pub name: String,
    pub metadata_source: MetadataSource, // Unused. We reference the `.git` dir instead.
}

impl Glue {
    pub fn prompt_add_details() {
        // name
        let name: String = get_name();

        let git_header = "Glue requires no further configuration. We will let boto3 handle the connection configuration.";
        println!("{}", git_header);

        let compiled_config = Glue {
            name,
            metadata_source: MetadataSource::Glue,
        };

        compiled_config
            .register_connection()
            .expect("Failed to register warehouse configuration");

        println!(
            "Added source: {:?} to ~/.whale/config/connections.yaml.",
            compiled_config.name,
        );
    }
}
