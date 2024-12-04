use std::fs;
use serde_json::Value;

pub struct ShardingExplicitConfiguration {
    shard_map: std::collections::HashMap<i64, usize>
}

impl ShardingExplicitConfiguration {
    pub fn determine_shard(&self, key: i64) -> usize {
        *self.shard_map.get(&key).expect("Key not found in shard map")
    }
    
    pub fn from_file(sharding_source: &Option<String>) -> ShardingExplicitConfiguration {
        // check, if file sharding_source exists, if not, fail hard
        // read file
        // parse file

        if let Some(file_path) = sharding_source {
            if !fs::metadata(&file_path).is_ok() {
                panic!("Sharding source file does not exist: {}", file_path);
            }

            
            let file_content = fs::read_to_string(file_path).expect("Unable to read file");
            let _json: Value = serde_json::from_str(&file_content).expect("Unable to parse JSON");

            let shard_map: std::collections::HashMap<i64, usize> = serde_json::from_value(_json).expect("Invalid JSON format for shard map");

            ShardingExplicitConfiguration {
                shard_map
            }
        } else {
            panic!("Sharding source file path is not provided");
        }
    }
}