use std::collections::HashMap;
use std::time::{Instant, Duration};

#[derive(Debug)]
pub struct Item {
    pub value: String,
    pub created: Instant,
    pub expires: usize, // Expiry in milliseconds
}

pub struct Storage {
    pub storage: HashMap<String, Item>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            storage: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: &str, value: &str, expires: usize) {
        let item = Item {
            value: value.to_string(),
            created: Instant::now(),
            expires,
        };
        self.storage.insert(key.to_string(), item);
    }

    pub fn get(&mut self, key: &str) -> Option<&Item> {
        self.remove_expired();  // Clean expired items before fetching

        if let Some(item) = self.storage.get(key) {
            let is_expired = item.expires > 0 && item.created.elapsed().as_millis() > item.expires as u128;
            if !is_expired {
                return Some(item);
            }
        }
        None
    }

    pub fn remove_expired(&mut self) {
        let keys_to_remove: Vec<String> = self.storage.iter()
            .filter_map(|(key, item)| {
                if item.expires > 0 && item.created.elapsed().as_millis() > item.expires as u128 {
                    Some(key.clone())
                } else {
                    None
                }
            })
            .collect();

        for key in keys_to_remove {
            self.storage.remove(&key);
        }
    }
}

impl Default for Storage {
    fn default() -> Self {
        Storage::new()
    }
}
