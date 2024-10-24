use tokio::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use resp::Value;
use anyhow::Result;
use std::collections::HashMap;
mod storage;
use crate::storage::Storage;
mod resp;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    let storage: Arc<Mutex<Storage>> = Arc::new(Mutex::new(Storage::new()));

    loop {
        let (stream, _) = listener.accept().await?;
        println!("Accepted new connection");

        let storage_clone = Arc::clone(&storage);
        tokio::spawn(handle_conn(stream, storage_clone));
    }
}

async fn handle_conn(mut stream: TcpStream, storage: Arc<Mutex<Storage>>) -> Result<()> {
    let mut handler = resp::RespHandler::new(stream);

    loop {
        // Clean up expired keys on each request
        {
            let mut storage_lock = storage.lock().unwrap();
            storage_lock.remove_expired();
        }

        match handler.read_value().await {
            Ok(Some(value)) => {
                let (command, args) = match extract_command(value) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        eprintln!("Error extracting command: {:?}", e);
                        continue;
                    },
                };

                // Lock storage and handle the command
                let response = {
                    let mut storage_lock = storage.lock().unwrap();
                    match unpack_bulk_str(Value::SimpleString(command.clone())) {

                        Ok(cmd_str) => {
                            let cmd_lower = cmd_str.to_lowercase();
                            match cmd_lower.as_str() {
                                "ping" => Value::SimpleString("PONG".to_string()),
                                "echo" => args.get(0).map(|val| Value::BulkString(unpack_bulk_str(val.clone()).unwrap_or_default())).unwrap_or(Value::Null),
                                "set" => {
                                    match (args.get(0), args.get(1), args.get(2), args.get(3)) {
                                        (Some(key), Some(value), Some(arg), Some(expiry)) if unpack_bulk_str(arg.clone()).unwrap_or_default().to_lowercase() == "px" => {
                                            let key_str = unpack_bulk_str(key.clone())?;  
                                            let value_str = unpack_bulk_str(value.clone())?;  
                                            let expires = unpack_bulk_str(expiry.clone())
                                                .unwrap_or_else(|_| "0".to_string()).parse::<usize>().unwrap_or(0);
                                            storage_lock.set(&key_str, &value_str, expires);
                                            Value::SimpleString("OK".to_string())  // Correct type here
                                        },
                                        (Some(key), Some(value), ..) => {
                                            let key_str = unpack_bulk_str(key.clone())?;  
                                            let value_str = unpack_bulk_str(value.clone())?;
                                            storage_lock.set(&key_str, &value_str, 0);  
                                            Value::SimpleString("OK".to_string())  // Correct type here
                                        },
                                        _ => Value::SimpleString("ERROR: SET requires at least a key and value".to_string()),
                                    }
                                },
                                "get" => {
                                    if let Some(key) = args.get(0) {
                                        let key_str = unpack_bulk_str(key.clone())?;
                                        match storage_lock.get(&key_str) {
                                            Some(item) => Value::BulkString(item.value.clone()),  // Wrap in BulkString
                                            None => Value::Null,
                                        }
                                    } else {
                                        Value::SimpleString("ERROR: GET requires one argument".to_string())
                                    }
                                },
                                _ => Value::SimpleString("ERROR: Unknown command".to_string()),
                            }
                        },
                        Err(_) => Value::SimpleString("ERROR: Command is not a valid bulk string".to_string()),
                    }
                };

                if let Err(e) = handler.write_value(response).await {
                    eprintln!("Failed to write response: {:?}", e);
                    break;
                }
            },
            Ok(None) => break,
            Err(e) => {
                eprintln!("Error reading value: {:?}", e);
                break;
            }
        }
    }

    Ok(()) // Return Ok on successful completion
}




async fn handle_command(command: String, args: Vec<Value>, storage: &Arc<Mutex<Storage>>) -> Result<Value> {
    let mut storage_lock = storage.lock().unwrap();
    
    match command.to_lowercase().as_str() {
        "ping" => Ok(Value::SimpleString("PONG".to_string())),
        "echo" => Ok(args.get(0).cloned().unwrap_or(Value::Null)),
        "set" => {
            if let (Some(key), Some(value)) = (args.get(0), args.get(1)) {
                let key_str = unpack_bulk_str(key.clone())?;  
                let value_str = unpack_bulk_str(value.clone())?;
                storage_lock.set(&key_str, &value_str, 0);  // 0 for no expiration
                Ok(Value::SimpleString("OK".to_string()))  // Ensure to return Value
            } else {
                Ok(Value::SimpleString("ERROR: SET requires a key and a value".to_string()))  // Ensure to return Value
            }
        },
        "get" => {
            if let Some(key) = args.get(0) {
                let key_str = unpack_bulk_str(key.clone())?;
                match storage_lock.get(&key_str) {
                    Some(item) => Ok(Value::BulkString(item.value.clone())),  // Return Value
                    None => Ok(Value::Null),  // Return Value
                }
            } else {
                Ok(Value::SimpleString("ERROR: GET requires one argument".to_string()))  // Ensure to return Value
            }
        },
        _ => Ok(Value::SimpleString("ERROR: Unknown command".to_string())),  // Ensure to return Value
    }
}


fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::Array(a) => {
            Ok((
                unpack_bulk_str(a.get(0).cloned().unwrap_or(Value::Null))?,
                a.into_iter().skip(1).collect(),
            ))
        },        
        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}

fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(s) => Ok(s),
        Value::Null => Ok("".to_string()),
        _ => Err(anyhow::anyhow!("Expected bulk string")),
    }
}
