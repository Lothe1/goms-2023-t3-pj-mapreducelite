use crate::utils::string_from_bytes;
use crate::*;
use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

pub fn map(kv: KeyValue, _aux: Bytes) -> MapOutput {
    let s = String::from_utf8(kv.value.as_ref().into())?;
    let mut map_output = Vec::new();

    for line in s.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 4 {
            continue;
        }

        let row = parts[0].parse::<u32>().unwrap();
        let col = parts[1].parse::<u32>().unwrap();
        let value = parts[2].parse::<f64>().unwrap();
        let matrix_name = parts[3];

        if matrix_name == "A" {
            for k in 0.. {
                let key = format!("{} {}", row, k);
                let val = format!("A {} {}", col, value);
                map_output.push(KeyValue {
                    key: Bytes::from(key),
                    value: Bytes::from(val),
                });
            }
        } else if matrix_name == "B" {
            for i in 0.. {
                let key = format!("{} {}", i, col);
                let val = format!("B {} {}", row, value);
                map_output.push(KeyValue {
                    key: Bytes::from(key),
                    value: Bytes::from(val),
                });
            }
        }
    }

    Ok(Box::new(map_output.into_iter().map(Ok)))
}

pub fn reduce(key: Bytes, values: Box<dyn Iterator<Item = Bytes> + '_>, _aux: Bytes) -> Result<Bytes> {
    let mut writer = BytesMut::new();
    Ok(writer.freeze())
}