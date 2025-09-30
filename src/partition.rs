

// top level coordinator that yields column group iterators

use std::{error::Error, io::Read, sync::Arc};

use csv::{Reader, StringRecord};

use crate::range::Range;

pub struct Partition<R: Read> {
    reader: Reader<R>,                      // consumes reader
    ranges: Arc<[Range]>,                   // don't actually need to mutate range values, just used as read + cheap copy
    headers_cached: Option<StringRecord>    // consumes a stringrecord
}

impl<R: Read> Partition<R> {
    pub fn new(mut reader: Reader<R>, num_groups: usize) -> Result<Self, Box<dyn Error>> {
        // create a new partition with equal-sized column groups

        // quick input validation
        if num_groups == 0 {
            return Err(format!("invalid chunk count: {}", num_groups).into());
        }

        let headers: StringRecord = reader.headers()?.clone();
        let total_columns: usize = headers.len();

        if total_columns == 0 {
            return Err("CSv has no columns".into());
        }

        // calculating equal-sized ranges
        let chunk_size: usize = (total_columns + num_groups - 1) / num_groups;
        let mut ranges: Vec<Range> = Vec::new();

        for i in 0..num_groups {
            let lower: usize = i * chunk_size;
            let upper: usize = std::cmp::min(lower + chunk_size, total_columns);

            // only add range if within bounds
            if lower < total_columns {
                ranges.push(Range::new(lower, upper)?);
            } else {
                // column count exceeded
                break
            }
        }

        Ok(Self {
            reader,
            ranges: ranges.into(),
            headers_cached: Some(headers)
        })
    }


}