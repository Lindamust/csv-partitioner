

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
            return Err("CSV has no columns".into());
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

        // shadow ranges into Arc<[Range]>
        let ranges: Arc<[Range]> = Arc::from(ranges);

        Ok(Self {
            reader,
            ranges: ranges,
            headers_cached: Some(headers)
        })
    }

    pub fn with_custom_ranges(mut reader: Reader<R>, ranges: Vec<Range>) -> Result<Self, Box<dyn Error>> {
        // create a partition with custom column ranges, very similiar to new(...) implementation
        
        // quick input validation
        if ranges.is_empty() {
            return Err("Must provide at least one range".into());
        }

        let headers: StringRecord = reader.headers()?.clone();
        let total_columns: usize = headers.len();

        if total_columns == 0 {
            return Err("CSV has no columns".into());
        }

        Self::validate_ranges(&ranges, total_columns)?;

        let ranges: Arc<[Range]> = Arc::from(ranges);

        Ok(Self { 
            reader, 
            ranges,
            headers_cached: Some(headers),
        })
    }

    fn validate_ranges(ranges: &[Range], total_columns: usize) -> Result<(), Box<dyn Error>> {
        // internal validation method

        // check bounds
        for (i, range) in ranges.iter().enumerate() {
            if range.upper > total_columns {
                return Err(format!(
                    "Range {} has upper bound {} which exceeds total columns {}",
                    i, range.upper, total_columns
                ).into());
            }

            if range.lower >= total_columns {
                return Err(format!(
                    "Range {} has lower bound {} which is >= total columns {}",
                    i, range.lower, total_columns
                ).into());
            }
        }

        // check overlaps
        for i in 0..ranges.len() {
            for j in (i + 1)..ranges.len() {
                let range_a: &Range = &ranges[i];
                let range_b: &Range = &ranges[j];

                if range_a.lower < range_b.upper && range_b.lower < range_a.upper {
                    return Err(format!(
                        "Range {} [{}, {}) overlaps with Range {} [{}, {})",
                        i, range_a.lower, range_a.upper,
                        j, range_b.lower, range_b.upper
                    ).into());
                }
            }
        }

        Ok(())
    }

}