


use std::{error::Error, io::Read, sync::Arc};

use csv::{Reader, StringRecord};

use crate::{columngroup::ColumnGroup, range::Range};

/// top level coordinator that yields column group iterators
pub struct Partition<R: Read> {
    reader: Reader<R>,                      // consumes reader
    ranges: Arc<[Range]>,                   // don't actually need to mutate range values, just used as read + cheap copy
    headers_cached: Option<StringRecord>    // consumes a stringrecord
}

impl<R: Read> Partition<R> {

    /// create a new partition with equal-sized column groups
    pub fn new(mut reader: Reader<R>, num_groups: usize) -> Result<Self, Box<dyn Error>> {

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

    /// create a partition with custom column ranges, very similiar to new(...) implementation
    pub fn with_custom_ranges(mut reader: Reader<R>, ranges: Vec<Range>) -> Result<Self, Box<dyn Error>> {
        
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

    /// internal validation method
    fn validate_ranges(ranges: &[Range], total_columns: usize) -> Result<(), Box<dyn Error>> {

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

    /// yields the column group for a specific range
    pub fn create_group(&mut self, group_index: usize) -> Option<ColumnGroup<R>> {
        let range: &Range = self.ranges.get(group_index)?;

        Some(ColumnGroup::new(
            &mut self.reader, 
            range, 
            group_index, 
            self.headers_cached.as_ref(),
        ))
    }

    /// iterator over all column groups
    /// iterating like: for group in partition.groups {...} should yield columngroups
    /// idea: columngroup iterator
    /// problem: can't have multiple mut references to self
    /// however, a mut ref to self is needed to construct a columngroup
    /// and a columngroup needs a mut ref to self to construct rowiter
    /// which yield rowviews, which uses the original reader to read the fields
    /// solution ideas:
    /// need to somehow avoid the multiple mut self refs, these refs are mut only
    /// because of the reader, so instead of directly consuming a reader when initialising a partition,
    /// i could instead only initialise a reader when i need to read the field values in rowview
    /// however this is a complete waste of memory and incredibly inefficient...
    /// 
    pub fn groups(&mut self) -> impl Iterator<Item = ColumnGroup<'_, R>> {
        let ranges = Arc::clone(&self.ranges);
        let headers = self.headers_cached.take();
        let mut groups = Vec::with_capacity(ranges.len());
        for (i, range) in ranges.iter().enumerate() {
            let headers_clone = headers.as_ref().cloned();
            groups.push(ColumnGroup::new(
                &mut self.reader,   // <--- multiple mut references made from this
                range,
                i,
                headers_clone.as_ref(),
            ));
        }
        groups.into_iter()
    }
    
    pub fn groups_with_indicies(&mut self) -> impl Iterator<Item = (usize, ColumnGroup<'_, R>)> {
        self.groups().enumerate()
    }

    // now just boiler plate functions

    /// number of column groups
    pub fn group_count(&self) -> usize {
        self.ranges.len()
    }

    /// total of CSV columns
    pub fn total_columns(&self) -> usize {
        self.headers_cached
            .as_ref()
            .map(|h| h.len())
            .unwrap_or(0)
    }

    /// ranges getter and potential dereffer, lifetime tied to &self
    pub fn ranges(&self) -> &[Range] {
        &self.ranges    // can deref Arc<[Range]> to &[Range]
    }

    /// ranges getter but Arc
    pub fn ranges_arc(&self) -> Arc<[Range]> {
        Arc::clone(&self.ranges)    // just increments the reference count
    }

    /// headers getter, get cached header record
    pub fn headers(&mut self) -> Result<&StringRecord, Box<dyn Error>> {
        if let Some(ref headers) = self.headers_cached {
            Ok(headers)
        } else {
            let headers: &StringRecord = self.reader.headers()?;
            self.headers_cached = Some(headers.clone());
            Ok(self.headers_cached.as_ref().unwrap())
        }
    }
    
}