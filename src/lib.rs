// lmao i realised that instead of building a really big and complicated system, i could just do this lmao

use std::{error::Error, ops::Range, io::Read};
use csv::{Reader, StringRecord};



/// describes a group's column range
#[derive(Debug)]
pub struct Partition {
    name: String,
    range: Range<usize>,
}

/// main csv partitioner
pub struct CsvPartitioner<R: Read> {
    reader: Reader<R>,
    partitions: Vec<Partition>,
}

impl<R: Read> CsvPartitioner<R> {
    pub fn new(reader: Reader<R>, partitions: Vec<Partition>) -> Self {
        Self {reader, partitions }
    }

    /// single-pass row iterator using a callback
    pub fn for_each_row<F>(&mut self, mut f: F) -> Result<(), csv::Error>
    where 
        F : FnMut(&StringRecord, &[Partition])
    {
        let mut buffer = StringRecord::new();

        while self.reader.read_record(&mut buffer)? {
            f(&buffer, &self.partitions);
        }

        Ok(())
    }
}

/// view of one group in a row (slice of columns)
pub struct GroupView<'a> {
    fields: Vec<&'a str>,
}

impl<'a> GroupView<'a> {
    pub fn get(&self, idx: usize) -> Option<&'a str> {
        self.fields.get(idx).copied()
    }
}

/// split a row into group views, helper
pub fn row_to_groups<'a>(
    row: &'a StringRecord,
    partitions: &'a [Partition]
) -> Vec<GroupView<'a>> {
    partitions
        .iter()
        .map(|p| {
            let slice: Vec<&str> = row
                .iter()
                .skip(p.range.start)
                .take(p.range.len())
                .map(|s| s.trim())
                .collect::<Vec<&str>>();
            GroupView { fields: slice }
        })
        .collect()
}

#[cfg(test)]
mod tests {
//! # CSV Partitioner
//!
//! This library provides utilities for partitioning CSV files into logical groups based on column ranges.
//! It is particularly useful for cases where a CSV file contains multiple topics or categories, each spanning a fixed number of columns.
//!
//! ## Features
//! - Partition CSV columns into named groups (topics).
//! - Iterate over rows and process each group as a logical unit.
//! - Easily map CSV data into custom Rust structs.
//!
//! ## Example
//!
//! Suppose you have a CSV file (`vocab.csv`) containing Japanese vocabulary, where each topic (e.g., verbs, adjectives) occupies a fixed set of columns:
//!
//! ```csv
//! verbs    ,                ,     , adjectives,                     ,         
//! おどろく , to be surprised, 驚く , はやい   , fast/quick            ,  早い
//! しぬ     , to die         , 死ぬ , むずかしい, difficult/troublesome, 難しい
//! ```
//!
//! You can define partitions for each topic and process the CSV as follows:
//!
//! ```rust
//! let partitions = vec![
//!     Partition { name: "verbs".into(), range: 0..3 },
//!     Partition { name: "adjectives".into(), range: 3..6 },
//! ];
//!
//! let mut csvp = CsvPartitioner::new(rdr, partitions);
//! csvp.for_each_row(|row, partitions| {
//!     let groups = row_to_groups(row, partitions);
//!     // process each group...
//! });
//! ```
//!
//! ## Structs
//! - `Partition`: Defines a named range of columns for a topic.
//! - `CsvPartitioner`: Handles reading and partitioning the CSV file.
//!
//! ## Usage
//! 1. Define your partitions as a vector of `Partition`.
//! 2. Create a `CsvPartitioner` with a CSV reader and the partitions.
//! 3. Use `for_each_row` to process each row, mapping column groups to your own structs.
//!
//! ## Testing
//! The library includes tests demonstrating how to partition a CSV file and map its contents to custom structs for further processing.
//!
    use super::*;


    #[derive(Debug, PartialEq)]
    struct Topic {
        name: String,
        words: Vec<Word>
    }

    #[derive(Debug, PartialEq)]
    struct Word {
        japanese: String,
        translation: String,
        kanji: String,
    }

    #[test]
    fn main() -> Result<(), Box<dyn Error>> {
        let file_path = "vocab.csv";
        let rdr = csv::Reader::from_path(file_path)?;

        // define partitions: verbs = columns 0..3, adjectives 3..6
        let partitions = vec![
            Partition { name: "verbs".into(), range: 0..3 },
            Partition { name: "adjectives".into(), range: 3..6 },
        ];

        // initiate topics collective with empty word vectors
        let mut topics: Vec<Topic> = partitions
            .iter()
            .map(|partition| Topic { name: partition.name.clone(), words: Vec::new() })
            .collect();

        let mut csvp = CsvPartitioner::new(rdr, partitions);

        // single pass iteration using closure
        csvp.for_each_row(|row, partitions| {
            let groups = row_to_groups(row, partitions);

            for (i, group) in groups.into_iter().enumerate() {
                let word = Word {
                    japanese: group.get(0).unwrap_or("").to_string(),
                    translation: group.get(1).unwrap_or("").to_string(),
                    kanji: group.get(2).unwrap_or("").to_string(),
                };

                topics[i].words.push(word);
            }
        })?;


        assert_eq!(
            topics[0], 
            Topic {
                name: "verbs".to_string(),
                words: vec![
                    Word { japanese: "おどろく".to_string(), translation: "to be surprised".to_string(), kanji: "驚く".to_string() },
                    Word { japanese: "しぬ".to_string(), translation: "to die".to_string(), kanji: "死ぬ".to_string() },
                ]
            }
        );

        assert_eq!(
            topics[1], 
            Topic {
                name: "adjectives".to_string(),
                words: vec![
                    Word { japanese: "はやい".to_string(), translation: "fast/quick".to_string(), kanji: "早い".to_string() },
                    Word { japanese: "むずかしい".to_string(), translation: "difficult/troublesome".to_string(), kanji: "難しい".to_string() },
                ]
            }
        );

        Ok(())
    }
}