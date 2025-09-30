use crate::{rowview::HeaderRowView, range::Range, rowiterator::{AllRowIterator, RowIterator}};
use std::{io::Read, sync::Arc};
use csv::{Error, Reader, StringRecord};


/// represents one column range, yields RowView iterators with 'owned' data
pub struct ColumnGroup<'a, R: Read> {
    reader: &'a mut Reader<R>,
    range: &'a Range,                           // just reference to the specific range
    group_index: usize,
    headers_cached: Option<&'a StringRecord>
}

impl<'a, R: Read> ColumnGroup<'a, R> {
    
    /// private constructor, only Partition should create ColumnGroups
    pub(crate) fn new(
        reader: &'a mut Reader<R>,
        range: &'a Range,
        group_index: usize,
        headers_cached: Option<&'a StringRecord>
    ) -> Self {
        ColumnGroup { reader, range, group_index, headers_cached }
    }

    /// gets the column range
    pub fn range(&self) -> &Range {
        &self.range
    }

    
    /// gets the group's index (obviously, useless ahh boilerplate)
    pub fn group_index(&self) -> usize {
        self.group_index
    }

    /// number of columns in this group
    pub fn column_count(&self) -> usize {
        self.range().len()
    }

    /// get header row for this group
    pub fn header_row(&'a self) -> Result<HeaderRowView, Error> {
        if let Some(headers) = self.headers_cached {
            let column_names: Vec<Arc<str>> = headers
                .iter()
                .skip(self.range().lower)
                .take(self.range().len())
                .map(|s| Arc::from(s))
                .collect();
            Ok(HeaderRowView::new(column_names, self.group_index()))
        } else {
            Err(Error::from(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Missing headers",
            )))
        }
    }

    /// iterator over data rows
    pub fn rows(&mut self) -> RowIterator<'_, R> {
        RowIterator::new(
            self.reader, 
            self.range, 
            self.group_index
        )
    }

    /// iterator over header + data rows
    pub fn all_rows(&mut self) -> AllRowIterator<'_, R> {
        AllRowIterator::new(
            self.reader, 
            self.range, 
            self.group_index, 
            self.headers_cached,
        )
    }
}