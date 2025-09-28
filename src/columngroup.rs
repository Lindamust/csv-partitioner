use crate::{headerrowview::HeaderRowView, range::Range, rowview::RowView};

use std::io::Read;
use csv::{Error, Reader, StringRecord};

pub struct ColumnGroup<'a, R: Read> {
    pub reader: &'a mut Reader<R>,
    range: &'a Range,
    group_index: usize,
    pub headers_caches: Option<&'a StringRecord>
}

impl<'a, R: Read> ColumnGroup<'a, R> {
    
    // private constructor, only Partition should create ColumnGroups
    pub(crate) fn new(
        reader: &'a mut Reader<R>,
        range: &'a Range,
        group_index: usize,
        headers_caches: Option<&'a StringRecord>
    ) -> Self {
        ColumnGroup { reader, range, group_index, headers_caches }
    }

    pub fn range(&self) -> &Range {
        // gets the column range (but within the struct)

        self.range
    }

    pub fn group_index(&self) -> usize {
        // gets the group's index (obviously, useless ahh boilerplate)

        self.group_index
    }

    pub fn column_count(&self) -> usize {
        // number of columns in this group

        self.range().len()
    }

    pub fn header_row(&'a self) -> Result<HeaderRowView<'a>, Error> {
        // get header row for this group

        Ok(HeaderRowView::new(
            self.headers_caches.unwrap(), 
            self.range(), 
            self.group_index()
        ))
    }

    pub fn rows(&mut self) -> impl Iterator<Item = Result<RowView, Error>> {
        // iterator over data rows

        // hold on this might be impossible within my current framework...
        // ill give it a rest.
    }

}