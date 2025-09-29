use std::{io::Read, sync::Arc};

use csv::{Error, Reader, StringRecord};
use crate::{range::Range, rowview::RowView};

pub struct RowIterator<'a, R: Read> {
    reader: &'a mut Reader<R>,
    range: &'a Range,
    group_index: usize,
    current_row_index: usize,
    record_buffer: StringRecord,
    finished: bool
}

impl<'a, R: Read> RowIterator<'a, R>  {

    // internal constructor, only ColumnGroup should yield RowIterators
    pub(crate) fn new(reader: &'a mut Reader<R>, range: &'a Range, group_index: usize) -> RowIterator<'a, R> {
        Self { 
            reader, 
            range, 
            group_index,
            current_row_index: 0, 
            record_buffer: StringRecord::new(), 
            finished: false 
        }
    }
}

impl<'a, R: Read> Iterator for RowIterator<'a, R>  {
    type Item = Result<RowView, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        match self.reader.read_record(&mut self.record_buffer) {
            Ok(true) => {
                // convert csv fields to Arc<str>
                let fields: Vec<Arc<str>> = self.record_buffer
                    .iter()
                    .skip(self.range.lower)
                    .take(self.range.len())
                    .map(|field| Arc::from(field))
                    .collect();

                let row_view = RowView::new(
                    fields, 
                    self.current_row_index, 
                    self.group_index,
                );

                self.current_row_index += 1;
                Some(Ok(row_view))
            },
            Ok(false) => {
                self.finished = true;
                None
            },
            Err(e) => {
                self.finished = true;
                Some(Err(e.into()))
            }
        }
    }    
}