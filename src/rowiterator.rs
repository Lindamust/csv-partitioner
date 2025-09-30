use std::{io::Read, sync::Arc};

use csv::{Error, Reader, StringRecord};
use crate::{range::Range, rowview::RowView};

// iterator that reads CSV records and converts it ot RowView with Arc<str> data
pub struct RowIterator<'a, R: Read> {
    reader: &'a mut Reader<R>,
    range: &'a Range,                  // reference to range from arc
    group_index: usize,
    current_row_index: usize,
    record_buffer: StringRecord,
    finished: bool
}

impl<'a, R: Read> RowIterator<'a, R>  {

    // internal constructor, only ColumnGroup should yield RowIterators
    pub(crate) fn new(
        reader: &'a mut Reader<R>, 
        range: &'a Range, 
        group_index: usize,
    ) -> RowIterator<'a, R> {
        Self { 
            reader, 
            range, 
            group_index,
            current_row_index: 0, 
            record_buffer: StringRecord::new(), 
            finished: false, 
        }
    }
}

impl<'a, R: Read> Iterator for RowIterator<'a, R>  {
    type Item = Result<RowView, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // get next row with Arc<str> fields

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

    fn size_hint(&self) -> (usize, Option<usize>) {
        // iterator size hint (unknown)

        match self.finished {
            true => (0, Some(0)),
            false => (0, None),
        }
    }    
}


// general iterator for headers + data rows
pub struct AllRowIterator<'a, R: Read> {
    reader: &'a mut Reader<R>,
    range: &'a Range,
    group_index: usize,
    headers_cached: Option<&'a StringRecord>,
    state: AllRowIteratorState,
    record_buffer: StringRecord,
}

// state machine
enum AllRowIteratorState {
    Header,                 // yield header row first
    Records {               // then yield data rows
        row_index: usize
    },
    Finished,
} 


impl<'a, R: Read> AllRowIterator<'a, R> {
    pub(crate) fn new(
        reader: &'a mut Reader<R>,
        range: &'a Range,
        group_index: usize,
        headers_cached: Option<&'a StringRecord>,
    ) -> Self {
        Self { 
            reader, 
            range,
            group_index,
            headers_cached, 
            state: AllRowIteratorState::Header, 
            record_buffer: StringRecord::new(), 
        }
    }
}

impl<'a, R: Read> Iterator for AllRowIterator<'a, R> {
    type Item = Result<RowView, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // get next row with Arc<str> fields

        match self.state {

            // first iteration yields header row
            AllRowIteratorState::Header => {
                if let Some(headers) = self.headers_cached {
                    
                    // convert header to RowView with Arc<str>
                    let fields: Vec<Arc<str>> = headers
                        .iter()
                        .skip(self.range.lower)
                        .take(self.range.len())
                        .map(|field| Arc::from(field))
                        .collect();

                    let header_row = RowView::new(
                        fields, 
                         0, 
                        self.group_index
                    );

                    // transition to reading data records
                    self.state = AllRowIteratorState::Records { row_index: 1 };

                    Some(Ok(header_row))
                } else {
                    
                    // no headers available, skip to records
                    self.state = AllRowIteratorState::Records { row_index: 1 };
                    self.next()
                }
            },

            // all subsequent iterations yield data rows
            AllRowIteratorState::Records { row_index } => {
                match self.reader.read_record(&mut self.record_buffer) {
                    Ok(true) => {
                        
                        // convert csv record to RowView with Arc<str>
                        let fields: Vec<Arc<str>> = self.record_buffer
                            .iter()
                            .skip(self.range.lower)
                            .take(self.range.len())
                            .map(|field| Arc::from(field))
                            .collect();

                        let row_view = RowView::new(
                            fields, 
                            row_index, 
                            self.group_index,
                        );

                        // increment row index for next iteration
                        self.state = AllRowIteratorState::Records { row_index: row_index + 1 };

                        Some(Ok(row_view))
                    },  

                    Ok(false) => {
                        // end of file

                        self.state = AllRowIteratorState::Finished;
                        None
                    },

                    Err(e) => {
                        // csv read error

                        self.state = AllRowIteratorState::Finished;
                        Some(Err(e.into()))
                    }
                }
            }

            AllRowIteratorState::Finished => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // iterator size hint (unknown)

        match self.state {
            AllRowIteratorState::Header => (1, None),           // At least 1 (header) + unknown data rows
            AllRowIteratorState::Records { .. } => (0, None),   // Unknown remaning rows
            AllRowIteratorState::Finished => (0, Some(0))
        }
    }
}