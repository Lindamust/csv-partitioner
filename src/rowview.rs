use csv::StringRecord;
use super::range::Range;

pub struct RowView<'a> {
    record: &'a StringRecord,
    range: &'a Range,
    row_index: usize, // 0 for header, 1+ for data rows
    group_index: usize,
}

impl<'a> RowView<'a> {

    // private constructor, only ColumnGroup should create RowViews
    pub(crate) fn new(
        record: &'a StringRecord,
        range: &'a Range,
        row_index: usize,
        group_index: usize
    ) -> Self {
        RowView { record, range , row_index , group_index }
    }

    pub fn row_index(&self) -> usize {
        // get row number

        self.row_index
    }

    pub fn group_index(&self) -> usize {
        // get parent group index

        self.group_index
    }

    pub fn is_header(&self) -> bool {
        // check if this is the header row

        if self.row_index() == 0 {
            return  true;
        }
        false
    }

    pub fn field_count(&self) -> usize {
        // number of fields in this row

        self.range.len()
    }

    pub fn get_field(&self, local_index: usize) -> Option<&str> {
        // get field at local column index

        if let Some(global_index) = self.range.local_to_global(local_index) {
            return self.record.get(global_index);
        }
        None
    }

    pub fn fields(&self) -> impl Iterator<Item = &str> {
        // iterator over field values in this row's range

        self.record
            .iter()
            .skip(self.range.lower)
            .take(self.range.len())
    }

    pub fn fields_with_indicies(&self) -> impl Iterator<Item = (usize, &str)> {
        // fields with local indicies; local enumerate equivalent

        self.fields().enumerate()
    }

    pub fn fields_with_global_indicies(&self) -> impl Iterator<Item = (usize, &str)> {
        // fields with global column indicies; global enumerate equivalent

        self.fields_with_indicies()
            .map(move |(local_idx, field)| (self.range.lower + local_idx, field))
    }
}