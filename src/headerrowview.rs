use csv::StringRecord;
use super::range::Range;

pub struct HeaderRowView<'a> {
    pub record: &'a StringRecord,
    pub range: &'a Range,
    pub group_index: usize,
}

impl<'a> HeaderRowView<'a> {

    // private constructor, only ColumnGroup should create HeaderRowView
    pub(crate) fn new(
        record: &'a StringRecord,
        range: &'a Range,
        group_index: usize
    ) -> Self {
        HeaderRowView { record, range, group_index }
    }

    pub fn group_index(&self) -> usize {
        // get parent group index

        self.group_index
    }

    pub fn column_count(&self) -> usize {
        // number of columns in this header

        self.range.len()
    }

    pub fn get_column_name(&self, local_index: usize) -> Option<&str> {
        // get header at local index

        if let Some(global_index) = self.range.local_to_global(local_index) {
            return self.record.get(global_index);
        }
        None
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        // iterator over header names

        self.record
            .iter()
            .skip(self.range.lower)
            .take(self.column_count())
    }

    pub fn column_names_with_indicies(&self) -> impl Iterator<Item = (usize, &str)> {
        // headers with local indicies; local enumerate equivalent

        self.column_names().enumerate()
    }

    pub fn column_names_with_global_indicies(&self) -> impl Iterator<Item = (usize, &str)> {
        // headers with global column indicies; global enumerate equivalent

        self.column_names_with_indicies()
            .map(move |(local_idx, field )| (self.range.lower + local_idx, field))
    }
}