use std::sync::Arc;

pub struct HeaderRowView {
    column_names: Vec<Arc<str>>,
    group_index: usize,
}

impl HeaderRowView {

    // internal constructor, only ColumnGroup should create HeaderRowView
    pub(crate) fn new(
        column_names: Vec<Arc<str>>,
        group_index: usize
    ) -> Self {
        HeaderRowView { column_names, group_index }
    }

    pub fn group_index(&self) -> usize {
        // get parent group index

        self.group_index
    }

    pub fn column_count(&self) -> usize {
        // number of columns in this header

        self.column_names.len()
    }

    pub fn get_column_name(&self, local_index: usize) -> Option<&str> {
        // get header at local index

        self.column_names.get(local_index).map(|arc_str| arc_str.as_ref())
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        // iterator over header names

        self.column_names.iter().map(AsRef::as_ref)
    }

    pub fn column_names_with_indicies(&self) -> impl Iterator<Item = (usize, &str)> {
        // headers with local indicies; local enumerate equivalent

        self.column_names().enumerate()
    }

    pub fn column_names_with_global_indicies(&self) -> impl Iterator<Item = (usize, &str)> {
        // headers with global column indicies; global enumerate equivalent
        
        let base = self.group_index() * self.column_count();
        self.column_names_with_indicies()
            .map(move |(local_idx, field)| (base + local_idx, field))
    }
}

// cheap cloning thanks to Arc<str>
impl Clone for HeaderRowView {
    fn clone(&self) -> Self {
        Self {
            column_names: self.column_names.clone(),    // just clones Arc references
            group_index: self.group_index,
        }
    }
}