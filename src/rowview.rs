use std::sync::Arc;


// represents one complete row with cheap-to-clone owned data
pub struct RowView {
    fields: Vec<Arc<str>>,  // owned, shareable field data
    row_index: usize,   // 0 for header, 1+ for data rows
    group_index: usize,
}

impl RowView {

    // internal constructor, only RowIterator should yield RowViews
    pub(crate) fn new(
        fields: Vec<Arc<str>>,
        row_index: usize,
        group_index: usize
    ) -> Self {
        RowView { fields, row_index , group_index }
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
        // number of fields in this row -> len of fields Vec

        self.fields.len()
    }

    pub fn get_field(&self, local_index: usize) -> Option<&str> {
        // get field at local column index

        self.fields.get(local_index).map(|arc_str| arc_str.as_ref()) 
    }

    pub fn fields(&self) -> impl Iterator<Item = &str> {
        // iterator over field values (&str)

        self.fields.iter().map(AsRef::as_ref)
    }

    pub fn fields_with_indices(&self) -> impl Iterator<Item = (usize, &str)> {
        // fields with local indicies; local enumerate equivalent

        self.fields().enumerate()
    }

    pub fn fields_with_global_indices(&self) -> impl Iterator<Item = (usize, &str)> {
        // fields with global column indicies; global enumerate equivalent

        let base = self.group_index() * self.field_count();
        self.fields_with_indices()
            .map(move |(local_idx, field)| (base + local_idx, field))
    }
}

// cheap cloning thanks to Arc<str>
impl Clone for RowView  {
    fn clone(&self) -> Self {
        Self {
            fields: self.fields.clone(),    // just clones Arc references
            row_index: self.row_index,
            group_index: self.group_index,
        }
    }
}