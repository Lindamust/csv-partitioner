pub struct Range {
    pub lower: usize,   // inclusive start column
    pub upper: usize,   // exclusive end column
}

impl Range {
    pub fn new(lower: usize, upper: usize) -> Result<Range, String> {
        // validated range creation

        if lower >= upper || upper <= lower {
            return Err(format!("Lower bound: {} must be strictly less than Upper bound: {}", lower, upper));
        }

        Ok(Range { lower, upper })
    }

    pub fn len(&self) -> usize {
        // number of columns in a range

        return self.upper - self.lower;
    }

    pub fn contains(&self, global_index: usize) -> bool {
        // check if global column is in local range

        (self.lower..self.upper).contains(&global_index)
    }

    pub fn local_to_global(&self, local_index: usize) -> Option<usize> {
        // convert local index to global column
        
        (self.lower..self.upper).nth(local_index)
    }

    pub fn global_to_local(&self, global_index: usize) -> Option<usize> {
        // convert global column to local index

        if !self.contains(global_index) {
            return None;
        }

        Some(global_index - self.lower)
    }
}