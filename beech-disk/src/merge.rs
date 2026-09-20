use std::{cmp::Ordering, collections::BinaryHeap, io, sync::Arc};

struct Head<T, C> {
    value: T,
    input: usize,
    compare: Arc<C>,
}
impl<T, C: Fn(&T, &T) -> Ordering> PartialEq for Head<T, C> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl<T, C: Fn(&T, &T) -> Ordering> Eq for Head<T, C> {}
impl<T, C: Fn(&T, &T) -> Ordering> PartialOrd for Head<T, C> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<T, C: Fn(&T, &T) -> Ordering> Ord for Head<T, C> {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.compare)(&other.value, &self.value).then_with(|| other.input.cmp(&self.input))
    }
}

/// Lazy heap merge adapted from this repository's original `rust/merge` crate
/// (cd12aaa). Inputs must be sorted by the same comparator. Keeps one head per
/// input and selects each output in O(log(inputs)). An I/O error terminates the
/// entire merge; exhausted inputs are removed and never polled again.
pub struct IterMerger<I, T, C> {
    inputs: Vec<I>,
    heap: BinaryHeap<Head<T, C>>,
    compare: Arc<C>,
    pending: Option<usize>,
    failed: bool,
}
impl<I: Iterator<Item = io::Result<T>>, T, C: Fn(&T, &T) -> Ordering> IterMerger<I, T, C> {
    pub fn new(inputs: Vec<I>, compare: C) -> io::Result<Self> {
        Self::with_compare(inputs, Arc::new(compare))
    }
    pub(crate) fn with_compare(mut inputs: Vec<I>, compare: Arc<C>) -> io::Result<Self> {
        let mut heads = Vec::with_capacity(inputs.len());
        for (input, reader) in inputs.iter_mut().enumerate() {
            if let Some(value) = reader.next().transpose()? {
                heads.push(Head {
                    value,
                    input,
                    compare: compare.clone(),
                });
            }
        }
        Ok(Self {
            inputs,
            heap: BinaryHeap::from(heads),
            compare,
            pending: None,
            failed: false,
        })
    }
}
impl<I: Iterator<Item = io::Result<T>>, T, C: Fn(&T, &T) -> Ordering> Iterator for IterMerger<I, T, C> {
    type Item = io::Result<T>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        // Refill on the next call so a later read error cannot swallow a valid output.
        if let Some(input) = self.pending.take() {
            match self.inputs[input].next().transpose() {
                Ok(Some(value)) => self.heap.push(Head {
                    value,
                    input,
                    compare: self.compare.clone(),
                }),
                Ok(None) => (),
                Err(error) => {
                    self.heap.clear();
                    self.failed = true;
                    return Some(Err(error));
                }
            }
        }
        let head = self.heap.pop()?;
        self.pending = Some(head.input);
        Some(Ok(head.value))
    }
}
impl<I: Iterator<Item = io::Result<T>>, T, C: Fn(&T, &T) -> Ordering> std::iter::FusedIterator
    for IterMerger<I, T, C>
{
}
