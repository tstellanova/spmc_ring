/*
Copyright (c) 2020 Todd Stellanova
LICENSE: BSD3 (see LICENSE file)
*/

use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use generic_array::{ArrayLength, GenericArray};

/// This is a ring-buffer queue that is intended to be
/// used for pub-sub applications.  That is, a single
/// producer writes items to the circular queue, and
/// multiple consumers can read items from the queue.
/// When a write to the fixed-size circular buffer overflows,
/// the oldest items in the queue are overwritten.
/// It is up to the readers to keep track of which items they
/// have already read and poll for the next available item.
pub struct SpmcQueue<T, N: ArrayLength<T>> {
    /// The inner item buffer
    buf: GenericArray<T, N>,

    /// cached mask for inner buffer length
    buf_len: usize,

    /// The oldest available item in the queue.
    /// This index grows unbounded until it wraps, and is only masked into
    /// the inner buffer range when we access the array.
    read_idx: AtomicUsize,

    /// The index at which the next item should be written to the buffer
    /// This grows unbounded until it wraps, and is only masked into
    /// the inner buffer range when we access the array.
    write_idx: AtomicUsize,

    /// Have at least buf_len items been written to the queue?
    filled: AtomicBool,

    /// A mutability lock
    mut_lock: AtomicBool,
}

pub struct ReadToken {
    idx: usize,
    initialized: bool,
}

impl Default for ReadToken {
    fn default() -> Self {
        Self {
            idx: 0,
            initialized: false,
        }
    }
}

impl<T, N> Default for SpmcQueue<T, N>
where
    T: core::default::Default + Copy,
    N: generic_array::ArrayLength<T>,
{
    fn default() -> Self {
        Self::new_with_generation(0)
    }
}

impl<T, N> SpmcQueue<T, N>
where
    T: core::default::Default + Copy,
    N: generic_array::ArrayLength<T>,
{
    /// Create a queue prepopulated with some
    /// number of default-value items.
    fn new_with_generation(gen: usize) -> Self {
        let mut inst = Self {
            buf: GenericArray::default(),
            buf_len: 0,
            read_idx: AtomicUsize::new(0),
            write_idx: AtomicUsize::new(gen),
            filled: AtomicBool::new(false),
            mut_lock: AtomicBool::new(false),
        };
        inst.buf_len = inst.buf.len();
        if gen > inst.buf_len {
            inst.filled.store(true, Ordering::SeqCst);
            inst.read_idx.store(
                inst.write_idx.load(Ordering::SeqCst) - inst.buf_len,
                Ordering::SeqCst,
            );
        }
        inst
    }

    /// Publish a single item
    pub fn publish(&mut self, val: &T) {
        self.lock_me();
        //effectively this reserves space for the write
        let widx = self.write_idx.fetch_add(1, Ordering::SeqCst);
        let clamped_widx = widx % self.buf_len;
        // println!("widx {} cwidx: {} ", widx, clamped_widx);
        //copy value into buffer
        self.buf[clamped_widx] = *val;

        // once the queue is full, read_idx should always trail write_idx by a fixed amount
        if self.filled.load(Ordering::SeqCst) {
            let new_ridx = self
                .write_idx
                .load(Ordering::SeqCst)
                .wrapping_sub(self.buf_len);
            //println!("trailing ridx {}", new_ridx);
            self.read_idx.store(new_ridx, Ordering::SeqCst);
        } else if clamped_widx == (self.buf_len - 1) {
            self.filled.store(true, Ordering::SeqCst);
        }

        //thanks to wrapping behavior, oldest value is
        //automatically removed when we push to a full buffer
        self.unlock_me();
    }

    /// Read an item from the queue
    /// Returns either an available msg or WouldBlock
    pub fn read_next(&mut self, token: &mut ReadToken) -> nb::Result<T, ()> {
        let widx = self.write_idx.load(Ordering::SeqCst);
        let oldest_idx = self.read_idx.load(Ordering::SeqCst);
        let total_avail = widx.wrapping_sub(oldest_idx);
        //println!("oldest: {} widx: {} avail: {}", oldest_idx, widx, total_avail);

        if 0 == total_avail {
            //note that this should never happen after the first push to the internal buffer
            return Err(nb::Error::WouldBlock);
        }

        let ridx = if token.initialized {
            // TODO adjust comparison math for wrapping

            if token.idx < oldest_idx {
                oldest_idx
            } else if (token.idx + 1) >= widx {
                return Err(nb::Error::WouldBlock);
            } else {
                token.idx + 1
            }
        } else {
            // println!("token uninit: use oldest as ridx");
            oldest_idx
        };
        token.initialized = true;
        token.idx = ridx;
        let clamped_ridx = ridx % self.buf_len;
        //println!("clamped_ridx: {}", clamped_ridx);

        let val = self.buf[clamped_ridx];
        Ok(val)
    }

    /// Is the queue empty?
    pub fn empty(&self) -> bool {
        self.write_idx.load(Ordering::SeqCst) == self.read_idx.load(Ordering::SeqCst)
    }

    pub fn available(&self) -> usize {
        self.write_idx
            .load(Ordering::SeqCst)
            .wrapping_sub(self.read_idx.load(Ordering::SeqCst))
    }

    pub fn item_at(&self, n: usize) -> T {
        self.buf[n]
    }

    fn lock_me(&mut self) {
        while self
            .mut_lock
            .compare_and_swap(false, true, Ordering::Acquire)
            != false
        {
            while self.mut_lock.load(Ordering::Relaxed) {
                core::sync::atomic::spin_loop_hint();
            }
        }
    }

    fn unlock_me(&mut self) {
        self.mut_lock
            .compare_and_swap(true, false, Ordering::Acquire);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use generic_array::typenum::U10;

    #[derive(Default, Debug, Copy, Clone)]
    struct Simple {
        x: u32,
        y: u32,
    }
    impl Simple {
        fn new(x: u32, y: u32) -> Self {
            Self { x, y }
        }
    }

    #[test]
    fn alternating_write_read() {
        const WRITE_COUNT: u32 = 5;
        let mut q = SpmcQueue::<Simple, U10>::default();
        let mut read_token = ReadToken::default();

        for i in 0..WRITE_COUNT {
            let s = Simple::new(i, i);
            q.publish(&s);
            assert_eq!(q.available(), (i + 1) as usize);
            let cur_msg = q.read_next(&mut read_token).unwrap();
            // println!("i: {} cur_msg: {:?} read_idx: {}",i, cur_msg, read_token.idx );
            assert_eq!(i, cur_msg.x);
        }

        let one_more = q.read_next(&mut read_token);
        assert!(one_more.is_err());
    }

    #[test]
    fn sequential_write_read() {
        const WRITE_COUNT: u32 = 5;
        let mut q = SpmcQueue::<Simple, U10>::default();
        let mut read_token = ReadToken::default();

        for i in 0..WRITE_COUNT {
            let s = Simple::new(i, i);
            q.publish(&s);
            // println!("item {}: in {:?} out {:?}", i, s, q.item_at(i as usize));
        }
        assert_eq!(q.available() as u32, WRITE_COUNT);

        for i in 0..WRITE_COUNT {
            let cur_msg = q.read_next(&mut read_token).unwrap();
            // println!("i: {} cur_msg: {:?} read_idx: {}",i, cur_msg, read_token.idx );
            assert_eq!(i, cur_msg.x);
        }

        let one_more = q.read_next(&mut read_token);
        assert!(one_more.is_err());
    }

    #[test]
    fn buffer_overflow_write_read() {
        const BUF_SIZE: u32 = 10;
        const ITEM_COUNT: u32 = BUF_SIZE * 2;
        let mut q = SpmcQueue::<Simple, U10>::default();

        //publish more items than the buffer has space to hold
        for i in 0..ITEM_COUNT {
            let s = Simple::new(i, i);
            q.publish(&s);
        }
        assert_eq!(q.available() as u32, BUF_SIZE);

        let mut read_token = ReadToken::default();
        let mut pre_val = 9;

        for _ in 0..BUF_SIZE {
            let cur_msg = q.read_next(&mut read_token).unwrap();
            // println!(
            //     "next {}: cur_msg: {:?} pre_val: {}  read_idx: {}",i,
            //     cur_msg, pre_val, read_token.idx
            // );
            //verify values ascending
            let cur_val = cur_msg.x;
            assert_eq!(cur_val.wrapping_sub(pre_val), 1);
            pre_val = cur_val;
        }

        let one_more = q.read_next(&mut read_token);
        assert!(one_more.is_err());
    }

    #[test]
    fn generation_overflow_write_read() {
        const BUF_SIZE: u32 = 10;
        const ITEM_PUBLISH_COUNT: u32 = 5 * BUF_SIZE;
        const FIRST_GENERATION: usize = usize::MAX - 20;

        // we initialize a queue with many generations already supposedly published:
        // this allows us to test generation overflow in a reasonable time
        let mut q: SpmcQueue<Simple, U10> = SpmcQueue::new_with_generation(FIRST_GENERATION);

        // now publish many more items, so that generation counter (write index) overflows
        for i in 0..ITEM_PUBLISH_COUNT {
            let s = Simple::new(i, i);
            q.publish(&s);
        }
        assert_eq!(q.available() as u32, BUF_SIZE);
        // then publish a few more generations
        for i in 0..5 {
            let s = Simple::new(i, i);
            q.publish(&s);
        }
        assert_eq!(q.available() as u32, BUF_SIZE);
    }
}
