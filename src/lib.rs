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
pub struct SpmsRing<T, N: ArrayLength<T>> {
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

impl<T, N> Default for SpmsRing<T, N>
where
    T: core::default::Default + Copy,
    N: generic_array::ArrayLength<T>,
{
    fn default() -> Self {
        Self::new_with_generation(0)
    }
}

impl<T, N> SpmsRing<T, N>
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
    }

    /// Used to serve reads when the buffer is already full
    /// In practice there are only three results:
    /// - Read from the token index + 1
    /// - Read from oldest index
    /// - nb::Error::WouldBlock
    fn read_after_full(&self, token: &mut ReadToken) -> nb::Result<T, ()> {
        let desired = token.idx.wrapping_add(1);
        let widx = self.write_idx.load(Ordering::SeqCst);
        if desired == widx {
            // widx always leads the available items by one,
            // so the caller is asking for an item that is not yet available
            return Err(nb::Error::WouldBlock);
        }

        let oldest_idx = self.read_idx.load(Ordering::SeqCst);
        let ridx = if token.initialized {
            if widx > desired {
                if desired >= oldest_idx {
                    //the most frequent case (until wrapping)
                    desired
                } else {
                    // we assume that the read token is stale and refresh it
                    oldest_idx
                }
            } else {
                // widx less than desired is only valid if we've wrapped
                let gap = widx.wrapping_sub(desired);
                if gap >= self.buf_len {
                    // assume that the caller hasn't read in a long time
                    println!("wrapped, assume stale");
                    oldest_idx
                } else {
                    println!("wrapped");
                    desired.wrapping_add(1)
                }
            }
        } else {
            oldest_idx
        };

        token.initialized = true;
        token.idx = ridx;
        let val = self.buf[ridx % self.buf_len];
        Ok(val)
    }

    /// this assumes that the indices haven't wrapped yet
    fn read_before_full(&self, token: &mut ReadToken) -> nb::Result<T, ()> {
        //oldest_idx should be zero if we aren't full yet
        let oldest_idx = 0; //self.read_idx.load(Ordering::SeqCst);
        let widx = self.write_idx.load(Ordering::SeqCst);

        let ridx = if !token.initialized {
            oldest_idx
        } else {
            let desired = token.idx.wrapping_add(1);
            desired
        };
        if ridx >= widx {
            return Err(nb::Error::WouldBlock);
        }
        token.initialized = true;
        token.idx = ridx;

        let val = self.buf[ridx % self.buf_len];
        Ok(val)
    }

    /// Read an item from the queue
    /// Returns either an available msg or WouldBlock
    pub fn read_next(&self, token: &mut ReadToken) -> nb::Result<T, ()> {
        if self.filled.load(Ordering::SeqCst) {
            self.read_after_full(token)
        } else {
            self.read_before_full(token)
        }
    }

    /// Is the queue empty?
    pub fn empty(&self) -> bool {
        self.write_idx.load(Ordering::SeqCst) == self.read_idx.load(Ordering::SeqCst)
    }

    /// How many total items are available to read?
    pub fn available(&self) -> usize {
        if !self.filled.load(Ordering::Relaxed) {
            self.write_idx
                .load(Ordering::SeqCst)
                .wrapping_sub(self.read_idx.load(Ordering::SeqCst))
        } else {
            self.buf_len
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::sync::atomic::AtomicPtr;
    use core::time;
    use generic_array::typenum::U10;
    use lazy_static::lazy_static;
    use std::sync::mpsc::{self, Receiver, Sender};
    use std::thread;

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
    fn verify_nonblocking() {
        const WRITE_COUNT: u32 = 5;
        let mut q = SpmsRing::<Simple, U10>::default();
        let mut read_token = ReadToken::default();

        //at first there is no data available
        let mut read_res = q.read_next(&mut read_token);
        //should be no data available yet
        assert!(read_res.is_err());
        assert_eq!( read_res.err().unwrap(), nb::Error::WouldBlock);

        for i in 0..WRITE_COUNT {
            let s = Simple::new(i, i);
            q.publish(&s);
            assert_eq!(q.available(), (i + 1) as usize);
            //read something that should exist
            read_res = q.read_next(&mut read_token);
            assert!(read_res.is_ok());
            read_res = q.read_next(&mut read_token);
            assert_eq!( read_res.err().unwrap(), nb::Error::WouldBlock);
        }
    }

    #[test]
    fn alternating_write_read() {
        const WRITE_COUNT: u32 = 5;
        let mut q = SpmsRing::<Simple, U10>::default();
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
        let mut q = SpmsRing::<Simple, U10>::default();
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
        // Write many more items than the buffer can hold
        const BUF_SIZE: u32 = 10;
        const ITEM_COUNT: u32 = BUF_SIZE * 2;
        let mut q = SpmsRing::<Simple, U10>::default();

        //publish more items than the buffer has space to hold
        for i in 0..ITEM_COUNT {
            let s = Simple::new(i, i);
            q.publish(&s);
        }
        assert_eq!(q.available() as u32, BUF_SIZE);

        let mut read_token = ReadToken::default();
        let mut pre_val = BUF_SIZE - 1;

        for _ in 0..BUF_SIZE {
            let cur_msg = q.read_next(&mut read_token).unwrap();
            //verify values ascending
            let cur_val = cur_msg.x;
            assert_eq!(cur_val - pre_val, 1);
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
        let mut q: SpmsRing<Simple, U10> = SpmsRing::new_with_generation(FIRST_GENERATION);

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

    #[test]
    fn multithreaded_writer_readers() {
        const BUF_SIZE: u32 = 10;
        const ITEM_PUBLISH_COUNT: u32 = 3 * BUF_SIZE;
        lazy_static! {
            /// this is how we share a ring between multiple threads
            static ref Q_PTR: AtomicPtr<SpmsRing::<Simple, U10>> = AtomicPtr::default();
        };
        let mut shared_q = SpmsRing::<Simple, U10>::default();
        Q_PTR.store(&mut shared_q, Ordering::Relaxed);

        //used to report back how many items each subscriber read
        let (tx, rx): (Sender<u32>, Receiver<u32>) = mpsc::channel();

        let mut children = Vec::new();
        const NUM_SUBSCRIBERS: u32 = 128;
        for _ in 0..NUM_SUBSCRIBERS {
            //let inner_q = arc_q.clone();
            let thread_tx = tx.clone();

            let child = thread::spawn(move || {
                let mut read_tok = ReadToken::default();
                let mut read_count = 0;
                while read_count < BUF_SIZE {
                    // safe because Q_PTR never changes and
                    // we are accessing this lock-free data structure as read-only
                    let msg = unsafe {
                        Q_PTR
                            .load(Ordering::Relaxed)
                            .as_ref()
                            .unwrap()
                            .read_next(&mut read_tok)
                    };
                    match msg {
                        Ok(_) => read_count += 1,
                        Err(nb::Error::WouldBlock) => {}
                        _ => break,
                    }
                }

                //report how many items we (eventually) read
                thread_tx
                    .send(read_count)
                    .expect("couldn't send read_count");
            });
            children.push(child);
        }

        //allow the read threads to start maybe
        thread::sleep(time::Duration::from_millis(1));

        //start the writer thread
        let writer_thread = thread::spawn(move || {
            for i in 0..ITEM_PUBLISH_COUNT {
                let s = Simple::new(i, i);
                //safe because only this thread ever uses a mutable SpmsRing,
                //and Q_PTR never changes
                unsafe { Q_PTR.load(Ordering::SeqCst).as_mut().unwrap().publish(&s) }
            }
            let avail =
                unsafe { Q_PTR.load(Ordering::SeqCst).as_ref().unwrap().available() as u32 };
            assert_eq!(avail, BUF_SIZE);
        });

        //wait for the writer thread to finish writing
        writer_thread.join().expect("writer thread panicked");

        // find out how many items the subscribers actually read
        for _ in 0..NUM_SUBSCRIBERS {
            let num_read = rx.recv().expect("couldn't receive num_read");
            assert_eq!(num_read, BUF_SIZE);
        }

        for child in children {
            child.join().expect("child panicked");
        }
    }
}
