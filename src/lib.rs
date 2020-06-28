/*
Copyright (c) 2020 Todd Stellanova
LICENSE: BSD3 (see LICENSE file)
*/
// #![cfg_attr(not(test), no_std)]

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

    /// Get a read token for subsequent reads
    pub fn subscribe(&self) -> ReadToken {
        ReadToken::default()
    }

    /// Publish a single item
    pub fn publish(&mut self, val: &T) {
        //effectively this reserves space for the write
        let widx = self.write_idx.fetch_add(1, Ordering::SeqCst);
        let clamped_widx = widx % self.buf_len;
        assert!(clamped_widx < self.buf_len);

        // println!("widx {} cwidx: {} ", widx, clamped_widx);
        //copy value into buffer
        self.buf[clamped_widx] = *val;

        // once the queue is full, read_idx should always trail write_idx by a fixed amount
        if self.filled.load(Ordering::SeqCst) {
            let new_ridx = widx.wrapping_add(1).wrapping_sub(self.buf_len);
            // let new_ridx = self
            //     .write_idx
            //     .load(Ordering::SeqCst)
            //     .wrapping_sub(self.buf_len);
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
    // fn read_blaster(&self, token: &mut ReadToken) -> nb::Result<T,()> {
    //     let oldest = self.read_idx.load(Ordering::SeqCst);
    //     let desired = if token.initialized { token.idx.wrapping_add(1) } else { oldest };
    //     let next_write = self.write_idx.load(Ordering::SeqCst);
    //     if desired == next_write {
    //         return Err(nb::Error::WouldBlock);
    //     }
    //
    //     let ridx =
    //         if next_write > oldest {
    //             if desired > oldest {
    //                 desired
    //             } else {
    //                 // assume read token is stale and update it
    //                 oldest
    //             }
    //         } else {
    //             //widx has wrapped, but not ridx
    //             if desired > oldest {
    //                 desired
    //             }  else {
    //                 if desired < next_write {
    //                     desired
    //                 } else {
    //                     // assume read token is stale and update it
    //                     oldest
    //                 }
    //             }
    //         };
    //
    //     token.initialized = true;
    //     token.idx = ridx;
    //     let clamped_idx = ridx % self.buf_len;
    //     assert!(clamped_idx < self.buf_len);
    //     let val = self.buf[clamped_idx];
    //     Ok(val)
    // }


    /// this assumes that the indices haven't wrapped yet
    fn read_before_full(&self, token: &mut ReadToken) -> nb::Result<T, ()> {
        let ridx =
            if !token.initialized {
                self.read_idx.load(Ordering::SeqCst)
            } else {
               token.idx + 1
            };

        let widx = self.write_idx.load(Ordering::SeqCst);
        if !(ridx < widx) {
            return Err(nb::Error::WouldBlock);
        }

        token.initialized = true;
        token.idx = ridx;

        let clamped_idx = ridx % self.buf_len;
        assert!(clamped_idx < self.buf_len);
        let val = self.buf[clamped_idx];
        Ok(val)
    }

    /// Read an item from the queue
    /// Returns either an available msg or WouldBlock
    /// In practice there are only three results:
    /// - Read from the token index + 1
    /// - Read from oldest index
    /// - nb::Error::WouldBlock
    pub fn read_next(&self, token: &mut ReadToken) -> nb::Result<T, ()> {
        // self.read_blaster(token)

        //TODO this currently passes all tests, which probably indicates lack of test coverage
        self.read_before_full(token)
    }

    /// Is the queue empty?
    pub fn empty(&self) -> bool {
        self.write_idx.load(Ordering::SeqCst) == self.read_idx.load(Ordering::SeqCst)
    }

    /// Is this buffer filled to capacity?
    pub fn at_capacity(&self) -> bool {
        self.filled.load(Ordering::SeqCst)
    }

    /// How many total items are available to read?
    pub fn available(&self) -> usize {
        if !self.filled.load(Ordering::SeqCst) {
            let widx =  self.write_idx.load(Ordering::SeqCst);
            let ridx = self.read_idx.load(Ordering::SeqCst);
            let avail = widx.wrapping_sub(ridx);
            assert!(avail <= self.buf_len);
            avail
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
    use generic_array::typenum::{U10,U24};
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
        let mut read_token = q.subscribe();

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
        let mut read_token = q.subscribe();

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
        let mut read_token = q.subscribe();

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

        let mut read_token = q.subscribe();
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
        const BUF_SIZE: u32 = 24;
        const ITEM_PUBLISH_COUNT: u32 =  BUF_SIZE;
        const FIRST_GENERATION: usize = usize::MAX - (ITEM_PUBLISH_COUNT/2) as usize;

        // we initialize a queue with many generations already supposedly published:
        // this allows us to test generation overflow in a reasonable time
        let mut q: SpmsRing<Simple, U24> = SpmsRing::new_with_generation(FIRST_GENERATION);

        // now publish more items, so that generation counter (write index) overflows
        for i in 0..ITEM_PUBLISH_COUNT {
            let s = Simple::new(i, i);
            q.publish(&s);
        }
        assert_eq!(q.available() as u32, BUF_SIZE);

        let mut read_token = q.subscribe();
        let mut pre_val = 0;
        for _ in 0..BUF_SIZE {
            let cur_msg = q.read_next(&mut read_token).unwrap();
            //verify values ascending
            let cur_val = cur_msg.x;
            if 0 != pre_val {
                assert_eq!(cur_val - pre_val, 1);
            }
            pre_val = cur_val;
        }
        // then publish a few more generations
        for i in 0..5 {
            let s = Simple::new(i, i);
            q.publish(&s);
        }
        assert_eq!(q.available() as u32, BUF_SIZE);
    }

    #[test]
    fn multithreaded_writer_readers() {
        const BUF_SIZE: u32 = 24;
        const ITEM_PUBLISH_COUNT: u32 = 3 * BUF_SIZE;
        lazy_static! {
            /// this is how we share a ring between multiple threads
            static ref Q_PTR: AtomicPtr<SpmsRing::<Simple, U24>> = AtomicPtr::default();
        };
        let mut shared_q = SpmsRing::<Simple, U24>::default();
        Q_PTR.store(&mut shared_q, Ordering::Relaxed);

        //used to report back how many items each subscriber read
        let (tx, rx): (Sender<u32>, Receiver<u32>) = mpsc::channel();

        let mut children = Vec::new();
        const NUM_SUBSCRIBERS: u32 = 128;
        for subscriber_id in 0..NUM_SUBSCRIBERS {
            //let inner_q = arc_q.clone();
            let thread_tx = tx.clone();

            let child = thread::Builder::new()
                .name(format!("sid{}",subscriber_id).to_string())
                .spawn(move || {
                let mut read_tok = unsafe {
                    Q_PTR
                        .load(Ordering::SeqCst)
                        .as_ref()
                        .unwrap()
                        .subscribe()
                };
                let mut read_count = 0;
                let mut prev_val = 0;
                let mut read_chain = vec![];
                let my_sub_id: u32 = subscriber_id;
                while read_count < BUF_SIZE {
                    // safe because Q_PTR never changes and
                    // we are accessing this lock-free data structure as read-only
                    let msg_r = unsafe {
                        Q_PTR
                            .load(Ordering::SeqCst)
                            .as_ref()
                            .unwrap()
                            .read_next(&mut read_tok)
                    };
                    match msg_r {
                        Ok(msg) => {
                            read_count += 1;
                            //ensure that we read in order
                            let cur_val = msg.x;
                            read_chain.push((read_tok.idx, cur_val));

                            if cur_val < prev_val {
                                //TODO the ridx is being returned as the value when it should block
                                println!("sid {}: {:?}", my_sub_id, read_chain);
                                break;
                            }

                            prev_val = cur_val;
                        },
                        Err(nb::Error::WouldBlock) => {}
                        _ => break,
                    }
                }

                //report how many items we (eventually) read
                let send_res = thread_tx.send(read_count);
                if send_res.is_err() {
                    println!("couldn't report: {:?}", send_res)
                }

            }).expect("couldn't spawn child");
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

            let filled =
                unsafe { Q_PTR.load(Ordering::SeqCst).as_ref().unwrap().at_capacity() };
            assert!(filled);
        });

        //wait for the writer thread to finish writing
        writer_thread.join().expect("writer thread panicked");

        // find out how many items the subscribers actually read
        for _ in 0..NUM_SUBSCRIBERS {
            let num_read = rx.recv().expect("couldn't receive num_read");
            println!("num_read: {}", num_read);
            // assert!(num_read >= BUF_SIZE);
        }

        for child in children {
            child.join().expect("child panicked");
        }
    }
}
