/*
Copyright (c) 2020 Todd Stellanova
LICENSE: BSD3 (see LICENSE file)
*/
#![cfg_attr(not(test), no_std)]

use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use generic_array::{ArrayLength, GenericArray};

/// This is a ring buffer that is intended to be
/// used for pub-sub applications.  That is, a single
/// producer writes items to the circular buffer, and
/// multiple subscribers can read items from the buffer.
/// When a write to the fixed-size circular buffer overflows,
/// the oldest items in the buffer are overwritten.
/// Subscribers use ReadTokens to track which items they have
/// already read, and use nonblocking poll to read the next available item.
///
/// Note that the fixed buffer length must always be a power of two.
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
    T: core::default::Default + Copy + core::fmt::Debug,
    N: ArrayLength<T>,
{
    /// This is how the buffer should be created
    fn default() -> Self {
        Self::new_with_generation(0)
    }
}

impl<T, N> SpmsRing<T, N>
where
    T: core::default::Default + Copy + core::fmt::Debug,
    N: ArrayLength<T>,
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
        //TODO obtain buf_len in some const way
        inst.buf_len = inst.buf.len();
        assert!(inst.buf_len % 2 == 0, "buffer capacity must be a power of two");
        // println!("buf_len: {}", inst.buf_len);
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

    /// clamp an index into the size allowed by our inner buffer
    fn clamp_index(&self, index: usize) -> usize {
        index & (self.buf_len - 1)
    }

    /// Publish a single item
    pub fn publish(&mut self, val: &T) {
        //reserve space for the write
        let widx = self.write_idx.fetch_add(1, Ordering::SeqCst);
        let clamped_widx = self.clamp_index(widx);
        self.buf[clamped_widx] = *val;

        // once the queue is full, read_idx should always trail write_idx by a fixed amount
        if self.filled.load(Ordering::SeqCst) {
            let new_ridx = widx.wrapping_sub(self.buf_len - 1);
            self.read_idx.store(new_ridx, Ordering::SeqCst);
            // println!("pub {}->{} = {:?}", widx, clamped_widx, self.buf[clamped_widx]);
        } else if clamped_widx == (self.buf_len - 1) {
            self.filled.store(true, Ordering::SeqCst);
        }

        //thanks to wrapping behavior on write_idx, oldest value is
        //automatically overwritten when we push to a full buffer
    }


    /// Read an item from the buffer.
    /// Returns either an available item or WouldBlock
    /// There are only three possible actions:
    /// - Read from the next available index after index in read token
    /// - Read from oldest index (the read token might be stale)
    /// - `nb::Error::WouldBlock`
    fn read_forgiving(&self, token: &mut ReadToken) -> nb::Result<T, ()> {
        let oldest = self.read_idx.load(Ordering::SeqCst);
        let desired =
            if !token.initialized {
                oldest
            } else {
                token.idx.wrapping_add(1)
            };

        let next_write = self.write_idx.load(Ordering::SeqCst);
        if desired == next_write {
            return Err(nb::Error::WouldBlock);
        }

        let wgap =
            if next_write > desired { next_write - desired }
            else { next_write.wrapping_sub(desired) };

        let ridx =
            if wgap < self.buf_len { desired }
            else { oldest };

            // if next_write > desired {
            //     // println!("oldest {} desired {} nextw {}",oldest, desired, next_write);
            //     // desired may have wrapped before oldest
            //     if (next_write - desired) < self.buf_len  { desired }
            //     else { oldest }
            // }
            // else {
            //     //widx may have wrapped
            //     let wgap = next_write.wrapping_sub(desired);
            //     // println!("oldest {} desired {} nextw {} wgap {}", oldest, desired, next_write, wgap);
            //     if wgap < self.buf_len { desired }
            //     else { oldest }
            // };


        token.initialized = true;
        token.idx = ridx;

        let clamped_idx = self.clamp_index(ridx);
        let val = self.buf[clamped_idx];
        Ok(val)
    }


    pub fn read_next(&self, token: &mut ReadToken) -> nb::Result<T, ()> {
        self.read_forgiving(token)
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
    use generic_array::typenum::{U8,U24};
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
        let mut q = SpmsRing::<Simple, U8>::default();
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
        let mut q = SpmsRing::<Simple, U8>::default();
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
        let mut q = SpmsRing::<Simple, U8>::default();
        let mut read_token = q.subscribe();

        for i in 0..WRITE_COUNT {
            let s = Simple::new(i, i);
            q.publish(&s);
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
        const BUF_SIZE: u32 = 8;
        const ITEM_COUNT: u32 = BUF_SIZE * 2;
        let mut q = SpmsRing::<Simple, U8>::default();

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
        const BUF_SIZE: u32 = 8;
        const ITEM_PUBLISH_COUNT: u32 =  BUF_SIZE;
        const FIRST_GENERATION: usize = usize::MAX - (ITEM_PUBLISH_COUNT/2) as usize;
        // println!("maxusize: {} first_gen: {}", usize::MAX, FIRST_GENERATION);

        // we initialize a queue with many generations already supposedly published:
        // this allows us to test generation overflow in a reasonable time
        let mut q: SpmsRing<Simple, U8> = SpmsRing::new_with_generation(FIRST_GENERATION);

        // now publish more items, so that generation counter (write index) overflows
        for i in 0..ITEM_PUBLISH_COUNT {
            let s = Simple::new(i, i);
            q.publish(&s);
        }
        assert_eq!(q.available() as u32, BUF_SIZE);

        let mut read_token = q.subscribe();
        let mut pre_val = 0;
        for read_count in 0..BUF_SIZE {
            let cur_msg = q.read_next(&mut read_token).unwrap();
            //verify values ascending
            let cur_val = cur_msg.x;
            // println!("{} cur {} pre {}", read_count, cur_val, pre_val);

            if 0 != pre_val {
                assert_eq!(cur_val.wrapping_sub(pre_val), 1);
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
