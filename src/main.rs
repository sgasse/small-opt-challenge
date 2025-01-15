//! Small optimization challenge
//!
//! This code is intentially inefficient in some parts
//! as it is intended as a task.

use std::{hint::black_box, io::IoSlice, thread, time::Duration};

use bytes::{Bytes, BytesMut};
use rand::{distributions::Uniform, prelude::Distribution, seq::SliceRandom, thread_rng, Rng as _};

const MAX_MSG_SIZE: usize = 1400;

fn main() {
    // Generate random payloads upfront to emulate incoming data
    // without causing a visible footprint in the profiling.
    let payloads: Vec<_> = (0..100)
        .into_iter()
        .map(|_| random_payload(10, 500))
        .collect();
    let num_payloads_sampler = Uniform::new(5, 10);

    let mut iovs = Vec::new();

    loop {
        // Choose a random set of payloads to pass to `send_payloads`.
        let num_payloads = num_payloads_sampler.sample(&mut thread_rng());
        let random_payloads = payloads.choose_multiple(&mut thread_rng(), num_payloads);

        send_payloads(random_payloads, &mut iovs);

        // Sleep to throttle the binary a bit.
        thread::sleep(Duration::from_nanos(100));
    }
}

/// Send payloads.
///
/// We want to send payloads grouped together to messages.
/// The total size of a message must not be larger than [MAX_MSG_SIZE].
/// To avoid extra allocations, we are creating `IoSlice` of buffers.
/// The number and size of payloads passed as an iterator is random,
/// so we have to dynamically "grow" a message until it cannot grow further.
fn send_payloads<'a, 'b>(payloads: impl Iterator<Item = &'a Bytes>, iovs: &mut Vec<IoSlice<'b>>)
where
    'a: 'b,
{
    let mut payloads = payloads.peekable();

    while payloads.peek().is_some() {
        iovs.clear();
        let mut msg_size = 0;

        'msg_growing: while let Some(next_payload) = payloads.peek() {
            if msg_size + next_payload.len() < MAX_MSG_SIZE {
                msg_size += next_payload.len();
                iovs.push(IoSlice::new(payloads.next().unwrap()));
            } else {
                break 'msg_growing;
            }
        }

        black_box(send_msg(iovs.as_slice()));
    }
}

// --- Helper methods which you do not need to fiddle with --- //

/// Send messages.
fn send_msg(iovs: &[IoSlice]) {
    black_box({
        let _ = iovs;
    });
}

/// Create a random payload.
fn random_payload(min_size: usize, max_size: usize) -> Bytes {
    let cap = Uniform::new_inclusive(min_size, max_size).sample(&mut thread_rng());
    let mut buf = BytesMut::with_capacity(cap);
    thread_rng().fill(&mut buf[..]);
    buf.freeze()
}
