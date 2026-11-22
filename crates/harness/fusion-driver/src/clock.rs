//! The driver process's wall clock, which the harness moves on.
//!
//! Fusion reads the wall clock (`SystemTime::now()`) to stamp a token's
//! expiry when it receives it (`expires_in` from now) and to decide
//! whether a token is still fresh (more than 300 s left). To age a token
//! within one process without waiting for it in real time, the driver
//! defines the C library's `clock_gettime` in its own binary: the linker
//! resolves the Rust standard library's calls to this definition instead
//! of the C library's. A `CLOCK_REALTIME` read returns the kernel's time
//! plus the offset [`advance`] has added; every other clock (notably
//! `CLOCK_MONOTONIC`, which tokio's timers and Fusion's timeouts read) is
//! the kernel's, unchanged. This is what `libfaketime` does for one clock,
//! without preloading a library into every child process.
//!
//! Only the driver's own process sees the offset: the Token broker and the
//! fake Upstream IdP run on the harness's test clock, which the harness
//! moves as far first.

use std::sync::atomic::{AtomicI64, Ordering};

/// Seconds added to the kernel's `CLOCK_REALTIME`.
static OFFSET_SECONDS: AtomicI64 = AtomicI64::new(0);

/// Move the process's wall clock `seconds` on.
pub fn advance(seconds: u64) {
    let seconds = i64::try_from(seconds).expect("a plausible number of seconds");
    OFFSET_SECONDS.fetch_add(seconds, Ordering::SeqCst);
}

/// `clock_gettime(2)`, through the system call, with the offset added to
/// `CLOCK_REALTIME`.
///
/// # Safety
///
/// As the C library's: `tp` must be valid for writes of one `timespec`.
#[no_mangle]
pub unsafe extern "C" fn clock_gettime(
    clock: libc::clockid_t,
    tp: *mut libc::timespec,
) -> libc::c_int {
    // SAFETY: the caller's contract is the system call's; `syscall` sets
    // `errno` and returns -1 on failure, as `clock_gettime` does.
    let result = unsafe { libc::syscall(libc::SYS_clock_gettime, clock, tp) } as libc::c_int;
    if result == 0 && clock == libc::CLOCK_REALTIME && !tp.is_null() {
        // SAFETY: the system call has just written `*tp`.
        unsafe {
            (*tp).tv_sec += OFFSET_SECONDS.load(Ordering::SeqCst) as libc::time_t;
        }
    }
    result
}
