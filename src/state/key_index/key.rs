//! Store one current key in one heap allocation.
//!
//! A `Key` is a thin, reference-counted pointer to a header and the key bytes. The header holds
//! the reference count, the key length, and the `KeyMeta` of the key. The hash set of
//! `KeyIndex` and the ordered set of `KeyOrder` share the allocation, so each of their slots is
//! one pointer. Only `KeyIndex` reads or writes the metadata. Other holders read the key bytes.

use std::alloc::{alloc, dealloc, handle_alloc_error, Layout};
use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::hash::{Hash, Hasher};
use std::ptr::NonNull;
use std::sync::atomic::{fence, AtomicU32, Ordering};

use anyhow::{Context, Result};

use super::KeyMeta;

#[repr(C)]
struct Header {
    refs: AtomicU32,
    len: u32,
    meta: UnsafeCell<KeyMeta>,
}

/// A shared current key with its metadata.
pub(crate) struct Key(NonNull<Header>);

// The key bytes never change after `new`, and the count is atomic. The metadata has the access
// rules of `meta` and `set_meta`: only the owner of `KeyIndex` uses it, through `&` for reads and
// `&mut` for writes. Holders on other threads only read the key bytes.
unsafe impl Send for Key {}
unsafe impl Sync for Key {}

impl Key {
    fn layout(len: usize) -> Layout {
        let bytes = Layout::array::<u8>(len).expect("key layout");
        let (layout, offset) = Layout::new::<Header>().extend(bytes).expect("key layout");
        debug_assert_eq!(offset, size_of::<Header>());
        layout.pad_to_align()
    }

    /// Copy a key into a new allocation with its metadata.
    pub(crate) fn new(key: &[u8], meta: KeyMeta) -> Result<Self> {
        let len = u32::try_from(key.len()).context("key is longer than 4 GiB")?;
        let layout = Self::layout(key.len());
        // The layout has a non-zero size because it contains the header.
        let ptr = unsafe { alloc(layout) }.cast::<Header>();
        let Some(ptr) = NonNull::new(ptr) else {
            handle_alloc_error(layout)
        };
        // The allocation fits the header and `len` bytes after it, and nothing else uses it yet.
        unsafe {
            ptr.as_ptr().write(Header {
                refs: AtomicU32::new(1),
                len,
                meta: UnsafeCell::new(meta),
            });
            std::ptr::copy_nonoverlapping(key.as_ptr(), Self::data(ptr), key.len());
        }
        Ok(Self(ptr))
    }

    fn data(ptr: NonNull<Header>) -> *mut u8 {
        // The bytes start right after the header, inside the same allocation.
        unsafe { ptr.as_ptr().cast::<u8>().add(size_of::<Header>()) }
    }

    fn header(&self) -> &Header {
        // The allocation lives while this handle holds a reference.
        unsafe { self.0.as_ref() }
    }

    /// Read the metadata.
    ///
    /// # Safety
    ///
    /// No `set_meta` call on any handle of this key may run while the returned reference lives.
    pub(super) unsafe fn meta(&self) -> &KeyMeta {
        unsafe { &*self.header().meta.get() }
    }

    /// Replace the metadata.
    ///
    /// # Safety
    ///
    /// No other reference to the metadata of this key may exist, and no other thread may read it.
    pub(super) unsafe fn set_meta(&self, meta: KeyMeta) {
        unsafe { *self.header().meta.get() = meta };
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        let len = self.header().len as usize;
        // `new` copied `len` bytes after the header, and they never change.
        unsafe { std::slice::from_raw_parts(Self::data(self.0), len) }
    }
}

impl Borrow<[u8]> for Key {
    fn borrow(&self) -> &[u8] {
        self.as_ref()
    }
}

impl Clone for Key {
    fn clone(&self) -> Self {
        // Like `Arc`, a new reference needs no ordering with other accesses.
        let old = self.header().refs.fetch_add(1, Ordering::Relaxed);
        if old >= u32::MAX / 2 {
            std::process::abort();
        }
        Self(self.0)
    }
}

impl Drop for Key {
    fn drop(&mut self) {
        // Like `Arc`, the last owner synchronizes with the drops of the other owners.
        if self.header().refs.fetch_sub(1, Ordering::Release) != 1 {
            return;
        }
        fence(Ordering::Acquire);
        let layout = Self::layout(self.header().len as usize);
        // This was the last handle. The header and bytes need no drop.
        unsafe { dealloc(self.0.as_ptr().cast(), layout) };
    }
}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // `Borrow<[u8]>` needs the same hash as the slice.
        self.as_ref().hash(state);
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for Key {}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_ref().cmp(other.as_ref())
    }
}

impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.as_ref().escape_ascii().fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_share_one_allocation_and_compare_by_bytes() {
        assert_eq!(size_of::<Key>(), 8);
        assert_eq!(size_of::<Option<Key>>(), 8);
        let meta = KeyMeta {
            create_revision: 1,
            mod_revision: 2,
            version: 3,
            lease: 4,
            row_len: 5,
        };
        let a = Key::new(b"abc", meta).unwrap();
        let b = a.clone();
        let empty = Key::new(b"", KeyMeta::default()).unwrap();
        assert_eq!(a.as_ref(), b"abc");
        assert_eq!(empty.as_ref(), b"");
        assert!(empty < a);
        assert_eq!(a, Key::new(b"abc", KeyMeta::default()).unwrap());
        // The test owns every handle and does not hold a metadata reference across the write.
        unsafe {
            assert_eq!(*b.meta(), meta);
            a.set_meta(KeyMeta::default());
            assert_eq!(*b.meta(), KeyMeta::default());
        }
        drop(a);
        assert_eq!(b.header().refs.load(Ordering::Relaxed), 1);
        let set: std::collections::HashSet<Key> = [b].into_iter().collect();
        assert!(set.contains(&b"abc"[..]));
        assert!(!set.contains(&b"ab"[..]));
    }
}
