use libc::{
    c_void, mlock, mmap, mprotect, munlock, munmap, MAP_ANONYMOUS, MAP_PRIVATE, MAP_SHARED, PROT_READ,
    PROT_WRITE,
};
use std::{fs::File, os::unix::prelude::AsRawFd, ptr::NonNull, sync::Arc};

pub struct MappedBuffer {
    ptr: NonNull<u8>,
    len: usize,
}

unsafe impl Send for MappedBuffer {}
unsafe impl Sync for MappedBuffer {}

impl AsRef<[u8]> for MappedBuffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }
}

impl MappedBuffer {
    /// Create an anonymous mmap buffer
    pub fn anonymous(
        len: usize,
        initializer: Option<&mut dyn FnMut(&mut [u8])>,
    ) -> std::io::Result<Arc<Self>> {
        let ptr = unsafe {
            mmap(
                std::ptr::null_mut(),
                len,
                PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }

        unsafe {
            mlock(ptr, len); // prevent from being swapped
        }
        initializer.map(|f| {
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr as *mut u8, len) };
            f(slice);
            // once its initialized, we can make it read-only
            unsafe {
                mprotect(ptr, len, PROT_READ);
            }
        });
        Ok(Arc::new(Self {
            ptr: NonNull::new(ptr as *mut u8).unwrap(),
            len,
        }))
    }

    /// Create a file-backed mmap buffer
    pub fn from_file(file: &File) -> std::io::Result<Arc<Self>> {
        let len = file.metadata()?.len() as usize;
        let ptr = unsafe {
            mmap(
                std::ptr::null_mut(),
                len,
                PROT_READ,
                MAP_SHARED,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(std::io::Error::last_os_error());
        }

        unsafe {
            mlock(ptr, len); // prevent from being swapped
        }

        Ok(Arc::new(Self {
            ptr: NonNull::new(ptr as *mut u8).unwrap(),
            len,
        }))
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
    pub fn len(&self) -> usize {
        self.len
    }
}

impl Drop for MappedBuffer {
    fn drop(&mut self) {
        unsafe {
            munlock(self.ptr.as_ptr() as *mut c_void, self.len);
            munmap(self.ptr.as_ptr() as *mut c_void, self.len);
        }
    }
}

#[cfg(test)]
#[path = "mmap_tests.rs"]
mod tests;
