use std::{
    io,
    ffi::OsString,
};
use thiserror::Error;

/// map_reduce中可能出现的所有自定义error, 包括client里的.
#[derive(Debug, Error)]
pub enum MapReduceError{
    #[error("Wrong message type received")]
    WrongMessageType,

    #[error("Unknown task id received")]
    WrongTaskId,

    #[error("Couldn't load pub fn {fntype:?}")]
    DllLoadingError{
        fntype : String,
    },

    #[error("File IO errors occur")]
    FileIOError(#[from] io::Error),

    #[error("Path processing failed (like when getting filename, extension name, 
            converting OsString into String, ...)")]
    PathError,

    #[error("Task Failed.")]
    TaskFailed,
}