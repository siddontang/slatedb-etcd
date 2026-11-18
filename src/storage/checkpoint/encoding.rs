//! Encode local checkpoint files in bounded blocks. Raft sends the original snapshot bytes.

use anyhow::{ensure, Context, Result};
use lz4_flex::frame::{BlockSize, FrameDecoder, FrameEncoder, FrameInfo};
use std::fs::File;
use std::io::{BufWriter, Read, Write};

fn worth_compressing(data: &[u8]) -> bool {
    const WINDOW: usize = 4096;
    const SAMPLES: usize = 4;
    if data.len() < 64 * 1024 {
        return false;
    }
    // Sample across the sorted key spaces. Require at least a twofold reduction.
    let step = (data.len() - WINDOW) / (SAMPLES - 1);
    let mut buffer = [0; lz4_flex::block::get_maximum_output_size(WINDOW)];
    let mut compressed = 0;
    for i in 0..SAMPLES {
        let start = step * i;
        compressed += lz4_flex::block::compress_into(&data[start..start + WINDOW], &mut buffer)
            .expect("sample output fits");
    }
    compressed < SAMPLES * WINDOW / 2
}

pub(super) fn write(file: &mut File, data: &[u8]) -> Result<Option<u64>> {
    if !worth_compressing(data) {
        file.write_all(data)?;
        return Ok(None);
    }
    let info = FrameInfo::new().block_size(BlockSize::Max64KB);
    let sink = BufWriter::with_capacity(128 * 1024, &mut *file);
    let mut encoder = FrameEncoder::with_frame_info(info, sink);
    encoder.write_all(data)?;
    encoder.finish()?.flush()?;
    Ok(Some(file.metadata()?.len()))
}

pub(super) fn read(mut file: File, len: u64, compressed: Option<u64>) -> Result<Vec<u8>> {
    ensure!(
        file.metadata()?.len() == compressed.unwrap_or(len),
        "checkpoint length mismatch"
    );
    let mut data = Vec::new();
    if compressed.is_some() {
        let limit = len.checked_add(1).context("invalid checkpoint length")?;
        FrameDecoder::new(file).take(limit).read_to_end(&mut data)?;
    } else {
        file.read_to_end(&mut data)?;
    }
    ensure!(data.len() as u64 == len, "checkpoint length mismatch");
    Ok(data)
}
