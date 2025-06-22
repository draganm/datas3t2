# TAR Index

A TAR index file describes the layout of a TAR archive file. It contains metadata about each file entry in the archive:

- **Header Position**: The byte offset where the file's TAR header begins
- **Header Blocks**: The number of 512-byte blocks occupied by the TAR header
- **File Size**: The total number of bytes in the file content

## Binary Format

To minimize file size, the index uses a compact binary format with the following structure:

| Field | Size | Description |
|-------|------|-------------|
| Header Position | 8 bytes | Byte offset to the TAR header |
| Header Blocks | 2 bytes | Number of 512-byte blocks for the header |
| File Size | 6 bytes | Total file size in bytes |

All values are stored in **big-endian** byte order.

## Example

For a file with:
- Header at byte position 1024
- Header spanning 1 block (512 bytes)
- File size of 2048 bytes

The index entry would be: `00 00 00 00 00 00 04 00 00 01 00 00 00 00 08 00`

