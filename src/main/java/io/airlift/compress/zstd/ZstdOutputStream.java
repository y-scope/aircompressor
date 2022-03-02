/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.airlift.compress.zstd;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static io.airlift.compress.zstd.CompressionParameters.DEFAULT_COMPRESSION_LEVEL;
import static io.airlift.compress.zstd.Constants.*;
import static io.airlift.compress.zstd.UnsafeUtil.UNSAFE;
import static io.airlift.compress.zstd.UnsafeUtil.put24BitLittleEndian;
import static io.airlift.compress.zstd.ZstdFrameCompressor.*;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

/**
 * Zstd outputstream compressor based on zstd frame compressor
 * Frame is kept open
 */

public class ZstdOutputStream extends FilterOutputStream {

  // Source and Destination positions
  private long srcPos = 0;
  private long dstPos = 0;
  private boolean isClosed = false;

  private boolean frameClosed = true;

  private boolean useChecksum = false;
  private boolean closeFrameOnFlush = false;

  private int blockSize = 0;

  private CompressionParameters parameters;
  private CompressionContext context;

  private byte[] inputBase;
  private long inputPos;

  private byte[] outputBase;

  private byte[] blockerHeaderBase;

  private static final int ZSTD_CONTENTSIZE_UNKNOWN = 0;

  private StreamingXxHash64 streamingXxHash64;

  public ZstdOutputStream(OutputStream outStream) throws IOException {
    super(outStream);
    initializeStream();
  }

  public ZstdOutputStream(OutputStream outputStream, boolean closeFrameOnFlush,
                          boolean useChecksum) {
    super(outputStream);
    this.closeFrameOnFlush = closeFrameOnFlush;
    this.useChecksum = useChecksum;
    initializeStream();
  }

  private void initializeStream() {
    parameters = CompressionParameters.getDefaultParameters(DEFAULT_COMPRESSION_LEVEL, 0);

    // Allocate an input buffer the same size as the sliding window size
    inputBase = new byte[MAX_BLOCK_SIZE];
    inputPos = 0;

    ZstdCompressor ctx = new ZstdCompressor();
    int maxLength = ctx.maxCompressedLength(MAX_BLOCK_SIZE);
    outputBase = new byte[maxLength];   // 128KB

    blockerHeaderBase = new byte[SIZE_OF_BLOCK_HEADER];

    if (useChecksum) {
      streamingXxHash64 = new StreamingXxHash64(0);
    }
  }


  private void openFrame() throws IOException {
    // Generate magic and frame header bytes into output buffer
    long output = ARRAY_BYTE_BASE_OFFSET;
    long outputLimit = outputBase.length + ARRAY_BYTE_BASE_OFFSET;
    output += writeMagic(outputBase, output, outputLimit);
    // TODO
//    output += writeFrameHeader(outputBase, output, outputLimit, inputBase.length, useChecksum);
    output += writeFrameHeader(outputBase, output, outputLimit, inputBase.length,
        1 << parameters.getWindowLog());

    // Transfer buffer content to outputStream
    out.write(outputBase, 0, (int) (output - ARRAY_BYTE_BASE_OFFSET));

    context = new CompressionContext(parameters, inputPos + ARRAY_BYTE_BASE_OFFSET, MAX_BLOCK_SIZE);

    if (useChecksum) {
      streamingXxHash64.reset();
    }

    frameClosed = false;
  }


  /**
   * Enable closing the frame on flush.
   *
   * This will guarantee that it can be ready fully if the process crashes
   * before closing the stream. On the downside it will negatively affect
   * the compression ratio.
   *
   * Default: false.
   */
  public ZstdOutputStream setCloseFrameOnFlush(boolean closeFrameOnFlush) {
    this.closeFrameOnFlush = closeFrameOnFlush;
    return this;
  }

  public void compressBlockToOutputStream(byte[] inputBase, long inputPos, int inputSize,
                                          boolean lastBlock) throws IOException {
    // Compress the entire input buffer as a block
    int compressedSize = compressBlock(inputBase, inputPos + ARRAY_BYTE_BASE_OFFSET, inputSize,
        outputBase, ARRAY_BYTE_BASE_OFFSET, MAX_BLOCK_SIZE, context, parameters);

    int blockHeader;
    int lastBlockFlag = lastBlock ? 1 : 0;
    if (compressedSize == 0) { // block is not compressible
      // Write block header
      blockHeader = lastBlockFlag | (RAW_BLOCK << 1) | (inputSize << 3);
      put24BitLittleEndian(blockerHeaderBase, ARRAY_BYTE_BASE_OFFSET, blockHeader);
      out.write(blockerHeaderBase);

      // Write raw block content directly from input buffer and reset position to the beginning
      out.write(inputBase, (int) inputPos, inputSize);
    } else {
      blockHeader = lastBlockFlag | (COMPRESSED_BLOCK << 1) | (compressedSize << 3);
      put24BitLittleEndian(blockerHeaderBase, ARRAY_BYTE_BASE_OFFSET, blockHeader);
      out.write(blockerHeaderBase);

      // Write compressed block content and reset input buffer position to the beginning
      out.write(outputBase, 0, compressedSize);
    }
  }

  /**
   * Writes the specified byte to this output stream. The general
   * contract for {@code write} is that one byte is written
   * to the output stream. The byte to be written is the eight
   * low-order bits of the argument {@code b}. The 24
   * high-order bits of {@code b} are ignored.
   * <p>
   * Subclasses of {@code OutputStream} must provide an
   * implementation for this method.
   *
   * @param      b   the {@code byte}.
   * @throws     IOException  if an I/O error occurs. In particular,
   *             an {@code IOException} may be thrown if the
   *             output stream has been closed.
   */
  @Override
  public void write(int b) throws IOException {
    if (useChecksum) {
      streamingXxHash64.update((byte) b);
    }

    if (frameClosed) {
      openFrame();
    }

    long inputRemaining = inputBase.length - inputPos;
    if (inputRemaining == 0) {
      // Input buffer is full, compress the entire input buffer as a block
      compressBlockToOutputStream(inputBase, 0, inputBase.length, false);

      // Reset input buffer to the beginning
      inputPos = 0;
    }

    UNSAFE.putByte(inputBase, inputPos + ARRAY_BYTE_BASE_OFFSET, (byte) b);
    ++inputPos;
  }

  /**
   * Writes {@code b.length} bytes from the specified byte array
   * to this output stream. The general contract for {@code write(b)}
   * is that it should have exactly the same effect as the call
   * {@code write(b, 0, b.length)}.
   *
   * @param      b   the data.
   * @throws     IOException  if an I/O error occurs.
   * @see        java.io.OutputStream#write(byte[], int, int)
   */
  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  /**
   * Writes {@code len} bytes from the specified byte array
   * starting at offset {@code off} to this output stream.
   * The general contract for {@code write(b, off, len)} is that
   * some of the bytes in the array {@code b} are written to the
   * output stream in order; element {@code b[off]} is the first
   * byte written and {@code b[off+len-1]} is the last byte written
   * by this operation.
   * <p>
   * The {@code write} method of {@code OutputStream} calls
   * the write method of one argument on each of the bytes to be
   * written out. Subclasses are encouraged to override this method and
   * provide a more efficient implementation.
   * <p>
   * If {@code b} is {@code null}, a
   * {@code NullPointerException} is thrown.
   * <p>
   * If {@code off} is negative, or {@code len} is negative, or
   * {@code off+len} is greater than the length of the array
   * {@code b}, then an {@code IndexOutOfBoundsException} is thrown.
   *
   * @param      b     the data.
   * @param      off   the start offset in the data.
   * @param      len   the number of bytes to write.
   * @throws     IOException  if an I/O error occurs. In particular,
   *             an {@code IOException} is thrown if the output
   *             stream is closed.
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (useChecksum) {
      streamingXxHash64.update(b, off, len);
    }

    if (frameClosed) {
      openFrame();
    }

    // Compress
    long inputRemaining = inputBase.length - inputPos;
    if (inputRemaining == 0) {
      // Input buffer is full, compress the entire input buffer as a block
      compressBlockToOutputStream(inputBase, 0, inputBase.length, false);

      // Reset input buffer to the beginning
      inputPos = 0;
    }

    if (inputPos + len <= inputBase.length) {
      // b[] is smaller than the input buffer's remaining capacity, append to the buffer
      UNSAFE.copyMemory(b, ARRAY_BYTE_BASE_OFFSET, inputBase, inputPos + ARRAY_BYTE_BASE_OFFSET,
          len);
      inputPos += len;
    } else {
      // b[] is larger than input buffer's remaining capacity

      // 1. Append some bytes from b to construct a full input buffer
      // skip if input buffer is empty and inputLength is larger window
      if (len < inputBase.length) {
        int numBytesToCopy = (int) (inputBase.length - inputPos);
        UNSAFE.copyMemory(b, off + ARRAY_BYTE_BASE_OFFSET, inputBase, inputPos + ARRAY_BYTE_BASE_OFFSET,
            numBytesToCopy);
        off += numBytesToCopy;

        // At this point, we have a full input buffer and want to compress it
        compressBlockToOutputStream(inputBase, 0, inputBase.length, false);
        len -= numBytesToCopy;

        // Reset input buffer to the beginning
        inputPos = 0;
      }

      // 2. Compress as much as possible from b[] directly
      while (len > inputBase.length) {
        compressBlockToOutputStream(b, off, inputBase.length, false);
        off += inputBase.length;
        len -= inputBase.length;
      }

      // 3. If there are left-over uncompressed bytes, copy them into the input buffer
      if (len > 0) {
        UNSAFE.copyMemory(b, off + ARRAY_BYTE_BASE_OFFSET, inputBase, inputPos + ARRAY_BYTE_BASE_OFFSET, len);
        inputPos += len;
      }
    }
  }

  /**
   * Flushes this output stream and forces any buffered output bytes
   * to be written out. The general contract of {@code flush} is
   * that calling it is an indication that, if any bytes previously
   * written have been buffered by the implementation of the output
   * stream, such bytes should immediately be written to their
   * intended destination.
   * <p>
   * If the intended destination of this stream is an abstraction provided by
   * the underlying operating system, for example a file, then flushing the
   * stream guarantees only that bytes previously written to the stream are
   * passed to the operating system for writing; it does not guarantee that
   * they are actually written to a physical device such as a disk drive.
   * <p>
   * The {@code flush} method of {@code OutputStream} does nothing.
   *
   * @throws     IOException  if an I/O error occurs.
   */
  @Override
  public void flush() throws IOException {
    if (closeFrameOnFlush) {
      // Close frame basically means the flushed block is the last block
      closeFrame();
    } else {
      if (inputPos == 0) {
        return;
      }
      // Flush block, but don't close frame
      compressBlockToOutputStream(inputBase, 0, (int) inputPos, false);
      inputPos = 0;
    }

    // FLush the outputstream
    out.flush();
  }

  /** Re-entrant capable */
  public void closeFrame() throws IOException {
    if (frameClosed) {
      return;
    }

    // Compress the data in the input buffer and flush it as last block
    compressBlockToOutputStream(inputBase, 0, (int) inputPos, true);
    inputPos = 0;

    if (useChecksum) {
      // Flush the last 4 bytes of the checksum as little endian integer
      UNSAFE.putInt(outputBase, ARRAY_BYTE_BASE_OFFSET, (int) streamingXxHash64.getValue());
      out.write(outputBase, 0, Integer.BYTES);
    }

    // Close frame
    frameClosed = true;
  }

  /**
   * Closes this output stream and releases any system resources
   * associated with this stream. The general contract of {@code close}
   * is that it closes the output stream. A closed stream cannot perform
   * output operations and cannot be reopened.
   * <p>
   * The {@code close} method of {@code OutputStream} does nothing.
   *
   * @throws     IOException  if an I/O error occurs.
   */
  @Override
  public void close() throws IOException {
    // Flush input buffer
    closeFrameOnFlush = true;
    flush();

    out.close();
    streamingXxHash64.close();
  }
}
