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

import java.io.Closeable;
import java.io.IOException;

import static io.airlift.compress.zstd.UnsafeUtil.*;

// Adapted from https://github.com/lz4/lz4-java/tree/master/src/java/net/jpountz/xxhash
public class StreamingXxHash64 implements Closeable {
  private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
  private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
  private static final long PRIME64_3 = 0x165667B19E3779F9L;
  private static final long PRIME64_4 = 0x85EBCA77C2b2AE63L;
  private static final long PRIME64_5 = 0x27D4EB2F165667C5L;

  private final byte[] memory;
  private final long seed;
  private int memSize;
  private long v1, v2, v3, v4;
  private long totalLen;

  StreamingXxHash64(long seed) {
    this.seed = seed;
    memory = new byte[32];
    reset();
  }

  /**
   * Returns the value of the checksum.
   *
   * @return the checksum
   */
  public long getValue() {
    long h64;
    if (this.totalLen >= 32L) {
      long v1 = this.v1;
      long v2 = this.v2;
      long v3 = this.v3;
      long v4 = this.v4;

      h64 = Long.rotateLeft(v1, 1) + Long.rotateLeft(v2, 7) + Long.rotateLeft(v3, 12) + Long.rotateLeft(v4, 18);

      v1 *= PRIME64_2; v1 = Long.rotateLeft(v1, 31); v1 *= PRIME64_1; h64 ^= v1;
      h64 = h64 * PRIME64_1 + PRIME64_4;

      v2 *= PRIME64_2; v2 = Long.rotateLeft(v2, 31); v2 *= PRIME64_1; h64 ^= v2;
      h64 = h64 * PRIME64_1 + PRIME64_4;

      v3 *= PRIME64_2; v3 = Long.rotateLeft(v3, 31); v3 *= PRIME64_1; h64 ^= v3;
      h64 = h64 * PRIME64_1 + PRIME64_4;

      v4 *= PRIME64_2; v4 = Long.rotateLeft(v4, 31); v4 *= PRIME64_1; h64 ^= v4;
      h64 = h64 * PRIME64_1 + PRIME64_4;
    } else {
      h64 = seed + PRIME64_5;
    }

    h64 += totalLen;

    int off = 0;
    while (off <= memSize - 8) {
      long k1 = readLongLE(memory, off);
      k1 *= PRIME64_2; k1 = Long.rotateLeft(k1, 31); k1 *= PRIME64_1; h64 ^= k1;
      h64 = Long.rotateLeft(h64, 27) * PRIME64_1 + PRIME64_4;
      off += 8;
    }

    if (off <= memSize - 4) {
      h64 ^= ((long) readIntLE(memory, off) & 4294967295L) * PRIME64_1;
      h64 = Long.rotateLeft(h64, 23) * PRIME64_2 + PRIME64_3;
      off += 4;
    }

    while (off < memSize) {
      h64 ^= (memory[off] & 0xFF) * PRIME64_5;
      h64 = Long.rotateLeft(h64, 11) * PRIME64_1;
      ++off;
    }

    h64 ^= h64 >>> 33;
    h64 *= PRIME64_2;
    h64 ^= h64 >>> 29;
    h64 *= PRIME64_3;
    h64 ^= h64 >>> 32;

    return h64;
  }

  /**
   * Updates the value of the hash with a single byte
   *
   * @param b the input data
   */
  public void update(byte b) {
    byte[] bytes = {b};
    update(bytes, 0, 1);
  }

  /**
   * Updates the value of the hash with buf
   *
   * @param buf the input data
   */
  public void update(byte[] buf) {
    update(buf, 0, buf.length);
  }

  /**
   * Updates the value of the hash with buf[off:off+len].
   *
   * @param buf the input data
   * @param off the start offset in buf
   * @param len the number of bytes to hash
   */
  public void update(byte[] buf, int off, int len) {
    totalLen += len;

    if (memSize + len < 32) { // fill in tmp buffer
      System.arraycopy(buf, off, memory, memSize, len);
      memSize += len;
      return;
    }

    final int end = off + len;

    if (memSize > 0) { // data left from previous update
      System.arraycopy(buf, off, memory, memSize, 32 - memSize);

      v1 += readLongLE(memory, 0) * PRIME64_2;
      v1 = Long.rotateLeft(v1, 31);
      v1 *= PRIME64_1;

      v2 += readLongLE(memory, 8) * PRIME64_2;
      v2 = Long.rotateLeft(v2, 31);
      v2 *= PRIME64_1;

      v3 += readLongLE(memory, 16) * PRIME64_2;
      v3 = Long.rotateLeft(v3, 31);
      v3 *= PRIME64_1;

      v4 += readLongLE(memory, 24) * PRIME64_2;
      v4 = Long.rotateLeft(v4, 31);
      v4 *= PRIME64_1;

      off += 32 - memSize;
      memSize = 0;
    }

    {
      final int limit = end - 32;
      long v1 = this.v1;
      long v2 = this.v2;
      long v3 = this.v3;
      long v4 = this.v4;

      while (off <= limit) {
        v1 += readLongLE(buf, off) * PRIME64_2;
        v1 = Long.rotateLeft(v1, 31);
        v1 *= PRIME64_1;
        off += 8;

        v2 += readLongLE(buf, off) * PRIME64_2;
        v2 = Long.rotateLeft(v2, 31);
        v2 *= PRIME64_1;
        off += 8;

        v3 += readLongLE(buf, off) * PRIME64_2;
        v3 = Long.rotateLeft(v3, 31);
        v3 *= PRIME64_1;
        off += 8;

        v4 += readLongLE(buf, off) * PRIME64_2;
        v4 = Long.rotateLeft(v4, 31);
        v4 *= PRIME64_1;
        off += 8;
      }

      this.v1 = v1;
      this.v2 = v2;
      this.v3 = v3;
      this.v4 = v4;
    }

    if (off < end) {
      System.arraycopy(buf, off, memory, 0, end - off);
      memSize = end - off;
    }
  }

  /**
   * Resets this instance to the state it had right after instantiation. The seed remains
   * unchanged.
   */
  public void reset() {
    v1 = seed + PRIME64_1 + PRIME64_2;
    v2 = seed + PRIME64_2;
    v3 = seed + 0;
    v4 = seed - PRIME64_1;
    totalLen = 0;
    memSize = 0;
  }

  /**
   * Releases any system resources associated with this instance. It is not mandatory to call this
   * method after using this instance because the system resources are released anyway when this
   * instance is reclaimed by GC.
   */
  @Override
  public void close() throws IOException {
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(seed=" + seed + ")";
  }
}
