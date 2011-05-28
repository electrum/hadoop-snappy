/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.io.compress.snappy;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;

/**
 * A {@link Compressor} based on the snappy compression algorithm.
 * http://code.google.com/p/snappy/
 */
public class SnappyCompressor implements Compressor {
  private static final Log LOG = 
    LogFactory.getLog(SnappyCompressor.class.getName());
  private static final int DEFAULT_DIRECT_BUFFER_SIZE = 64 * 1024;

  // HACK - Use this as a global lock in the JNI layer
  @SuppressWarnings({ "unchecked", "unused" })
  private static Class clazz = SnappyCompressor.class;

  private int directBufferSize;
  private Buffer compressedDirectBuf = null;
  private int uncompressedDirectBufLen;
  private Buffer uncompressedDirectBuf = null;
  private byte[] userBuf = null;
  private int userBufOff = 0, userBufLen = 0;
  private boolean finish, finished;

  private long bytesRead = 0L;
  private long bytesWritten = 0L;

  private static boolean nativeSnappyLoaded = false;

  static {
    if (SnappyNativeCodeLoader.isNativeCodeLoaded()) {
      // Initialize the native library
      try {
        initIDs();
        nativeSnappyLoaded = true;
      } catch (Throwable t) {
        // Ignore failure to load/initialize snappy
        LOG.warn(t.toString());
        nativeSnappyLoaded = false;
      }
    } else {
      LOG.error("Cannot load " + SnappyCompressor.class.getName() + 
      " without snappy library!");
      nativeSnappyLoaded = false;
    }
  }
  
  /**
   * Are the snappy compressors initialized? 
   * 
   * @return true if initialized, otherwise false
   */
  public static boolean isNativeSnappyLoaded() {
    return nativeSnappyLoaded;
  }

  /**
   * Creates a new compressor.
   * 
   * @param directBufferSize size of the direct buffer to be used.
   */
  public SnappyCompressor(int directBufferSize) {
    this.directBufferSize = directBufferSize;

    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf.position(directBufferSize);
  }

  /**
   * Creates a new compressor with the default buffer size.
   */
  public SnappyCompressor() {
    this(DEFAULT_DIRECT_BUFFER_SIZE);
  }

  public void setInput(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    finished = false;

    if (len > uncompressedDirectBuf.remaining()) {
      // save data; now !needsInput
      this.userBuf = b;
      this.userBufOff = off;
      this.userBufLen = len;
    } else {
      ((ByteBuffer) uncompressedDirectBuf).put(b, off, len);
      uncompressedDirectBufLen = uncompressedDirectBuf.position();
    }

    bytesRead += len;
  }
  
  /**
   * If a write would exceed the capacity of the direct buffers, it is set
   * aside to be loaded by this function while the compressed data are
   * consumed.
   */
  void setInputFromSavedData() {
    if (0 >= userBufLen) {
      return;
    }
    finished = false;

    uncompressedDirectBufLen = Math.min(userBufLen, directBufferSize);
    ((ByteBuffer) uncompressedDirectBuf).put(userBuf, userBufOff,
        uncompressedDirectBufLen);

    // Note how much data is being fed to snappy
    userBufOff += uncompressedDirectBufLen;
    userBufLen -= uncompressedDirectBufLen;
  }

  public void setDictionary(byte[] b, int off, int len) {
    // do nothing
  }

  public boolean needsInput() {
    return !(compressedDirectBuf.remaining() > 0
        || uncompressedDirectBuf.remaining() == 0 || userBufLen > 0);
  }

  public void finish() {
    finish = true;
  }

  public boolean finished() {
    // Check if all uncompressed data has been consumed
    return (finish && finished && compressedDirectBuf.remaining() == 0);
  }

  public int compress(byte[] b, int off, int len)
      throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    // Check if there is compressed data
    int n = compressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      ((ByteBuffer) compressedDirectBuf).get(b, off, n);
      bytesWritten += n;
      return n;
    }

    // Re-initialize the snappy's output direct-buffer
    compressedDirectBuf.clear();
    compressedDirectBuf.limit(0);
    if (0 == uncompressedDirectBuf.position()) {
      // No compressed data, so we should have !needsInput or !finished
      setInputFromSavedData();
      if (0 == uncompressedDirectBuf.position()) {
        // Called without data; write nothing
        finished = true;
        return 0;
      }
    }

    // Compress data
    n = compressBytesDirect();
    compressedDirectBuf.limit(n);
    uncompressedDirectBuf.clear(); // snappy consumes all buffer input

    // Set 'finished' if snapy has consumed all user-data
    if (0 == userBufLen) {
      finished = true;
    }

    // Get atmost 'len' bytes
    n = Math.min(n, len);
    bytesWritten += n;
    ((ByteBuffer) compressedDirectBuf).get(b, off, n);

    return n;
  }

  public void reset() {
    finish = false;
    finished = false;
    uncompressedDirectBuf.clear();
    uncompressedDirectBufLen = 0;
    compressedDirectBuf.clear();
    compressedDirectBuf.limit(0);
    userBufOff = userBufLen = 0;
    bytesRead = bytesWritten = 0L;
  }

  public void reinit(Configuration conf) {
    reset();
  }
  
  /**
   * Return number of bytes given to this compressor since last reset.
   */
  public long getBytesRead() {
    return bytesRead;
  }
  
  /**
   * Return number of bytes consumed by callers of compress since last reset.
   */
  public long getBytesWritten() {
    return bytesWritten;
  }

  public void end() {
  }

  private native static void initIDs();

  private native int compressBytesDirect();
}
