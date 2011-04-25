package org.apache.hadoop.io.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.BlockCompressorStream;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.apache.hadoop.io.compress.snappy.SnappyNativeCodeLoader;

/**
 * This class creates snappy compressors/decompressors.
 * 
 */
public class SnappyCodec implements Configurable, CompressionCodec {
  public static final String SNAPPY_BUFFER_SIZE_KEY = "io.compression.codec.snappy.buffersize";
  public static final int DEFAULT_SNAPPY_BUFFER_SIZE = 256 * 1024;

  Configuration conf;

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return conf;
  }

  private static boolean nativeSnappyLoaded = false;

  static {
    if (SnappyNativeCodeLoader.isNativeCodeLoaded()) {
      nativeSnappyLoaded = SnappyCompressor.isNativeSnappyLoaded()
          && SnappyDecompressor.isNativeSnappyLoaded();
    }
  }
  
  /**
   * Are the native snappy libraries loaded & initialized? 
   * 
   * @param conf configuration
   * @return true if loaded & initialized, otherwise false
   */
  public static boolean isNativeSnappyLoaded(Configuration conf) {
    return nativeSnappyLoaded && conf.getBoolean("hadoop.native.lib", true);
  }

  public CompressionOutputStream createOutputStream(OutputStream out)
      throws IOException {
    return createOutputStream(out, createCompressor());
  }

  public CompressionOutputStream createOutputStream(OutputStream out,
      Compressor compressor) throws IOException {
    if (!isNativeSnappyLoaded(conf)) {
      throw new RuntimeException("native snappy library not available");
    }
    int bufferSize = conf.getInt(SNAPPY_BUFFER_SIZE_KEY,
        DEFAULT_SNAPPY_BUFFER_SIZE);

    int compressionOverhead = (bufferSize >> 3) + 128 + 3;
    // int compressionOverhead = 0;

    return new BlockCompressorStream(out, compressor, bufferSize,
        compressionOverhead);
  }

  public Class<? extends Compressor> getCompressorType() {
    if (!isNativeSnappyLoaded(conf)) {
      throw new RuntimeException("native snappy library not available");
    }

    return SnappyCompressor.class;
  }

  public Compressor createCompressor() {
    if (!isNativeSnappyLoaded(conf)) {
      throw new RuntimeException("native snappy library not available");
    }
    int bufferSize = conf.getInt(SNAPPY_BUFFER_SIZE_KEY,
        DEFAULT_SNAPPY_BUFFER_SIZE);
    return new SnappyCompressor(bufferSize);
  }

  public CompressionInputStream createInputStream(InputStream in)
      throws IOException {
    return createInputStream(in, createDecompressor());
  }

  public CompressionInputStream createInputStream(InputStream in,
      Decompressor decompressor) throws IOException {
    if (!isNativeSnappyLoaded(conf)) {
      throw new RuntimeException("native snappy library not available");
    }

    return new BlockDecompressorStream(in, decompressor, conf.getInt(
        SNAPPY_BUFFER_SIZE_KEY, DEFAULT_SNAPPY_BUFFER_SIZE));
  }

  public Class<? extends Decompressor> getDecompressorType() {
    if (!isNativeSnappyLoaded(conf)) {
      throw new RuntimeException("native snappy library not available");
    }

    return SnappyDecompressor.class;
  }

  public Decompressor createDecompressor() {
    if (!isNativeSnappyLoaded(conf)) {
      throw new RuntimeException("native snappy library not available");
    }
    int bufferSize = conf.getInt(SNAPPY_BUFFER_SIZE_KEY,
        DEFAULT_SNAPPY_BUFFER_SIZE);
    return new SnappyDecompressor(bufferSize);
  }

  public String getDefaultExtension() {
    return ".snappy";
  }
}
