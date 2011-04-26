package org.apache.hadoop.io.compress.snappy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SnappyNativeCodeLoader {

  private static final Log LOG = LogFactory.getLog(SnappyNativeCodeLoader.class);
  private static boolean nativeCodeLoaded = false;

  static {
    try {
      System.loadLibrary("snappy");
      System.loadLibrary("hadoopsnappy");
      nativeCodeLoaded = true;
      LOG.info("Loaded native snappy library");
    } catch (Throwable t) {
      LOG.error("Could not load native snappy library", t);
      nativeCodeLoaded = false;
    }
  }
  
  /**
   * Are the native snappy libraries loaded? 
   * @return true if loaded, otherwise false
   */
  public static boolean isNativeCodeLoaded() {
    return nativeCodeLoaded;
  }
}