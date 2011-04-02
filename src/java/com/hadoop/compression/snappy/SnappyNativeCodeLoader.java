package com.hadoop.compression.snappy;

public class SnappyNativeCodeLoader {
  private static boolean nativeCodeLoaded = false;

  static {
    try {
      System.loadLibrary("snappy");
      System.loadLibrary("hadoopsnappy");
      nativeCodeLoaded = true;
    } catch (Throwable t) {
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