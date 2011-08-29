/**
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

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Determines if Snappy native library is available and loads it if available.
 */
public class LoadSnappy {
  private static final Log LOG = LogFactory.getLog(LoadSnappy.class.getName());

  private static boolean LOADED = false;

  static {
    try {
      System.loadLibrary("snappy");
      System.loadLibrary("hadoopsnappy");

      // Find the path to the native library
      String snappyPath = findLibrary("snappy");

      // Initialize the native library. This causes hadoopsnappy to
      // dynamically load the symbols from the native snappy library.
      // If this fails, the library can't be used, and attempting to
      // use it will cause the JVM to crash.
      SnappyCompressor.initIDs(snappyPath);
      SnappyDecompressor.initIDs(snappyPath);

      LOADED = true;
    } catch (UnsatisfiedLinkError ex) {
      LOG.warn("Failed to load library from " +
          System.getProperty("java.library.path") + ": " + ex.getMessage());
    }
    if (LOADED) {
      LOG.info("Snappy native library loaded");
    } else {
      LOG.warn("Snappy native library not loaded");
    }
  }

  private static String findLibrary(String name) {
    name = System.mapLibraryName(name).replace(".jnilib", ".dylib");
    String paths[] = System.getProperty("java.library.path").split(":");
    for (String path : paths) {
      File file = new File(path, name);
      if (file.exists()) {
        return file.toString();
      }
    }
    throw new UnsatisfiedLinkError("cannot find path to " + name);
  }

  /**
   * Returns if Snappy native library is loaded.
   *
   * @return <code>true</code> if Snappy native library is loaded,
   * <code>false</code> if not.
   */
  public static boolean isLoaded() {
    return LOADED;
  }

}
