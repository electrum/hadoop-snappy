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