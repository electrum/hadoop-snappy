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

#include "org_apache_hadoop_io_compress_snappy_SnappyCompressor.h"
#include "hadoop_snappy.h"
#include <snappy.h>
#include <stdlib.h>

static jfieldID SnappyCompressor_clazz;
static jfieldID SnappyCompressor_uncompressedDirectBuf;
static jfieldID SnappyCompressor_uncompressedDirectBufLen;
static jfieldID SnappyCompressor_compressedDirectBuf;

JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_snappy_SnappyCompressor_initIDs
(JNIEnv *env, jclass clazz){
  SnappyCompressor_clazz = env->GetStaticFieldID(clazz, "clazz",
                                                 "Ljava/lang/Class;");
  SnappyCompressor_uncompressedDirectBuf = env->GetFieldID(clazz,
                                                           "uncompressedDirectBuf",
                                                           "Ljava/nio/Buffer;");
  SnappyCompressor_uncompressedDirectBufLen = env->GetFieldID(clazz,
                                                              "uncompressedDirectBufLen", "I");
  SnappyCompressor_compressedDirectBuf = env->GetFieldID(clazz,
                                                         "compressedDirectBuf",
                                                         "Ljava/nio/Buffer;");
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_compress_snappy_SnappyCompressor_compressBytesDirect
(JNIEnv *env, jobject thisj){
  // Get members of SnappyCompressor
  jclass c = env->GetObjectClass(thisj);
  jobject clazz = env->GetStaticObjectField(c, SnappyCompressor_clazz);
  jobject uncompressed_direct_buf = env->GetObjectField(thisj, SnappyCompressor_uncompressedDirectBuf);
  jint uncompressed_direct_buf_len = env->GetIntField(thisj, SnappyCompressor_uncompressedDirectBufLen);
  jobject compressed_direct_buf = env->GetObjectField(thisj, SnappyCompressor_compressedDirectBuf);
  
  // Get the input direct buffer
  LOCK_CLASS(env, clazz, "SnappyCompressor");
  const char* uncompressed_bytes = (const char*)env->GetDirectBufferAddress(uncompressed_direct_buf);
  UNLOCK_CLASS(env, clazz, "SnappyCompressor");

  if (uncompressed_bytes == 0) {
    return (jint)0;
  }

  // Get the output direct buffer
  LOCK_CLASS(env, clazz, "SnappyCompressor");
  char* compressed_bytes = (char *)env->GetDirectBufferAddress(compressed_direct_buf);
  UNLOCK_CLASS(env, clazz, "SnappyCompressor");

  if (compressed_bytes == 0) {
    return (jint)0;
  }

  size_t compress_length;
  snappy::RawCompress(uncompressed_bytes, uncompressed_direct_buf_len, compressed_bytes, &compress_length);

  env->SetIntField(thisj, SnappyCompressor_uncompressedDirectBufLen, 0);
  
  return (jint)compress_length;
}
