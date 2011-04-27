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

#include "org_apache_hadoop_io_compress_snappy_SnappyDecompressor.h"
#include "hadoop_snappy.h"
#include <snappy.h>
#include <stdlib.h>

static jfieldID SnappyDecompressor_clazz;
static jfieldID SnappyDecompressor_compressedDirectBuf;
static jfieldID SnappyDecompressor_compressedDirectBufLen;
static jfieldID SnappyDecompressor_uncompressedDirectBuf;

JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_snappy_SnappyDecompressor_initIDs
(JNIEnv *env, jclass clazz){
  SnappyDecompressor_clazz = env->GetStaticFieldID(clazz, "clazz",
                                                   "Ljava/lang/Class;");
  SnappyDecompressor_compressedDirectBuf = env->GetFieldID(clazz,
                                                           "compressedDirectBuf",
                                                           "Ljava/nio/Buffer;");
  SnappyDecompressor_compressedDirectBufLen = env->GetFieldID(clazz,
                                                              "compressedDirectBufLen", "I");
  SnappyDecompressor_uncompressedDirectBuf = env->GetFieldID(clazz,
                                                             "uncompressedDirectBuf",
                                                             "Ljava/nio/Buffer;");
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_compress_snappy_SnappyDecompressor_decompressBytesDirect
(JNIEnv *env, jobject thisj){
  // Get members of SnappyDecompressor
  jclass c = env->GetObjectClass(thisj);
  jobject clazz = env->GetStaticObjectField(c, SnappyDecompressor_clazz);
  jobject compressed_direct_buf = env->GetObjectField(thisj, SnappyDecompressor_compressedDirectBuf);
  jint compressed_direct_buf_len = env->GetIntField(thisj, SnappyDecompressor_compressedDirectBufLen);
  jobject uncompressed_direct_buf = env->GetObjectField(thisj, SnappyDecompressor_uncompressedDirectBuf);
  
  // Get the input direct buffer
  LOCK_CLASS(env, clazz, "SnappyDecompressor");
  const char* compressed_bytes = (const char*)env->GetDirectBufferAddress(compressed_direct_buf);
  UNLOCK_CLASS(env, clazz, "SnappyDecompressor");

  if (compressed_bytes == 0) {
    return (jint)0;
  }

  // Get the output direct buffer
  LOCK_CLASS(env, clazz, "SnappyDecompressor");
  char* uncompressed_bytes = (char *)env->GetDirectBufferAddress(uncompressed_direct_buf);
  UNLOCK_CLASS(env, clazz, "SnappyDecompressor");

  if (uncompressed_bytes == 0) {
    return (jint)0;
  }

  size_t uncompress_length;
  bool ret = snappy::GetUncompressedLength(compressed_bytes, compressed_direct_buf_len, &uncompress_length);
  if (!ret){
    THROW(env, "Ljava/lang/InternalError", "Could not get decompressed length.");
  }
  
  ret = snappy::RawUncompress(compressed_bytes, compressed_direct_buf_len, uncompressed_bytes);
  if (!ret){
    THROW(env, "Ljava/lang/InternalError", "Could not decompress data.");
  }

  env->SetIntField(thisj, SnappyDecompressor_compressedDirectBufLen, 0);
  
  return (jint)uncompress_length;
}
