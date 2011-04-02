/**
 * This file includes some common utilities 
 */
 
#if !defined HADOOP_SNAPPY_H
#define HADOOP_SNAPPY_H

#include <config.h>
#include <dlfcn.h>
#include <jni.h>


/* A helper macro to 'throw' a java exception. */ 
#define THROW(env, exception_name, message) \
  { \
	jclass ecls = env->FindClass(exception_name); \
	if (ecls) { \
	  env->ThrowNew(ecls, message); \
	  env->DeleteLocalRef(ecls); \
	} \
  }

/** 
 * A helper function to dlsym a 'symbol' from a given library-handle. 
 * 
 * @param env jni handle to report contingencies.
 * @param handle handle to the dlopen'ed library.
 * @param symbol symbol to load.
 * @return returns the address where the symbol is loaded in memory, 
 *         <code>NULL</code> on error.
 */

#define LOCK_CLASS(env, clazz, classname) \
  if (env->MonitorEnter(clazz) != 0) { \
    char exception_msg[128]; \
    snprintf(exception_msg, 128, "Failed to lock %s", classname); \
    THROW(env, "java/lang/InternalError", exception_msg); \
  }

#define UNLOCK_CLASS(env, clazz, classname) \
  if (env->MonitorExit(clazz) != 0) { \
    char exception_msg[128]; \
    snprintf(exception_msg, 128, "Failed to unlock %s", classname); \
    THROW(env, "java/lang/InternalError", exception_msg); \
  }

#endif

