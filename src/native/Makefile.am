AM_CPPFLAGS = @JNI_CPPFLAGS@ -I$(HADOOP_NATIVE_SRCDIR)/src
AM_LDFLAGS = @JNI_LDFLAGS@
AM_CFLAGS = -g -Wall -fPIC -O2 -m$(JVM_DATA_MODEL)

lib_LTLIBRARIES = libhadoopsnappy.la

libhadoopsnappy_la_SOURCES = src/SnappyCompressor.cc src/SnappyDecompressor.cc
libhadoopsnappy_la_LIBADD = -lsnappy
libhadoopsnappy_la_LDFLAGS = -version-info 0:1:0
