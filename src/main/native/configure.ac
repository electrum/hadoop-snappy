#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(hadoop-snappy, 0.0.1, issaymk2@gmail.com)
AC_CONFIG_SRCDIR([src/SnappyCompressor.cc])
AC_CONFIG_AUX_DIR([config])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_HEADER([config.h])
AC_SYS_LARGEFILE
AM_INIT_AUTOMAKE([subdir-objects foreign no-dist])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_PROG_LIBTOOL

# Checks for libraries.

# Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS([stdio.h stddef.h], [], AC_MSG_ERROR(Some system headers not found... please ensure their presence on your platform.))
AC_LANG_PUSH([C++])
AC_CHECK_HEADERS([snappy.h], [], AC_MSG_ERROR([Native snappy headers not found.]))
AC_LANG_POP([C++])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_TYPE_SIZE_T

# Checks for library functions.
dnl Check for '-ljvm'
JNI_LDFLAGS=""
if test "x$JAVA_HOME" != "x"
then
JNI_LDFLAGS="-L$JAVA_HOME/jre/lib/$OS_ARCH/server"
fi
ldflags_bak=$LDFLAGS
LDFLAGS="$LDFLAGS $JNI_LDFLAGS"
AC_CHECK_LIB([jvm], [JNI_GetCreatedJavaVMs], [snappy])
LDFLAGS=$ldflags_bak
AC_SUBST([JNI_LDFLAGS])

dnl Check for JNI headers
JNI_CPPFLAGS=""
if test "x$JAVA_HOME" != "x"
then
        for dir in `find $JAVA_HOME/include -follow -type d`
        do
                JNI_CPPFLAGS="$JNI_CPPFLAGS -I$dir"
        done
fi
cppflags_bak=$CPPFLAGS
CPPFLAGS="$CPPFLAGS $JNI_CPPFLAGS"
AC_CHECK_HEADERS([jni.h], [], AC_MSG_ERROR([Native java headers not found. Is \$JAVA_HOME set correctly?]))
CPPFLAGS=$cppflags_bak
AC_SUBST([JNI_CPPFLAGS])


AC_CONFIG_FILES([Makefile])
AC_OUTPUT
