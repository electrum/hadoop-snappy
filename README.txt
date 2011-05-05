Hadoop-Snappy enables Snappy compression for Hadoop.

  http://code.google.com/p/hadoop-snappy/

This project is preparing to integrate the code into Hadoop Common.

Build Hadoop Snappy
=====
1. Requirements: gcc c++, libtool, Java 6, JAVA_HOME set, Maven 3

2. Build/install Snappy (http://code.google.com/p/snappy/)

3. Build Hadoop Snappy

  $ mvn package

The build tarball is at target/hadoop-snappy-0.0.1-distro.tar.gz 

 
Install Hadoop Snappy in Hadoop
=====

1. Expand hadoop-snappy-0.0.1-distro.tar.gz file

Copy (recursively) the lib directory of the expanded tarball in 
the <HADOOP_HOME>/lib of all Hadoop nodes

  $ cp -r hadoop-snappy-0.0.1/lib/* <HADOOP_HOME>/lib

IMPORTANT: Hadoo Snappy 0.0.1 tarball includes Snappy native 
library (there is no need to install it the Hadoop nodes)

2. Add the following key/value pairs into core-site.xml

  <property>
    <name>io.compression.codecs</name>
    <value>
      org.apache.hadoop.io.compress.GzipCodec,
      org.apache.hadoop.io.compress.DefaultCodec,
      org.apache.hadoop.io.compress.BZip2Codec,
      org.apache.hadoop.io.compress.SnappyCodec
    </value>
  </property>

3. Restart Hadoop.

License
=======
Hadoop Snappy is licensed under the the Apache License, Version 2.0.

Origins
=======
This project is based on the Hadoop LZO codec classes.

The authors of the Hadoop LZO codec classes gave permission to 
relicense as the Apache License 2.0 (Todd Lipcon, Kevin Weil, 
Owen O'Malley, Hong Tang, Chris Douglas, Arun C Murthy)
Thanks!

  http://code.google.com/p/hadoop-gpl-compression
  https://github.com/kevinweil/hadoop-lzo
