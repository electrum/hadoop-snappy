package com.hadoop.compression.snappy;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class TestSnappyCodec extends TestCase {
  private String inputDir;
  
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    inputDir = System.getProperty("test.build.data", "data");
  }
  
  public void testFile() throws Exception {
    run("test.txt");
  }
  
  private void run(String filename) throws FileNotFoundException, IOException{
    File inputFile = new File(inputDir, filename);
    File snappyFile = new File(inputDir, filename + new SnappyCodec().getDefaultExtension());
    if (snappyFile.exists()) {
      snappyFile.delete();
    }
    
    Configuration conf = new Configuration();
    CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(SnappyCodec.class, conf);
    
    // Compress
    FileInputStream is = new FileInputStream(inputFile);
    FileOutputStream os = new FileOutputStream(snappyFile);
    CompressionOutputStream cos = codec.createOutputStream(os);
    
    byte buffer[] = new byte[8192];
    try {
      int bytesRead = 0;
      while ((bytesRead = is.read(buffer)) > 0) {
        cos.write(buffer, 0, bytesRead);
      }
    } catch (IOException e) {
      System.err.println("Compress Error");
      e.printStackTrace();
    } finally {
      is.close();
      cos.close();
      os.close();
    }
    
    // Decompress
    is = new FileInputStream(inputFile);
    FileInputStream is2 = new FileInputStream(snappyFile);
    CompressionInputStream cis = codec.createInputStream(is2);
    BufferedReader r = new BufferedReader(new InputStreamReader(is));
    BufferedReader cr = new BufferedReader(new InputStreamReader(cis));
    
    
    try {
      String line, rline;
      int lineNum = 0;
      while ((line = r.readLine()) != null) {
        lineNum++;
        rline = cr.readLine();
        if (!rline.equals(line)) {
          System.err.println("Decompress error at line " + line + " of file " + filename);
          System.err.println("Original: [" + line + "]");
          System.err.println("Decompressed: [" + rline + "]");
        }
        assertEquals(rline, line);
      }
      assertNull(cr.readLine());
    } catch (IOException e) {
      System.err.println("Decompress Error");
      e.printStackTrace();
    } finally {
      cis.close();
      is.close();
      os.close();
    }
  }
}