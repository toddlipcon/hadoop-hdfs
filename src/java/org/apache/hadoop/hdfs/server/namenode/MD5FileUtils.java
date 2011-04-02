package org.apache.hadoop.hdfs.server.namenode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.util.StringUtils;

/**
 * Static functions for dealing with files of the same format
 * that the Unix "md5sum" utility writes.
 */
public abstract class MD5FileUtils {
  private static final Log LOG = LogFactory.getLog(
      MD5FileUtils.class);
  
  /**
   * Verify that the previously saved md5 for the given file matches
   * expectedMd5.
   * @throws IOException 
   */
  public static void verifySavedMD5(File dataFile, MD5Hash computedMd5)
      throws IOException {
    MD5Hash storedHash = readMD5ForFile(dataFile);
    // Check the hash itself
    String computedMd5String = StringUtils.byteToHexString(computedMd5.getDigest()); 
    if (!computedMd5String.equals(storedHash)) {
      throw new IOException(
          "File " + dataFile + " did not match stored MD5 checksum " +
          " (stored: " + storedHash + ", computed: " + computedMd5String);
    }
  }
  
  public static MD5Hash readMD5ForFile(File dataFile) throws IOException {
    File md5File = getDigestFileForFile(dataFile);

    String md5Line;
    
    BufferedReader reader =
      new BufferedReader(new FileReader(md5File));
    try {
      md5Line = reader.readLine().trim();
    } catch (IOException ioe) {
      throw new IOException("Error reading md5 file at " + md5File, ioe);
    } finally {
      IOUtils.cleanup(LOG, reader);
    }
    
    String[] fields = md5Line.split("\\s+", 2);
    if (fields.length != 2) {
      throw new IOException("Invalid MD5 file at " + md5File
          + " (incorrect number of fields)");
    }
    String storedHash = fields[0];
    File referencedFile = new File(fields[1]);

    // Sanity check: Make sure that the file referenced in the .md5 file at
    // least has the same name as the file we expect
    if (!referencedFile.getName().equals(dataFile.getName())) {
      throw new IOException(
          "MD5 file at " + md5File + " references file named " +
          referencedFile.getName() + " but we expected it to reference " +
          dataFile);
    }
    return new MD5Hash(storedHash);
  }

  /**
   * Save the ".md5" file that lists the md5sum of another file.
   * @param dataFile the original file whose md5 was computed
   * @param digest the computed digest
   * @throws IOException
   */
  public static void saveMD5File(File dataFile, MD5Hash digest)
      throws IOException {
    File md5File = getDigestFileForFile(dataFile);
    String digestString = StringUtils.byteToHexString(
        digest.getDigest());
    String md5Line = digestString + "\t" + dataFile.getName() + "\n";
    
    File md5FileTmp = new File(md5File.getParentFile(),
        md5File.getName() + ".tmp");
    
    boolean success = false;
    
    // Write to tmp file
    FileWriter writer = new FileWriter(md5FileTmp);
    try {
      writer.write(md5Line);
      success = true;
    } finally {
      IOUtils.cleanup(LOG, writer);
      if (!success) {
        md5FileTmp.delete();
      }
    }
    
    // Move tmp file into place
    if (!md5FileTmp.renameTo(md5File)) {
      if (!md5File.delete() || !md5FileTmp.renameTo(md5File)) {
        md5FileTmp.delete();
        throw new IOException(
            "Unable to rename " + md5FileTmp + " to " + md5File);
      }
    }
    
    LOG.debug("Saved MD5 " + digest + " to " + md5File);
  }

  public static File getDigestFileForFile(File file) {
    return new File(file.getParentFile(), file.getName() + ".md5");
  }
}
