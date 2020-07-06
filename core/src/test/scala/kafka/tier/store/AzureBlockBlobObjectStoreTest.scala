/*
 Copyright 2019 Confluent Inc.
 */
package kafka.tier.store

import java.io.{BufferedWriter, FileReader, FileWriter, IOException}

import kafka.utils.TestUtils
import org.junit.Test
import org.junit.Assert._

class AzureBlockBlobObjectStoreTest {
  @Test
  @throws(classOf[IOException])
  def testAzureCredentials(): Unit = {
    val testFile = TestUtils.tempFile()
    val bw = new BufferedWriter(new FileWriter(testFile))
    bw.write("{\"connectionString\" : \"value\"}\n")
    bw.close()
    val azureCreds = AzureBlockBlobTierObjectStore.readCredFilePath(new FileReader(testFile))
    assertEquals("value", azureCreds.getConnectionString)
  }
}
