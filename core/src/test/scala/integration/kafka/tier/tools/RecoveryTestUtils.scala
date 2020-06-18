package kafka.tier.tools

import java.io.File
import java.io.PrintWriter

import kafka.tier.TopicIdPartition

object RecoveryTestUtils {
  def writeFencingFile(file: File, tpIdsToBeFenced: List[TopicIdPartition]): Unit = {
    val pw = new PrintWriter(file)
    tpIdsToBeFenced.foreach(tpid => {
      pw.write("%s,%s,%d".format(tpid.topicIdAsBase64(), tpid.topic(), tpid.partition()))
      pw.println()
    })
    pw.close()
  }
}
