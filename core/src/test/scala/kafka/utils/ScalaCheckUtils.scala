package kafka.utils

import org.junit.Assert
import org.scalacheck.Test.Parameters
import org.scalacheck.{Prop, Test}

object ScalaCheckUtils {
  def assertProperty(p: Prop, params: Parameters = Parameters.defaultVerbose): Unit = {
    assertProperty(null, p, params)
  }

  def assertProperty(msg: String, p: Prop, params: Parameters): Unit = {
    Assert.assertTrue(msg, Test.check(params, p).passed)
  }
}
