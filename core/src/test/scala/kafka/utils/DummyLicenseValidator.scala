/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.utils

import java.util
import java.util.ServiceLoader

import org.apache.kafka.server.license.LicenseValidator
import scala.jdk.CollectionConverters._

// This dummy validator is enabled only in tests which don't have a dependency on `ce-licensing`
object DummyLicenseValidator {
  val enabled = !ServiceLoader.load(classOf[LicenseValidator]).asScala.toSet.exists { validator =>
    validator.getClass != classOf[DummyLicenseValidator] && validator.enabled
  }
}

class DummyLicenseValidator extends LicenseValidator {
  override def enabled(): Boolean = DummyLicenseValidator.enabled
  override def configure(configs: util.Map[String, _]): Unit = {}
  override def start(componentId: String): Unit = {}
  override def isLicenseValid: Boolean = true
  override def close(): Unit = {}
}
