// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.common.license;

import java.io.Closeable;
import org.apache.kafka.common.utils.Time;

public interface LicenseValidator extends Closeable {

  void initializeAndVerify(String license, String zkConnect, Time time, String metricGroup);

  void verifyLicense(boolean failOnError);
}
