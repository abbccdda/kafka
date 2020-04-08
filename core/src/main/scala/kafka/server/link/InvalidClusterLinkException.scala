/*
 Copyright 2020 Confluent Inc.
 */

package kafka.server.link

import org.apache.kafka.common.errors.ApiException

class InvalidClusterLinkException(message: String) extends ApiException(message)
