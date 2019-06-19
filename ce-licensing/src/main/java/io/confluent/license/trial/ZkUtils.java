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

package io.confluent.license.trial;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.kafka.common.config.ConfigException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

// Minimal ZkUtils from `core` to avoid dependency on core from components using licensing
public class ZkUtils implements AutoCloseable {

  private final ZkClient zkClient;
  private final List<ACL> acls;
  private volatile boolean isNamespacePresent;

  public ZkUtils(String zkConnect, int sessionTimeoutMs, int connectionTimeout, boolean isSecure) {
    ZkConnection zkConnection = new ZkConnection(zkConnect, sessionTimeoutMs);
    this.zkClient = new ZkClient(zkConnection, connectionTimeout, new ZkStringSerializer());
    if (isSecure) {
      acls = new ArrayList<>();
      acls.addAll(ZooDefs.Ids.CREATOR_ALL_ACL);
      acls.addAll(ZooDefs.Ids.READ_ACL_UNSAFE);
    } else {
      acls = Collections.unmodifiableList(ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }
  }

  @Override
  public void close() {
    zkClient.close();
  }

  public void createPersistentPath(String path, String data) {
    try {
      createPersistent(path, data);
    } catch (ZkNoNodeException e) {
      createParentPath(path);
      createPersistent(path, data);
    }
  }

  public Stat readStat(String path) {
    Stat stat = new Stat();
    zkClient.readData(path, stat);
    return stat;
  }

  private void createParentPath(String path) {
    String parentDir = path.substring(0, path.lastIndexOf('/'));
    if (!parentDir.isEmpty()) {
      checkNamespace();
      zkClient.createPersistent(path, true, acls);
    }
  }

  private void createPersistent(String path, Object data) {
    checkNamespace();
    zkClient.createPersistent(path, data, acls);
  }

  private void checkNamespace() {
    if (!isNamespacePresent && !zkClient.exists("/")) {
      throw new ConfigException("Zookeeper namespace does not exist");
    }
    isNamespacePresent = true;
  }

  private static class ZkStringSerializer implements ZkSerializer {

    @Override
    public byte[] serialize(Object data) throws ZkMarshallingError {
      try {
        return ((String) data).getBytes("UTF-8");
      } catch (Exception e) {
        throw new ZkMarshallingError(e);
      }
    }

    @Override
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
      try {
        return bytes == null ? null : new String(bytes, "UTF-8");
      } catch (Exception e) {
        throw new ZkMarshallingError(e);
      }
    }
  }
}
