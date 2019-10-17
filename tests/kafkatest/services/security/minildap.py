# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from kafkatest.services.security.minikdc import MiniKdc
from kafkatest.version import DEV_BRANCH

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin, CORE_LIBS_JAR_NAME, CORE_DEPENDANT_TEST_LIBS_JAR_NAME, AUTH_PROVIDERS_JAR_NAME, AUTH_PROVIDERS_DEPENDANT_TEST_LIBS_JAR_NAME
from kafkatest.version import DEV_BRANCH

class MiniLdap(MiniKdc):

    def __init__(self, context, ldap_users=""):
        kafka_nodes = []
        super(MiniLdap, self).__init__(context, kafka_nodes)
        self.ldap_users = ldap_users
        self.ldap_port = 3268

    def start_node(self, node):
        node.account.ssh("mkdir -p %s" % MiniKdc.WORK_DIR, allow_fail=False)
        props_file = self.render('minildap.properties',  node=node, ldap_port=self.ldap_port)
        node.account.create_file(MiniKdc.PROPS_FILE, props_file)
        self.logger.info("minildap.properties")
        self.logger.info(props_file)

        user_metadata = ""
        for principal in self.ldap_users:
          metadata = self.ldap_users[principal]
          user_metadata = "%s %s:%s:%s" % (user_metadata, principal, metadata["password"], metadata["groups"])

        self.logger.info("Starting MiniLdap with principals:groups %s" % user_metadata)

        core_libs_jar = self.path.jar(CORE_LIBS_JAR_NAME, DEV_BRANCH)
        core_dependant_test_libs_jar = self.path.jar(CORE_DEPENDANT_TEST_LIBS_JAR_NAME, DEV_BRANCH)
	auth_providers_libs_jar = self.path.jar(AUTH_PROVIDERS_JAR_NAME, DEV_BRANCH)
	auth_providers_dependant_test_libs_jar = self.path.jar(AUTH_PROVIDERS_DEPENDANT_TEST_LIBS_JAR_NAME, DEV_BRANCH)

        cmd = "for file in %s; do CLASSPATH=$CLASSPATH:$file; done;" % core_dependant_test_libs_jar
        cmd += " for file in %s; do CLASSPATH=$CLASSPATH:$file; done;" % auth_providers_libs_jar
        cmd += " for file in %s; do CLASSPATH=$CLASSPATH:$file; done;" % auth_providers_dependant_test_libs_jar
        cmd += " for file in %s; do CLASSPATH=$CLASSPATH:$file; done;" % core_libs_jar
        cmd += " export CLASSPATH; "
        cmd += " %s io.confluent.security.minikdc.MiniKdcWithLdapService %s %s %s %s 1>> %s 2>> %s &" % (self.path.script("kafka-run-class.sh", node), MiniKdc.WORK_DIR, MiniKdc.PROPS_FILE, MiniKdc.KEYTAB_FILE, user_metadata, MiniKdc.LOG_FILE, MiniKdc.LOG_FILE)
        self.logger.debug("Attempting to start MiniLdap on %s with command: %s" % (str(node.account), cmd))
        with node.account.monitor_log(MiniKdc.LOG_FILE) as monitor:
            node.account.ssh(cmd)
            monitor.wait_until("MiniLdapServer Running", timeout_sec=60, backoff_sec=1, err_msg="MiniLdap didn't finish startup")

        node.account.copy_from(MiniKdc.KEYTAB_FILE, MiniKdc.LOCAL_KEYTAB_FILE)
        node.account.copy_from(MiniKdc.KRB5CONF_FILE, MiniKdc.LOCAL_KRB5CONF_FILE)

        # KDC is set to bind openly (via 0.0.0.0). Change krb5.conf to hold the specific KDC address
        self.replace_in_file(MiniKdc.LOCAL_KRB5CONF_FILE, '0.0.0.0', node.account.hostname)

        self.ldap_url = "ldap://%s:%s/DC=EXAMPLE,DC=COM" % (node.account.hostname, self.ldap_port)
        self.logger.info("LDAP provider url is %s" % self.ldap_url)

