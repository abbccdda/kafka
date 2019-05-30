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

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.kafka import KafkaService
from kafkatest.services.security.kafka_acls import ACLs
from kafkatest.services.security.minildap import MiniLdap
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.tests.end_to_end import EndToEndTest
from kafkatest.utils.remote_account import file_exists
import time

class RbacTest(EndToEndTest, KafkaPathResolverMixin):
    """
    These tests validate role-based authorization.
    Since these tests are run without a real Metadata Server, role bindings are
    initialized using a file containing role bindings.
    """

    CLIENT_GROUP = "TestClients"

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(RbacTest, self).__init__(test_context=test_context)
        self.context = test_context
        self.group_id = "test_group"

    @cluster(num_nodes=5)
    def test_rbac(self):
        """
        Test role-based authorization.
        """

        self.create_zookeeper()
        self.zk.start()

        self.create_kafka(num_nodes=1,
                          security_protocol="SASL_PLAINTEXT",
                          interbroker_security_protocol="PLAINTEXT",
                          client_sasl_mechanism="SCRAM-SHA-256",
                          interbroker_sasl_mechanism="PLAINTEXT",
                          authorizer_class_name="io.confluent.kafka.security.authorizer.ConfluentKafkaAuthorizer",
                          server_prop_overides=[
                              ["super.users", "User:ANONYMOUS"],
                              ["confluent.authorizer.access.rule.providers", "ACL,FILE_RBAC"],
                              ["confluent.authorizer.metadata.provider", "FILE_RBAC"],
                              ["confluent.metadata.server.test.metadata.rbac.file", SecurityConfig.ROLES_PATH]
                          ])
        self.kafka.start()
        self.create_roles(self.kafka, "User:" + SecurityConfig.SCRAM_CLIENT_USER)
        self.validate_access()

    @cluster(num_nodes=6)
    def test_rbac_with_ldap(self):
        """
        Test role-based authorization with LDAP groups.
        """

        self.create_zookeeper()
        self.zk.start()
        self.enable_ldap()

        self.create_kafka(num_nodes=1,
                          security_protocol="SASL_PLAINTEXT",
                          interbroker_security_protocol="PLAINTEXT",
                          client_sasl_mechanism="SCRAM-SHA-256",
                          interbroker_sasl_mechanism="PLAINTEXT",
                          authorizer_class_name="io.confluent.kafka.security.authorizer.ConfluentKafkaAuthorizer",
                          server_prop_overides=[
                              ["super.users", "User:ANONYMOUS"],
                              ["confluent.authorizer.access.rule.providers", "ACL,FILE_RBAC"],
                              ["confluent.authorizer.metadata.provider", "FILE_RBAC"],
                              ["confluent.authorizer.group.provider", "FILE_RBAC"],
                              ["ldap.java.naming.provider.url", self.minildap.ldap_url],
                              ["confluent.metadata.server.test.metadata.rbac.file", SecurityConfig.ROLES_PATH]
                          ])
        self.kafka.start()
        self.create_roles(self.kafka, "Group:" + RbacTest.CLIENT_GROUP)
        self.validate_access()

    @cluster(num_nodes=8)
    def test_simple_to_rbac_authorizer_upgrade(self):
        """
        Start with a cluster using SimpleAclAuthorizer. Upgrade brokers to ConfluentKafkaAuthorizer with RBAC
        using rolling upgrade and ensure we could produce and consume throughout.
        """

        self.topic_config = {"partitions": 2, "replication-factor": 3, "min.insync.replicas": 2}
        self.create_zookeeper()
        self.zk.start()
        self.acls = ACLs(self.test_context)

        self.create_kafka(num_nodes=3,
                          security_protocol="SASL_PLAINTEXT",
                          interbroker_security_protocol="PLAINTEXT",
                          client_sasl_mechanism="SCRAM-SHA-256",
                          interbroker_sasl_mechanism="PLAINTEXT",
                          authorizer_class_name=KafkaService.SIMPLE_AUTHORIZER)

        self.set_acls("User:" + SecurityConfig.SCRAM_CLIENT_USER)
        self.kafka.start()
        self.start_producer_and_consumer()

	server_prop_overides=[
	    ["confluent.authorizer.access.rule.providers", "ACL,FILE_RBAC"],
	    ["confluent.authorizer.metadata.provider", "FILE_RBAC"],
	    ["confluent.metadata.server.test.metadata.rbac.file", SecurityConfig.ROLES_PATH]
	]
        self.configure_rbac(server_prop_overides)
        self.clean_bounce(delay_sec=10)
        self.run_validation(min_records=self.producer.num_acked+5000)
        self.verify_access_denied()

    @cluster(num_nodes=8)
    def test_ldap_to_rbac_authorizer_upgrade(self):
        """
        Start with a cluster using LdapAuthorizer. Upgrade brokers to ConfluentKafkaAuthorizer with RBAC
        and LDAP using rolling upgrade and ensure we could produce and consume throughout.
        """

        self.topic_config = {"partitions": 2, "replication-factor": 3, "min.insync.replicas": 2}
        self.create_zookeeper()
        self.zk.start()
        self.enable_ldap()
        self.acls = ACLs(self.test_context)

        self.create_kafka(num_nodes=3,
                          security_protocol="SASL_PLAINTEXT",
                          interbroker_security_protocol="PLAINTEXT",
                          client_sasl_mechanism="SCRAM-SHA-256",
                          interbroker_sasl_mechanism="PLAINTEXT",
                          authorizer_class_name="io.confluent.kafka.security.ldap.authorizer.LdapAuthorizer",
                          server_prop_overides=[
                              ["ldap.authorizer.java.naming.provider.url", self.minildap.ldap_url]
                          ])

        self.set_acls("Group:" + RbacTest.CLIENT_GROUP)
        self.kafka.start()
        self.start_producer_and_consumer()

	server_prop_overides=[
	    ["confluent.authorizer.access.rule.providers", "ACL,FILE_RBAC"],
	    ["confluent.authorizer.group.provider", "FILE_RBAC"],
	    ["confluent.authorizer.metadata.provider", "FILE_RBAC"],
	    ["confluent.metadata.server.test.metadata.rbac.file", SecurityConfig.ROLES_PATH],
	    ["ldap.java.naming.provider.url", self.minildap.ldap_url],
	]
        self.configure_rbac(server_prop_overides)
        self.clean_bounce(delay_sec=10)
        self.run_validation(min_records=self.producer.num_acked+5000)
        self.verify_access_denied()


    def start_producer_and_consumer(self):
        self.create_producer(log_level="INFO")
        self.producer.start()
        self.create_consumer(log_level="INFO", group_id=self.group_id)
        self.consumer.start()

    def validate_access(self):
        self.start_producer_and_consumer()
        self.run_validation()
        self.verify_access_denied()

    def enable_ldap(self):
        ldap_users = {SecurityConfig.SCRAM_CLIENT_USER : { "groups" : RbacTest.CLIENT_GROUP, "password" : SecurityConfig.SCRAM_CLIENT_PASSWORD } }
        self.minildap = MiniLdap(self.test_context, ldap_users)
        self.minildap.start()

    def create_roles(self, kafka, principal):
        node = self.kafka.nodes[0]

        role_file = SecurityConfig.ROLES_PATH
        roles = kafka.security_config.rbac_roles(kafka.cluster_id(), principal)
        node.account.create_file(role_file, roles)
        self.logger.info("Creating RBAC roles: " + roles)
        wait_until(lambda:  file_exists(node, role_file) != True,
                   timeout_sec=30, backoff_sec=.2, err_msg="Role bindings file not deleted by broker.")

    def verify_access_denied(self):
        try :
            node = self.producer.nodes[0]
            cmd = "%s --bootstrap-server %s --command-config %s --list" % \
                  (self.path.script("kafka-acls.sh", node),
                   self.kafka.bootstrap_servers("SASL_PLAINTEXT"),
                   VerifiableProducer.CONFIG_FILE)
            output = node.account.ssh_capture(cmd, allow_fail=False)
            self.logger.error("kafka-acls.sh succeeded without permissions: " + "\n".join(output))
            raise RuntimeError("User without permission described ACLs successfully")
        except RemoteCommandError as e:
            self.logger.debug("kafka-acls.sh failed as expected with kafka-client: %s" % e)

    def set_acls(self, principal):
        zk_connect = self.kafka.zk_connect_setting()
        node = self.kafka.nodes[0]
        self.acls.acls_command(node, ACLs.add_cluster_acl(zk_connect, "User:ANONYMOUS"))
        self.acls.acls_command(node, ACLs.broker_read_acl(zk_connect, "*", "User:ANONYMOUS"))
        self.acls.acls_command(node, ACLs.produce_acl(zk_connect, self.topic, principal))
        self.acls.acls_command(node, ACLs.consume_acl(zk_connect, self.topic, self.group_id, principal))

    def configure_rbac(self, server_prop_overides):
        acls_cmd = "--authorizer-properties zookeeper.connect=%s --add --topic=_confluent-license " \
               "--topic=_confluent-metadata-auth --group=_confluent-metadata-coordinator-group " \
               "--producer --consumer --allow-principal=User:ANONYMOUS" % self.kafka.zk_connect_setting()
        self.acls.acls_command(self.kafka.nodes[0], acls_cmd)
	self.kafka.authorizer_class_name="io.confluent.kafka.security.authorizer.ConfluentKafkaAuthorizer"
	self.kafka.server_prop_overides=server_prop_overides

    def clean_bounce(self, delay_sec=0):
        for node in self.kafka.nodes:
            self.kafka.stop_node(node)
            self.kafka.start_node(node)
            time.sleep(delay_sec)

