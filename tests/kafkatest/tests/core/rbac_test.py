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
from ducktape.mark import matrix
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
import signal
import time

def clean_shutdown(test, node):
    test.kafka.stop_node(node)

def hard_shutdown(test, node):
    test.kafka.signal_node(node, sig=signal.SIGKILL)
    wait_until(lambda: len(test.kafka.pids(node)) == 0 and not test.kafka.is_registered(node),
           timeout_sec=test.kafka.zk_session_timeout + 5,
           err_msg="Failed to see timely deregistration of hard-killed broker %s" % str(node.account))

shutdown_actions = {
    "clean_bounce": clean_shutdown,
    "hard_bounce": hard_shutdown
}


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
                          authorizer_class_name="io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer",
                          server_prop_overides=[
                              ["super.users", "User:ANONYMOUS"],
                              ["confluent.authorizer.access.rule.providers", "ZK_ACL,FILE_RBAC"],
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
                          authorizer_class_name="io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer",
                          server_prop_overides=[
                              ["super.users", "User:ANONYMOUS"],
                              ["confluent.authorizer.access.rule.providers", "ZK_ACL,FILE_RBAC"],
                              ["ldap.java.naming.provider.url", self.minildap.ldap_url],
                              ["confluent.metadata.server.test.metadata.rbac.file", SecurityConfig.ROLES_PATH]
                          ])
        self.kafka.start()
        self.create_roles(self.kafka, "Group:" + RbacTest.CLIENT_GROUP)
        self.validate_access()

    @cluster(num_nodes=8)
    def test_simple_to_rbac_authorizer_upgrade(self):
        """
        Start with a cluster using SimpleAclAuthorizer. Upgrade brokers to ConfluentServerAuthorizer with RBAC
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
                          authorizer_class_name=KafkaService.SIMPLE_AUTHORIZER,
                          server_prop_overides=[
                              ["confluent.license.topic.replication.factor", "3"]
                          ])

        self.set_acls("User:" + SecurityConfig.SCRAM_CLIENT_USER)
        self.kafka.start()
        self.start_producer_and_consumer()

	server_prop_overides=[
	    ["confluent.authorizer.access.rule.providers", "ZK_ACL,FILE_RBAC"],
	    ["confluent.metadata.server.test.metadata.rbac.file", SecurityConfig.ROLES_PATH]
	]
        self.configure_rbac(server_prop_overides)
        self.bounce(delay_sec=10)
        self.run_validation(min_records=self.producer.num_acked+5000)
        self.verify_access_denied()

    @cluster(num_nodes=8)
    def test_ldap_to_rbac_authorizer_upgrade(self):
        """
        Start with a cluster using LdapAuthorizer. Upgrade brokers to ConfluentServerAuthorizer with RBAC
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
                              ["confluent.license.topic.replication.factor", "3"],
                              ["ldap.authorizer.java.naming.provider.url", self.minildap.ldap_url]
                          ])

        self.set_acls("Group:" + RbacTest.CLIENT_GROUP)
        self.kafka.start()
        self.start_producer_and_consumer()

	server_prop_overides=[
	    ["confluent.authorizer.access.rule.providers", "ZK_ACL,FILE_RBAC"],
	    ["confluent.metadata.server.test.metadata.rbac.file", SecurityConfig.ROLES_PATH],
	    ["ldap.java.naming.provider.url", self.minildap.ldap_url],
	]
        self.configure_rbac(server_prop_overides)
        self.bounce(delay_sec=10)
        self.run_validation(min_records=self.producer.num_acked+5000)
        self.verify_access_denied()

    @matrix(failure_mode=["clean_bounce", "hard_bounce"])
    def test_broker_failure(self, failure_mode):
        """
        Verify that producers and consumers using RBAC continue to work with broker failures
        and metadata service backend recovers on restart.
        """

        self.topic_config = {"partitions": 2, "replication-factor": 3, "min.insync.replicas": 2}
        self.create_zookeeper()
        self.zk.start()
        self.enable_ldap()

        self.create_kafka(num_nodes=3,
                          security_protocol="SASL_PLAINTEXT",
                          interbroker_security_protocol="PLAINTEXT",
                          client_sasl_mechanism="SCRAM-SHA-256",
                          interbroker_sasl_mechanism="PLAINTEXT",
                          authorizer_class_name="io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer",
                          server_prop_overides=[
                              ["super.users", "User:ANONYMOUS"],
                              ["confluent.authorizer.access.rule.providers", "ZK_ACL,FILE_RBAC"],
                              ["confluent.license.topic.replication.factor", "3"],
                              ["ldap.java.naming.provider.url", self.minildap.ldap_url],
                              ["confluent.metadata.server.test.metadata.rbac.file", SecurityConfig.ROLES_PATH]
                          ])
        self.kafka.start_concurrently()

        self.create_roles(self.kafka, "Group:" + RbacTest.CLIENT_GROUP)
        self.start_producer_and_consumer()
        self.bounce(shutdown_actions[failure_mode], verify_roles=True)
        self.run_validation(min_records=self.producer.num_acked+5000)


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
        role_file = SecurityConfig.ROLES_PATH
        roles = kafka.security_config.rbac_roles(kafka.cluster_id(), principal)
        self.logger.info("Creating RBAC roles: " + roles)
        for node in self.kafka.nodes:
            node.account.create_file(role_file, roles)
        wait_until(lambda:  self.role_updated(role_file) != True,
                   timeout_sec=30, backoff_sec=.2, err_msg="Role bindings file not deleted by broker.")

    def role_updated(self, role_file):
        # Role file is deleted by the writer node after update. Return true if file has been deleted on any node.
        for node in self.kafka.nodes:
            if file_exists(node, role_file) != True:
                return True
        return False

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
        self.acls.set_broker_acls(node, zk_connect, "User:ANONYMOUS")

        self.acls.acls_command(node, ACLs.produce_acl(zk_connect, self.topic, principal))
        self.acls.acls_command(node, ACLs.consume_acl(zk_connect, self.topic, self.group_id, principal))

    def configure_rbac(self, server_prop_overides):
        acls_cmd = "--authorizer-properties zookeeper.connect=%s --add --topic=_confluent-license " \
               "--topic=_confluent-metadata-auth --group=_confluent-metadata-coordinator-group " \
               "--producer --consumer --allow-principal=User:ANONYMOUS" % self.kafka.zk_connect_setting()
        self.acls.acls_command(self.kafka.nodes[0], acls_cmd)
	self.kafka.authorizer_class_name="io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer"
	self.kafka.server_prop_overides=server_prop_overides

    def bounce(self, shutdown_action=clean_shutdown, verify_roles=False, delay_sec=0):
        index = 0
        for node in self.kafka.nodes:
            shutdown_action(self, node)
            if verify_roles:
                self.create_roles(self.kafka, "User:test-user-%d" % index)

            self.kafka.start_node(node)
            if delay_sec > 0:
                time.sleep(delay_sec)
            index = index + 1

