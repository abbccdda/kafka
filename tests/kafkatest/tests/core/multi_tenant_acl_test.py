# Copyright 2019 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.cluster.remoteaccount import RemoteCommandError

from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.tests.produce_consume_validate import ProduceConsumeValidateTest
from kafkatest.utils import is_int

INTERCEPTOR_CLASS_PROP = "broker.interceptor.class"
PRINCIPAL_BUILDER_CLASS_PROP = "principal.builder.class"
SECRETS_DIR="/mnt/secrets"
APIKEYS_PATH="/mnt/secrets/apikeys.json"
LOGICAL_CLUSTER="lc1"
USER_API_KEY="user123"
USER_API_SECRET="user123-secret"
USER_UID="123"
SERVICE_API_KEY="service100"
SERVICE_API_SECRET="service100-secret"
SERVICE_UID="100"
APIKEYS="""{
  "keys": {
    "%(user_api_key)s": {
      "sasl_mechanism": "PLAIN",
      "hashed_secret": "%(user_api_secret)s",
      "hash_function": "none",
      "user_id": "%(user_uid)s",
      "logical_cluster_id": "%(logical_cluster)s",
      "service_account": "false"
    },
    "%(service_api_key)s": {
      "sasl_mechanism": "PLAIN",
      "hashed_secret": "%(service_api_secret)s",
      "hash_function": "none",
      "user_id": "%(service_uid)s",
      "logical_cluster_id": "%(logical_cluster)s",
      "service_account": "true"
    }
  }
}""" % {
  'logical_cluster' : LOGICAL_CLUSTER,
  'user_api_key' : USER_API_KEY,
  'user_api_secret' : USER_API_SECRET,
  'user_uid' : USER_UID,
  'service_api_key' : SERVICE_API_KEY,
  'service_api_secret' : SERVICE_API_SECRET,
  'service_uid' : SERVICE_UID,
}


def listener_prop(listener, prop):
    return "listener.name.%s.%s" % (listener, prop)


class MultiTenantAclTest(ProduceConsumeValidateTest, KafkaPathResolverMixin):

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(MultiTenantAclTest, self).__init__(test_context=test_context)
        self.context = test_context
        self.client_listener = "SASL_PLAINTEXT"
        self.broker_listener = "PLAINTEXT"

        # For now, we depend on auto-creation of this topic
        self.topic = "test_topic"
        self.consumer_group = "test_group"
        self.zk = ZookeeperService(test_context, num_nodes=1)

        server_jaas_config = """io.confluent.kafka.server.plugins.auth.FileBasedLoginModule required \
            config_path="{apikeys_path}" \
            refresh_ms="1000";""".format(apikeys_path=APIKEYS_PATH)

        self.kafka = KafkaService(test_context, num_nodes=1, zk=self.zk,
                                  security_protocol=self.client_listener,
                                  client_sasl_mechanism="PLAIN",
                                  interbroker_security_protocol=self.broker_listener,
                                  authorizer_class_name="io.confluent.kafka.multitenant.authorizer.MultiTenantAuthorizer",
                                  server_prop_overides=[
                                      [listener_prop("sasl_plaintext", INTERCEPTOR_CLASS_PROP),
                                       "io.confluent.kafka.multitenant.MultiTenantInterceptor"],
                                      [listener_prop("sasl_plaintext", PRINCIPAL_BUILDER_CLASS_PROP),
                                       "io.confluent.kafka.multitenant.MultiTenantPrincipalBuilder"],
                                      [listener_prop("sasl_plaintext", "plain.sasl.jaas.config"),
                                       server_jaas_config],
                                      ["super.users", "User:ANONYMOUS"],
                                      ["confluent.max.acls.per.tenant", "10000"]
                                  ])

        for node in self.kafka.nodes:
            node.account.mkdirs(SECRETS_DIR)
            node.account.create_file(APIKEYS_PATH, APIKEYS)

        user_jaas_config_variables = {
            'plain_client_user': USER_API_KEY,
            'plain_client_password': USER_API_SECRET
        }
        service_jaas_config_variables = {
            'plain_client_user': SERVICE_API_KEY,
            'plain_client_password': SERVICE_API_SECRET
        }

        self.producer = VerifiableProducer(test_context, 1, self.kafka, self.topic,
                                           jaas_override_variables=service_jaas_config_variables)

        self.consumer = ConsoleConsumer(self.test_context, 1,
                                        self.kafka, self.topic,
                                        group_id=self.consumer_group,
                                        consumer_timeout_ms=60000,
                                        message_validator=is_int,
                                        jaas_override_variables=user_jaas_config_variables)

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    def tearDown(self):
        for node in self.kafka.nodes:
            node.account.remove(SECRETS_DIR)
        super(MultiTenantAclTest, self).tearDown()

    def acls_command(self, node, username, password, command_args):
        bootstrap = "--bootstrap-server %s " % self.kafka.bootstrap_servers(self.client_listener)
        config_path = "%s/command.config" % SECRETS_DIR
        command_config = """security.protocol=SASL_PLAINTEXT
                            sasl.mechanism=PLAIN
                            sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";
                         """ % (username, password)
        node.account.create_file(config_path, command_config)
        args = "%s --command-config %s %s" % (bootstrap, config_path, command_args)
        cmd = "%s %s" % (self.path.script("kafka-acls.sh", node), args)
        output = node.account.ssh_capture(cmd, allow_fail=False)
        return "\n".join(output)

    def verify_topic_tenant_prefix(self, tenant_prefix):
        alltopics = set(self.kafka.list_topics(None))
        topics = [x for x in alltopics if not (x.startswith('__confluent') or
                                               x.startswith('_confluent') or
                                               x.startswith('__consumer_offsets'))]
        assert {"%s_%s" % (tenant_prefix, self.topic)} == set(topics), "Found unexpected topics: %s" % str(topics)


    def test_multi_tenant_authorizer(self):
        """
        Test scenario:
          - Broker with two listeners: PLAINTEXT (inter-broker) and SASL_PLAINTEXT (clients using SASL/PLAIN with API keys)
          - SASL_PLAINTEXT listener configured with multi-tenant interceptor
          - Broker configured with multi-tenant authorizer
          - Consumer using user account - all access granted without any ACLs
          - Producer using service account - access explicitly granted using ACLs to a specific topic
        """

        # Create ACLs for service account using credentials of user account
        service_principal = "User:%s" % SERVICE_UID
        add_args = "--add --topic=%s --producer --allow-principal=%s" % (self.topic, service_principal)
        self.acls_command(self.kafka.nodes[0], USER_API_KEY, USER_API_SECRET, add_args)
        describe_out = self.acls_command(self.kafka.nodes[0], USER_API_KEY, USER_API_SECRET, "--list")
        self.logger.debug("ACLs listed %s" % describe_out)
        error_message = "ACL not listed correctly: %s" % describe_out
        assert "name=test_topic" in describe_out, error_message
        assert "principal=%s" % service_principal in describe_out, error_message

        # Verify that service account cannot be use to describe ACLs
        try :
            describe_out = self.acls_command(self.kafka.nodes[0], SERVICE_API_KEY, SERVICE_API_SECRET, "--list")
            self.logger.error("kafka-acls.sh succeeded with service account: %s" % describe_out)
            raise RuntimeError("Service account without permission described ACLs successfully")
        except RemoteCommandError as e:
            self.logger.debug("kafka-acls.sh failed as expected with service account: %s" % e)

        # Verify that produce and consume work using service account with ACLs and user account without ACLs
        self.run_produce_consume_validate()

        # Verify that topic was created with tenant prefix
        self.verify_topic_tenant_prefix(LOGICAL_CLUSTER)

