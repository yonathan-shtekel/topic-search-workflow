from typing import List
from confluent_kafka.admin import AdminClient, ClusterMetadata, TopicMetadata


class KafkaAdmin:
    def __init__(self,
                 bootstrap_servers: str,
                 sasl_username: str,
                 sasl_password: str
                 ):
        self._conf = {
            'bootstrap.servers': bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': sasl_username,
            'sasl.password': sasl_password
        }

        self._admin_client = None

    def start(self):
        self._admin_client = AdminClient(self._conf)

    def get_partitions_ids_by_topic(self, topic: str) -> List[int]:
        # Get topic metadata with offsets

        cluster_meta: ClusterMetadata = self._admin_client.list_topics()

        topic_meta: TopicMetadata = cluster_meta.topics.get(topic)

        if topic_meta is None:
            raise ValueError(f"Topic {topic} not found")

        partitions_ids = [partition for partition in topic_meta.partitions]

        return partitions_ids
