# Please complete the TODO items in the code

import json

import requests


REST_PROXY_URL = "http://localhost:8082"


def get_topics():
    """Gets topics from REST Proxy"""
    # TODO: See: https://docs.confluent.io/current/kafka-rest/api.html#get--topics
    resp = requests.get(f"{REST_PROXY_URL}/topics")  # TODO

    try:
        resp.raise_for_status()
    except:
        print("Failed to get topics {json.dumps(resp.json(), indent=2)})")
        return []

    print("Fetched topics from Kafka:")
    print(json.dumps(resp.json(), indent=2))
    return resp.json()


def get_topic(topic_name):
    """Get specific details on a topic"""
    # TODO: See: https://docs.confluent.io/current/kafka-rest/api.html#get--topics
    resp = requests.get(f"{REST_PROXY_URL}/topics/{topic_name}")  # TODO

    try:
        resp.raise_for_status()
    except:
        print("Failed to get topics {json.dumps(resp.json(), indent=2)})")

    print("Fetched topics from Kafka:")
    print(json.dumps(resp.json(), indent=2))


def get_brokers():
    """Gets broker information"""
    # TODO See: https://docs.confluent.io/current/kafka-rest/api.html#get--brokers
    resp = requests.get(f"{REST_PROXY_URL}/brokers")  # TODO

    try:
        resp.raise_for_status()
    except:
        print("Failed to get brokers {json.dumps(resp.json(), indent=2)})")

    print("Fetched brokers from Kafka:")
    print(json.dumps(resp.json(), indent=2))


def get_partitions(topic_name):
    """Prints partition information for a topic"""
    # TODO: Using the above endpoints as an example, list
    #       partitions for a given topic name using the API
    #
    #       See: https://docs.confluent.io/current/kafka-rest/api.html#get--topics-(string-topic_name)-partitions
    resp = requests.get(f"{REST_PROXY_URL}/topics/{topic_name}/partitions")
    
    try:
        resp.raise_for_status()
    except:
        print("Failed to get partitions {json.dumps{resp.json(), indent=2}}")
        
    print("Fetched partitions from Kafka:")
    print(json.dumps(resp.json(), indent=2))

if __name__ == "__main__":
    topics = get_topics()
    get_topic(topics[0])
    get_brokers()
    get_partitions(topics[-1])
"""
etched topics from Kafka:
[
  "__confluent.support.metrics",
  "_confluent-ksql-default__command_topic",
  "_confluent-metrics",
  "_schemas",
  "com.udacity.streams.clickevents",
  "com.udacity.streams.pages",
  "com.udacity.streams.purchases",
  "com.udacity.streams.users",
  "connect-configs",
  "connect-offsets",
  "connect-status"
]
Fetched topics from Kafka:
{
  "name": "__confluent.support.metrics",
  "configs": {
    "message.downconversion.enable": "true",
    "file.delete.delay.ms": "60000",
    "segment.ms": "604800000",
    "min.compaction.lag.ms": "0",
    "retention.bytes": "-1",
    "segment.index.bytes": "10485760",
    "cleanup.policy": "delete",
    "follower.replication.throttled.replicas": "",
    "message.timestamp.difference.max.ms": "9223372036854775807",
    "segment.jitter.ms": "0",
    "preallocate": "false",
    "message.timestamp.type": "CreateTime",
    "message.format.version": "2.1-IV2",
    "segment.bytes": "1073741824",
    "unclean.leader.election.enable": "false",
    "max.message.bytes": "1000012",
    "retention.ms": "31536000000",
    "flush.ms": "9223372036854775807",
    "delete.retention.ms": "86400000",
    "leader.replication.throttled.replicas": "",
    "min.insync.replicas": "1",
    "flush.messages": "9223372036854775807",
    "compression.type": "producer",
    "index.interval.bytes": "4096",
    "min.cleanable.dirty.ratio": "0.5"
  },
  "partitions": [
    {
      "partition": 0,
      "leader": 1,
      "replicas": [
        {
          "broker": 1,
          "leader": true,
          "in_sync": true
        }
      ]
    }
  ]
}
Fetched brokers from Kafka:
{
  "brokers": [
    1
  ]
}
Traceback (most recent call last):
  File "exercise4.4.py", line 75, in <module>
    get_partitions(topics[-1])
  File "exercise4.4.py", line 61, in get_partitions
    resp = request.get(f"{REST_PROXY_URL}/topics/{topic_name}/partitions")
NameError: name 'request' is not defined
root@fcdcc04a373a:/home/workspace# python exercise4.4.py
Fetched topics from Kafka:
[
  "__confluent.support.metrics",
  "_confluent-ksql-default__command_topic",
  "_confluent-metrics",
  "_schemas",
  "com.udacity.streams.clickevents",
  "com.udacity.streams.pages",
  "com.udacity.streams.purchases",
  "com.udacity.streams.users",
  "connect-configs",
  "connect-offsets",
  "connect-status"
]
Fetched topics from Kafka:
{
  "name": "__confluent.support.metrics",
  "configs": {
    "message.downconversion.enable": "true",
    "file.delete.delay.ms": "60000",
    "segment.ms": "604800000",
    "min.compaction.lag.ms": "0",
    "retention.bytes": "-1",
    "segment.index.bytes": "10485760",
    "cleanup.policy": "delete",
    "follower.replication.throttled.replicas": "",
    "message.timestamp.difference.max.ms": "9223372036854775807",
    "segment.jitter.ms": "0",
    "preallocate": "false",
    "message.timestamp.type": "CreateTime",
    "message.format.version": "2.1-IV2",
    "segment.bytes": "1073741824",
    "unclean.leader.election.enable": "false",
    "max.message.bytes": "1000012",
    "retention.ms": "31536000000",
    "flush.ms": "9223372036854775807",
    "delete.retention.ms": "86400000",
    "leader.replication.throttled.replicas": "",
    "min.insync.replicas": "1",
    "flush.messages": "9223372036854775807",
    "compression.type": "producer",
    "index.interval.bytes": "4096",
    "min.cleanable.dirty.ratio": "0.5"
  },
  "partitions": [
    {
      "partition": 0,
      "leader": 1,
      "replicas": [
        {
          "broker": 1,
          "leader": true,
          "in_sync": true
        }
      ]
    }
  ]
}
Fetched brokers from Kafka:
{
  "brokers": [
    1
  ]
}
Fetched partitions from Kafka:
[
  {
    "partition": 0,
    "leader": 1,
    "replicas": [
      {
        "broker": 1,
        "leader": true,
        "in_sync": true
      }
    ]
  },
  {
    "partition": 1,
    "leader": 1,
    "replicas": [
      {
        "broker": 1,
        "leader": true,
        "in_sync": true
      }
    ]
  },
  {
    "partition": 2,
    "leader": 1,
    "replicas": [
      {
        "broker": 1,
        "leader": true,
        "in_sync": true
      }
    ]
  },
  {
    "partition": 3,
    "leader": 1,
    "replicas": [
      {
        "broker": 1,
        "leader": true,
        "in_sync": true
      }
    ]
  },
  {
    "partition": 4,
    "leader": 1,
    "replicas": [
      {
        "broker": 1,
        "leader": true,
        "in_sync": true
      }
    ]
  }
]
root@fcdcc04a373a:/home/workspace# kafka-topics --list --zookeeper localhost:2181
__confluent.support.metrics
__consumer_offsets
_confluent-ksql-default__command_topic
_confluent-metrics
_schemas
com.udacity.streams.clickevents
com.udacity.streams.pages
com.udacity.streams.purchases
com.udacity.streams.users
connect-configs
connect-offsets
connect-status
root@fcdcc04a373a:/home/workspace# 

"""