import os
import uuid
import logging

from typing import List
from dotenv import load_dotenv
from fastapi import FastAPI
from faker import Faker

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.producer import KafkaProducer

from commands import CreatePeopleCommands
from entities import Person

load_dotenv(vaerbose=True)

app = FastAPI()

@app.on_event('startup')
async def startyp_event():
    client = KafkaAdminClient(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'])
    topic = NewTopic(name=os.environ['TOPICS_PEOPLE_BASIC_NAME'],num_partitions=int(os.environ['TOPICS_PEOPLE_BASIC_PARTITIONS']),replication_factor=int(os.environ['TOPICS_PEOPLE_BASIC_REPLICAS']))

    try:
        client.create_topics([topic])
    except TopicAlreadyExistsError as e:
        logger.warning("Topic already exists")
    finally:
        client.close()