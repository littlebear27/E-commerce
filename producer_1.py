from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
import pandas as pd
import json
from time import sleep,time
import concurrent.futures

def run_producer(cols, topic, producer):
    counter = 0
    for chunk in pd.read_csv('joined_data_set.csv',usecols=cols, chunksize=60):
        for _, row in chunk.iterrows():
            row_dict = row.to_dict()
            row_dict['current_time']=int(time()*1e6) # origonal was time
            data = json.dumps(row_dict, default=str).encode('utf-8')
            producer.send(topic=topic, key=str(counter).encode(), value=data)
            counter += 1
        sleep(0.2) #<--- change to tweak Dashboard # Optional: Introduce a small delay for better sequencing

# Kafka configuration
kafka_bootstrap_servers = '127.0.0.1:9092'

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

# List of topics
kafka_topics = ['order_items', 'order_payments','order_reviews','orders','customers']

files=[['order_id',
 'order_item_id',
 'product_id',
 'seller_id',
 'shipping_limit_date',
 'price',
 'freight_value'],
 ['order_id',
 'payment_sequential',
 'payment_type',
 'payment_installments',
 'payment_value'],
 ['review_id',
 'order_id',
 'review_score',
 'review_comment_title',
 'review_comment_message',
 'review_creation_date',
 'review_answer_timestamp'],
 ['order_id',
 'customer_id',
 'order_status',
 'order_purchase_timestamp',
 'order_approved_at',
 'order_delivered_carrier_date',
 'order_delivered_customer_date',
 'order_estimated_delivery_date'],
 ['customer_id',
 'customer_unique_id',
 'customer_zip_code_prefix',
 'customer_city',
 'customer_state']]



# Create a KafkaAdminClient
admin_client = KafkaAdminClient(bootstrap_servers=kafka_bootstrap_servers)

# Create topics if they do not exist
def create_topic_if_not_exists(topic_name):
    existing_topics = admin_client.list_topics()
    if topic_name not in existing_topics:
        new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])

        
for i in kafka_topics:
    create_topic_if_not_exists(i)

admin_client.close()
# Use ThreadPoolExecutor to run producers concurrently
with concurrent.futures.ThreadPoolExecutor() as executor:
    # Submit tasks to create topics concurrently
    # Submit tasks to run producers concurrently
    futures = [executor.submit(run_producer, file, topic, producer) for file, topic in zip(files, kafka_topics)]
    concurrent.futures.wait(futures)

producer.close()




#-------------------------------------------------------------------------------------------------------------------------------------------------

# from kafka.admin import KafkaAdminClient, NewTopic
# from kafka import KafkaProducer
# import pandas as pd
# import json
# from time import sleep

# def run_producer(file, topic, producer):
#     counter = 0
#     for chunk in pd.read_csv(file, chunksize=60):
#         for _, row in chunk.iterrows():
#             row_dict = row.to_dict()
#             data = json.dumps(row_dict, default=str).encode('utf-8')
#             producer.send(topic=topic, key=str(counter).encode(), value=data)
#             counter += 1
#         sleep(5)  # Optional: Introduce a small delay for better sequencing

# # Kafka configuration
# kafka_bootstrap_servers = 'localhost:9092'

# # Create a Kafka producer
# producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

# # List of files to process
# files = ['olist_order_items_dataset.csv', 'olist_order_payments_dataset.csv']

# # List of topics
# kafka_topics = ['seller1', 'newer']

# # Create a KafkaAdminClient
# admin_client = KafkaAdminClient(bootstrap_servers=kafka_bootstrap_servers)

# # Create topics if they do not exist
# for topic_name in kafka_topics:
#     existing_topics = admin_client.list_topics()
#     if topic_name not in existing_topics:
#         new_topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
#         admin_client.create_topics([new_topic])

# # Close the KafkaAdminClient
# admin_client.close()

# # Run the producer for each file and topic
# for file, topic in zip(files, kafka_topics):
#     run_producer(file, topic, producer)

# # Close the Kafka producer
# producer.close()






# from kafka.admin import KafkaAdminClient, NewTopic
# from kafka import KafkaProducer
# import pandas as pd
# import json
# from time import sleep

# producer_config = {
#     'bootstrap_servers': ['localhost:9092'],
#     'acks': 'all',  # Adjust the acknowledgment setting based on your requirement
#     # 'retries': 3,
#     # 'retry_backoff_ms': 1000,
#     # 'linger_ms': 10000,  # Adjust the linger time in milliseconds
#     # Add more configurations as needed
# }


# # counter = 0
# # for chunk in pd.read_csv(file, chunksize=60,nrows=600):
# #     for _, row in chunk.iterrows():
# #         row_dict = row.to_dict()
# #         data = json.dumps(row_dict, default=str).encode('utf-8')
# #         producer.send(topic='seller', key=str(counter).encode(), value=data)  # Use 'data' instead of 'csv_data'
# #         counter += 1
# #     sleep(10)
# def run_producer(file, topic, producer):
#     counter = 0
#     for chunk in pd.read_csv(file, chunksize=60):
#         for _, row in chunk.iterrows():
#             row_dict = row.to_dict()
#             data = json.dumps(row_dict, default=str).encode('utf-8')
#             producer.send(topic=topic,key=str(counter).encode(), value=data)
#             counter += 1
#         sleep(5)  # Optional: Introduce a small delay for better sequencing

# # Kafka configuration
# kafka_bootstrap_servers = 'your_kafka_broker'
# # kafka_topic = 'your_topic'

# # Create a Kafka producer
# producer = KafkaProducer(bootstrap_servers='localhost:9092')

# # List of files to process
# kafka_topic  = ['seller1', 'newer']
# admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')

# new_topic = NewTopic(name=kafka_topic[0], num_partitions=1, replication_factor=1)
# new_topic2 = NewTopic(name=kafka_topic[1], num_partitions=1, replication_factor=1)


# existing_topics = admin_client.list_topics()

# if kafka_topic[0] not in existing_topics:
#     admin_client.create_topics([new_topic])

# if kafka_topic[1] not in existing_topics:
#     admin_client.create_topics([new_topic2])

# # admin_client.create_topics([new_topic])

# # Close the KafkaAdminClient
# admin_client.close()

# files = ['olist_order_items_dataset.csv','olist_order_payments_dataset.csv']

# # Run the producer for each file

# run_producer(files[0], kafka_topic, producer)
# run_producer(files[1], kafka_topic, producer)


# # Close the Kafka producer
# producer.close()
