from kafka.admin import KafkaAdminClient, NewTopic

def create_topics():
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:9092",
        client_id='stock-analysis-admin'
    )
    
    # Define topics
    topic_list = [
        NewTopic(name="raw-stock-data", num_partitions=3, replication_factor=1),
        NewTopic(name="processed-stock-metrics", num_partitions=3, replication_factor=1),
        NewTopic(name="market-alerts", num_partitions=3, replication_factor=1)
    ]
    
    # Create topics
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print("Topics created successfully")
    except Exception as e:
        print(f"Failed to create topics: {e}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    create_topics()
