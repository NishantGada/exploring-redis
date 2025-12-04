import pika
import os
import json
from dotenv import load_dotenv

load_dotenv()

def publish_to_queue(message_data, operation_type):
    """
    Simple function to publish a message to RabbitMQ
    message_data: the plan JSON
    operation_type: "CREATE", "UPDATE", or "DELETE"
    """
    try:
        # Connect to RabbitMQ
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=os.getenv("RABBITMQ_HOST", "localhost"),
                port=int(os.getenv("RABBITMQ_PORT", 5672))
            )
        )
        channel = connection.channel()
        
        # Declare queue (creates if doesn't exist)
        queue_name = os.getenv("RABBITMQ_QUEUE", "plan_queue")
        channel.queue_declare(queue=queue_name, durable=True)
        
        # Add operation type to message
        message = {
            "operation": operation_type,
            "data": message_data
        }
        
        # Publish message
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)  # Make persistent
        )
        
        print(f"✅ Published {operation_type} message to queue")
        connection.close()
        return True
        
    except Exception as e:
        print(f"❌ Error publishing to queue: {e}")
        return False