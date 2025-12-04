import pika
import json
import os
from dotenv import load_dotenv
from elasticsearch_config import create_index, index_document, delete_all_plan_documents, index_document_with_routing

load_dotenv()

def flatten_and_index_plan(plan_data):
    """
    Index plan with proper parent-child relationships
    """
    plan_id = plan_data['objectId']
    
    # 1. Index main plan (parent)
    plan_doc = {
        'objectId': plan_data['objectId'],
        'objectType': plan_data['objectType'],
        '_org': plan_data['_org'],
        'planType': plan_data.get('planType'),
        'creationDate': plan_data.get('creationDate'),
        'plan_join': {
            'name': 'plan'  # This is the parent
        }
    }
    index_document_with_routing(plan_id, plan_doc, routing=plan_id)
    
    # 2. Index planCostShares (child of plan)
    cost_shares = plan_data['planCostShares']
    cost_doc = {
        'objectId': cost_shares['objectId'],
        'objectType': cost_shares['objectType'],
        '_org': cost_shares['_org'],
        'deductible': cost_shares['deductible'],
        'copay': cost_shares['copay'],
        'plan_join': {
            'name': 'planCostShares',
            'parent': plan_id  # Reference to parent
        }
    }
    index_document_with_routing(cost_shares['objectId'], cost_doc, routing=plan_id)
    
    # 3. Index linkedPlanServices
    for service in plan_data.get('linkedPlanServices', []):
        service_id = service['objectId']
        
        # Index linkedPlanService (child of plan)
        service_doc = {
            'objectId': service['objectId'],
            'objectType': service['objectType'],
            '_org': service['_org'],
            'plan_join': {
                'name': 'linkedPlanServices',
                'parent': plan_id
            }
        }
        index_document_with_routing(service_id, service_doc, routing=plan_id)
        
        # Index linkedService (child of linkedPlanService)
        linked_service = service['linkedService']
        linked_doc = {
            'objectId': linked_service['objectId'],
            'objectType': linked_service['objectType'],
            '_org': linked_service['_org'],
            'name': linked_service['name'],
            'plan_join': {
                'name': 'linkedService',
                'parent': service_id
            }
        }
        index_document_with_routing(linked_service['objectId'], linked_doc, routing=plan_id)
        
        # Index planserviceCostShares (child of linkedPlanService)
        plan_cost = service['planserviceCostShares']
        plan_cost_doc = {
            'objectId': plan_cost['objectId'],
            'objectType': plan_cost['objectType'],
            '_org': plan_cost['_org'],
            'deductible': plan_cost['deductible'],
            'copay': plan_cost['copay'],
            'plan_join': {
                'name': 'planserviceCostShares',
                'parent': service_id
            }
        }
        index_document_with_routing(plan_cost['objectId'], plan_cost_doc, routing=plan_id)
    
    print(f"✅ Indexed all objects with parent-child relationships for plan: {plan_id}")

def process_message(message):
    """Process a message from the queue"""
    operation = message['operation']
    data = message['data']
    
    if operation == "DELETE":
        plan_id = data['objectId']
        delete_all_plan_documents(plan_id)
    else:  # CREATE or UPDATE
        flatten_and_index_plan(data)

def callback(ch, method, properties, body):
    """Callback when message is received"""
    try:
        message = json.loads(body)
        print(f"\n📨 Received {message['operation']} message")
        
        process_message(message)
        
        # Acknowledge message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"✅ Message processed successfully\n")
        
    except Exception as e:
        print(f"❌ Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    """Start the consumer"""
    print("🚀 Starting RabbitMQ Consumer...\n")
    
    # Create Elasticsearch index
    print("Setting up Elasticsearch...")
    create_index()
    print()
    
    # Connect to RabbitMQ
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.getenv("RABBITMQ_HOST", "localhost"),
            port=int(os.getenv("RABBITMQ_PORT", 5672))
        )
    )
    channel = connection.channel()
    
    # Declare queue
    queue_name = os.getenv("RABBITMQ_QUEUE", "plan_queue")
    channel.queue_declare(queue=queue_name, durable=True)
    
    # Start consuming
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)
    
    print(f"👂 Listening for messages on queue: {queue_name}")
    print("Press CTRL+C to exit\n")
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
        channel.stop_consuming()
        connection.close()

if __name__ == '__main__':
    main()