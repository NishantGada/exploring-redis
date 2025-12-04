import pika
import json
import os
from dotenv import load_dotenv
from elasticsearch_config import (
    create_index,
    index_document,
    delete_all_plan_documents,
    index_document_with_routing,
    es,
    INDEX_NAME,
)

load_dotenv()


def flatten_and_index_plan(plan_data):
    """
    Index plan with proper parent-child relationships
    """
    plan_id = plan_data["objectId"]
    print(f"📝 Indexing plan: {plan_id}")

    try:
        # 1. Index main plan (parent)
        plan_doc = {
            "objectId": plan_data["objectId"],
            "objectType": plan_data["objectType"],
            "_org": plan_data["_org"],
            "planType": plan_data.get("planType"),
            "creationDate": plan_data.get("creationDate"),
            "plan_join": {"name": "plan"},
        }
        index_document_with_routing(plan_id, plan_doc, routing=plan_id)
        print(f"  ✅ Indexed parent plan: {plan_id}")

        # 2. Index planCostShares (child of plan)
        cost_shares = plan_data["planCostShares"]
        cost_doc = {
            "objectId": cost_shares["objectId"],
            "objectType": cost_shares["objectType"],
            "_org": cost_shares["_org"],
            "deductible": cost_shares["deductible"],
            "copay": cost_shares["copay"],
            "plan_join": {"name": "planCostShares", "parent": plan_id},
        }
        index_document_with_routing(cost_shares["objectId"], cost_doc, routing=plan_id)
        print(f"  ✅ Indexed planCostShares: {cost_shares['objectId']}")

        # 3. Index linkedPlanServices and their children
        service_count = 0
        for service in plan_data.get("linkedPlanServices", []):
            service_id = service["objectId"]

            # Index linkedPlanService (child of plan)
            service_doc = {
                "objectId": service["objectId"],
                "objectType": service["objectType"],
                "_org": service["_org"],
                "plan_join": {"name": "linkedPlanServices", "parent": plan_id},
            }
            index_document_with_routing(service_id, service_doc, routing=plan_id)
            print(f"  ✅ Indexed linkedPlanServices: {service_id}")

            # Index linkedService (child of linkedPlanService)
            linked_service = service["linkedService"]
            linked_doc = {
                "objectId": linked_service["objectId"],
                "objectType": linked_service["objectType"],
                "_org": linked_service["_org"],
                "name": linked_service["name"],
                "plan_join": {"name": "linkedService", "parent": service_id},
            }
            index_document_with_routing(
                linked_service["objectId"], linked_doc, routing=plan_id
            )
            print(f"    ✅ Indexed linkedService: {linked_service['objectId']}")

            # Index planserviceCostShares (child of linkedPlanService)
            plan_cost = service["planserviceCostShares"]
            plan_cost_doc = {
                "objectId": plan_cost["objectId"],
                "objectType": plan_cost["objectType"],
                "_org": plan_cost["_org"],
                "deductible": plan_cost["deductible"],
                "copay": plan_cost["copay"],
                "plan_join": {"name": "planserviceCostShares", "parent": service_id},
            }
            index_document_with_routing(
                plan_cost["objectId"], plan_cost_doc, routing=plan_id
            )
            print(f"    ✅ Indexed planserviceCostShares: {plan_cost['objectId']}")

            service_count += 1

        # Force refresh to make changes visible immediately
        es.indices.refresh(index=INDEX_NAME)

        print(f"✅ Successfully indexed plan {plan_id} with {service_count} services")
        print(f"🔄 Index refreshed - changes visible immediately\n")

    except Exception as e:
        print(f"❌ Error indexing plan {plan_id}: {e}")
        raise


def process_message(message):
    """Process a message from the queue"""
    operation = message["operation"]

    if operation == "DELETE":
        # Handle both formats: {'data': {'objectId': 'xxx'}} or {'plan_id': 'xxx'}
        if "plan_id" in message:
            plan_id = message["plan_id"]
        elif "data" in message and "objectId" in message["data"]:
            plan_id = message["data"]["objectId"]
        else:
            raise ValueError("DELETE message missing plan_id or data.objectId")

        print(f"🗑️  Deleting plan: {plan_id}")
        delete_all_plan_documents(plan_id)

        # Force refresh after delete
        es.indices.refresh(index=INDEX_NAME)
        print(f"✅ Successfully deleted plan {plan_id}")
        print(f"🔄 Index refreshed - changes visible immediately\n")

    else:  # CREATE or UPDATE
        data = message["data"]
        flatten_and_index_plan(data)


def callback(ch, method, properties, body):
    """Callback when message is received"""
    try:
        message = json.loads(body)
        operation = message.get("operation", "UNKNOWN")
        print(f"\n{'='*60}")
        print(f"📨 Received {operation} message")
        print(f"{'='*60}")

        process_message(message)

        # Acknowledge message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print(f"✅ Message acknowledged\n")

    except Exception as e:
        print(f"\n❌ ERROR processing message: {e}")
        print(f"Message body: {body}\n")
        # Don't requeue failed messages
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    """Start the consumer"""
    print("\n" + "=" * 60)
    print("🚀 Starting RabbitMQ Consumer for Demo 3")
    print("=" * 60 + "\n")

    # Create Elasticsearch index
    print("Setting up Elasticsearch...")
    create_index()
    print()

    # Connect to RabbitMQ
    print("Connecting to RabbitMQ...")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.getenv("RABBITMQ_HOST", "localhost"),
            port=int(os.getenv("RABBITMQ_PORT", 5672)),
        )
    )
    channel = connection.channel()
    print("✅ Connected to RabbitMQ\n")

    # Declare queue
    queue_name = os.getenv("RABBITMQ_QUEUE", "plan_queue")
    channel.queue_declare(queue=queue_name, durable=True)
    print(f"✅ Queue declared: {queue_name}\n")

    # Start consuming
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=callback)

    print("=" * 60)
    print(f"👂 Listening for messages on queue: {queue_name}")
    print("Press CTRL+C to exit")
    print("=" * 60 + "\n")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\n" + "=" * 60)
        print("👋 Shutting down consumer...")
        print("=" * 60)
        channel.stop_consuming()
        connection.close()
        print("✅ Consumer stopped cleanly\n")


if __name__ == "__main__":
    main()
