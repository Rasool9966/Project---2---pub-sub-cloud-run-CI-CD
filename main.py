import base64
import io
import json
import avro.schema
import avro.io
from google.cloud import pubsub_v1

PROJECT_ID = "northern-cooler-464505-t9"
SCHEMA_ID = "e_comm"

def get_avro_schema():
    """Fetch Avro schema from Pub/Sub schema registry."""
    client = pubsub_v1.SchemaServiceClient()
    schema_path = client.schema_path(PROJECT_ID, SCHEMA_ID)
    schema = client.get_schema(request={"name": schema_path})
    return avro.schema.parse(schema.definition)

avro_schema = get_avro_schema()

def process_pubsub(request):
    """HTTP-triggered entry point for Pub/Sub push subscription."""
    envelope = request.get_json()

    if not envelope or "message" not in envelope:
        return ("Bad Request: No message", 400)

    pubsub_message = envelope["message"]
    data = base64.b64decode(pubsub_message.get("data", b""))

    record = None
    msg_type = None

    # Try JSON first
    try:
        record = json.loads(data)
        msg_type = "JSON"
    except json.JSONDecodeError:
        try:
            # Try Avro decoding
            bytes_reader = io.BytesIO(data)
            decoder = avro.io.BinaryDecoder(bytes_reader)
            reader = avro.io.DatumReader(avro_schema)
            record = reader.read(decoder)
            msg_type = "AVRO"
        except Exception as e:
            print(f"❌ Failed to parse as JSON or Avro: {e}")
            return ("Bad message format", 400)

    # Transformation
    record["fulfillment_status"] = "PENDING"
    print(f"✅ Processed {msg_type} order: {record}")

    return ("", 204)  # ACK
