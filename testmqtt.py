import paho.mqtt.client as mqtt
import json
import time
import uuid
import threading
import random
import re
import argparse
from concurrent.futures import ThreadPoolExecutor

# Configuration
BROKER = "localhost"
PORT = 1883
USERNAME = "admin"
PASSWORD = "admin"
REQUEST_TOPIC = "rust/request"
RESPONSE_TOPIC_BASE = "rust/response/"


class MqttClient:
    def __init__(self, client_name=None):
        self.client_id = client_name or f"python-client-{str(uuid.uuid4())[:8]}"
        self.received_response = None
        self.response_received = threading.Event()

        self.paginated_messages = {}
        self.client = mqtt.Client(client_id=self.client_id)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.username_pw_set(USERNAME, PASSWORD)
        self.lock = threading.Lock()

    def on_connect(self, client, userdata, flags, rc):
        with self.lock:
            if rc == 0:
                print(f"✅ Client {self.client_id} connected to broker (Code: {rc})")
            else:
                print(f"❌ Client {self.client_id} connection error (Code: {rc})")

    def on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())

            # Handle paginated messages
            page_pattern = re.compile(r"(.*)/page/(\d+)")
            page_match = page_pattern.match(msg.topic)
            if page_match:
                with self.lock:
                    print(
                        f"\n📄 Client {self.client_id} received paginated message part on {msg.topic}"
                    )
                self.handle_paginated_message(page_match, payload)
                return

            # Handle paginated summary messages
            if msg.topic.endswith("/summary"):
                with self.lock:
                    print(
                        f"\n📋 Client {self.client_id} received pagination summary on {msg.topic}"
                    )
                self.handle_pagination_summary(msg.topic, payload)
                return

            # Regular message
            with self.lock:
                print(f"\n📨 Client {self.client_id} received response on {msg.topic}:")
                print(json.dumps(payload, indent=2))
            self.received_response = payload
            self.response_received.set()

        except json.JSONDecodeError:
            with self.lock:
                print(f"⚠️ Client {self.client_id} received invalid JSON: {msg.payload}")

    def handle_paginated_message(self, match, payload):
        base_topic = match.group(1)
        page_num = int(match.group(2))

        # Extract pagination metadata
        request_id = payload.get("request_id")
        if not request_id:
            with self.lock:
                print(f"⚠️ Paginated message missing request_id: {payload}")
            return

        message_id = f"{base_topic}_{request_id}"

        # Initialize tracking structure for this paginated message if needed
        if message_id not in self.paginated_messages:
            self.paginated_messages[message_id] = {
                "total_pages": payload.get("total_pages", 0),
                "pages": {},
                "received_pages": 0,
                "complete": False,
            }

        # Update tracking info with total pages if we didn't have it
        if self.paginated_messages[message_id]["total_pages"] == 0:
            self.paginated_messages[message_id]["total_pages"] = payload.get(
                "total_pages", 0
            )

        # Store this page
        self.paginated_messages[message_id]["pages"][page_num] = payload.get("data", [])
        self.paginated_messages[message_id]["received_pages"] += 1

        with self.lock:
            print(
                f"📄 Received page {page_num}/{self.paginated_messages[message_id]['total_pages']} for {message_id}"
            )

        # Check if we have all pages and can reassemble
        if (
            self.paginated_messages[message_id]["received_pages"]
            == self.paginated_messages[message_id]["total_pages"]
        ):
            with self.lock:
                print(f"🔄 All pages received for {message_id}, reassembling...")
            self.reassemble_paginated_message(message_id)

    def handle_pagination_summary(self, topic, payload):
        request_id = payload.get("request_id")
        total_pages = payload.get("total_pages", 0)
        base_topic = payload.get("topic_base")

        if not request_id or not base_topic:
            with self.lock:
                print(f"⚠️ Summary message missing critical data: {payload}")
            return

        message_id = f"{base_topic}_{request_id}"

        with self.lock:
            print(
                f"📋 Summary for {message_id}: {total_pages} pages, {payload.get('total_items', 0)} items"
            )

        # If we already have all pages, trigger reassembly
        if (
            message_id in self.paginated_messages
            and self.paginated_messages[message_id]["received_pages"] == total_pages
        ):
            with self.lock:
                print(
                    f"🔄 All pages already received for {message_id}, reassembling..."
                )
            self.reassemble_paginated_message(message_id)

    def reassemble_paginated_message(self, message_id):
        if not self.paginated_messages[message_id]["complete"]:
            data = self.paginated_messages[message_id]

            # Combine all pages in order
            all_items = []
            for page_num in sorted(data["pages"].keys()):
                all_items.extend(data["pages"][page_num])

            with self.lock:
                print(
                    f"✅ Reassembled {len(all_items)} items from {len(data['pages'])} pages"
                )

            # Mark as complete to avoid reassembling again if we get multiple triggers
            self.paginated_messages[message_id]["complete"] = True

            # Set as the response and trigger the event
            self.received_response = all_items
            self.response_received.set()

            # Save to file
            filename = f"paginated_response_{self.client_id}_{int(time.time())}.json"
            with open(filename, "w") as f:
                json.dump(all_items, f, indent=2)

            with self.lock:
                print(f"💾 Paginated response saved to {filename}")

    def send_request(self, query_data, response_suffix, timeout=15):
        self.response_received.clear()
        self.received_response = None
        # Reset paginated message tracking
        self.paginated_messages = {}

        try:
            self.client.connect(BROKER, PORT)
            self.client.loop_start()

            if "request" in query_data and query_data["request"] == "uuid":
                response_topic = f"rust/uuid/{self.client_id}"
            else:
                response_topic = (
                    f"{RESPONSE_TOPIC_BASE}{self.client_id}/{response_suffix}"
                )

            # Subscribe to all the possible topic patterns

            page_wildcard = f"{response_topic}/page/#"
            summary_topic = f"{response_topic}/summary"

            self.client.subscribe(response_topic)

            self.client.subscribe(page_wildcard)
            self.client.subscribe(summary_topic)

            with self.lock:
                print(f"🔔 Client {self.client_id} subscribed to: {response_topic}")
                print(f"🔔 Client {self.client_id} subscribed to: {page_wildcard}")
                print(f"🔔 Client {self.client_id} subscribed to: {summary_topic}")

            # Add client_id to query data
            query_data["client_id"] = self.client_id

            # Send request
            self.client.publish(REQUEST_TOPIC, json.dumps(query_data))

            with self.lock:
                print(
                    f"📤 Client {self.client_id} sent request: {json.dumps(query_data)}"
                )

            # Wait for response - using a longer timeout for paginated responses
            timeout = 30 if "all" in response_suffix else timeout
            result = self.response_received.wait(timeout)

            if not result:
                with self.lock:
                    print(f"⌛ Client {self.client_id} timeout: No response received")

            return self.received_response

        finally:
            self.client.loop_stop()
            self.client.disconnect()


def run_test_client(client_name, request_type, params):
    """Run a single test client with the given parameters"""
    client = MqttClient(client_name)

    if request_type == "uuid":
        uuid_val = params.get("uuid", "abc123xyz001")
        return client.send_request(
            {"request": "uuid", "payload": [{"uuid": uuid_val}]}, "uuid"
        )
    elif request_type == "all":
        return client.send_request({"request": "all"}, "all")
    elif request_type == "color":
        color = params.get("color", "red")
        return client.send_request({"request": "color", "data": color}, "color")
    elif request_type == "time_range":
        start = params.get("start", "2025-01-01T00:00:00Z")
        end = params.get("end", "2025-03-01T00:00:00Z")
        return client.send_request(
            {"request": "time_range", "start": start, "end": end}, "time_range"
        )
    elif request_type == "temperature_humidity":
        temp = params.get("temperature", 22.5)
        hum = params.get("humidity", 45.0)
        return client.send_request(
            {"request": "temperature_humidity", "temperature": temp, "humidity": hum},
            "temperature_humidity",
        )
    elif request_type == "timestamp":
        ts = params.get("timestamp", "2025-02-15T12:30:00Z")
        return client.send_request({"request": "timestamp", "data": ts}, "timestamp")
    elif request_type == "energy_cost":
        cost = params.get("cost", 0.25)
        return client.send_request(
            {"request": "energy_cost", "data": cost}, "energy_cost"
        )
    elif request_type == "energy_consume":
        consume = params.get("consume", 150.0)
        return client.send_request(
            {"request": "energy_consume", "data": consume}, "energy_consume"
        )


def run_multiple_clients(num_clients=2, test_type=None):
    """Run multiple clients simultaneously"""
    test_types = [
        "uuid",
        "all",
        "color",
        "time_range",
        "temperature_humidity",
        "timestamp",
        "energy_cost",
        "energy_consume",
    ]

    if test_type and test_type not in test_types:
        print(f"Invalid test type: {test_type}")
        return

    tasks = []

    for i in range(num_clients):
        client_name = f"test-client-{i+1}"

        # If test_type is provided, use it for all clients
        # Otherwise, randomly select a test type for each client
        selected_test = test_type or random.choice(test_types)

        # Prepare parameters with slight variations
        params = {}
        if selected_test == "uuid":
            params["uuid"] = f"test-uuid-{i+1}"
        elif selected_test == "color":
            colors = ["red", "green", "blue", "yellow", "purple"]
            params["color"] = colors[i % len(colors)]
        elif selected_test == "time_range":
            params["start"] = f"2025-0{i+1}-01T00:00:00Z"
            params["end"] = f"2025-0{i+2}-01T00:00:00Z"
        elif selected_test == "temperature_humidity":
            params["temperature"] = 20.0 + i
            params["humidity"] = 40.0 + i
        elif selected_test == "timestamp":
            params["timestamp"] = f"2025-02-{15+i}T12:30:00Z"
        elif selected_test == "energy_cost":
            params["cost"] = 0.20 + (i * 0.05)
        elif selected_test == "energy_consume":
            params["consume"] = 100.0 + (i * 50.0)

        tasks.append((client_name, selected_test, params))

    print(f"🚀 Starting {num_clients} clients simultaneously...")

    # Create a thread pool and submit all tasks
    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        futures = [
            executor.submit(run_test_client, name, test, params)
            for name, test, params in tasks
        ]

        # Wait for all to complete
        for future in futures:
            future.result()

    print(f"✅ All client tests completed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MQTT Multi-Client Test Tool")
    parser.add_argument(
        "--clients", type=int, default=2, help="Number of simultaneous clients"
    )
    parser.add_argument(
        "--test",
        type=str,
        choices=[
            "uuid",
            "all",
            "color",
            "time_range",
            "temperature_humidity",
            "timestamp",
            "energy_cost",
            "energy_consume",
        ],
        help="Specific test to run (if not specified, random tests will be chosen)",
    )

    args = parser.parse_args()

    run_multiple_clients(args.clients, args.test)
