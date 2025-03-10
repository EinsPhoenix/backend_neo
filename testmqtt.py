import paho.mqtt.client as mqtt
import json
import time
import uuid

BROKER = "localhost"
PORT = 1883
USERNAME = "admin"
PASSWORD = "admin"
REQUEST_TOPIC = "rust/topic"
RESPONSE_TOPIC_BASE = "rust/response/"


received_response = None
response_received = False


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"Erfolgreich mit Broker verbunden (Code: {rc})")
    else:
        print(f"Verbindungsfehler (Code: {rc})")


def on_message(client, userdata, msg):
    global received_response, response_received
    try:
        payload = json.loads(msg.payload.decode())
        print(f"\nAntwort empfangen auf {msg.topic}:")
        print(json.dumps(payload, indent=2))
        received_response = payload
        response_received = True
    except json.JSONDecodeError:
        print(f"Ungültiges JSON empfangen: {msg.payload}")


def test_uuid_query(test_uuid):
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.username_pw_set(USERNAME, PASSWORD)

    try:

        client.connect(BROKER, PORT, 60)
        client.loop_start()

        response_topic = f"{RESPONSE_TOPIC_BASE}{test_uuid}"
        client.subscribe(response_topic)
        print(f"Abonniert auf: {response_topic}")
        time.sleep(1)

        message = {"type": "uuid", "data": test_uuid}
        client.publish(REQUEST_TOPIC, json.dumps(message))
        print(f"\nGesendete Nachricht: {json.dumps(message, indent=2)}")
        print(f"An Topic: {REQUEST_TOPIC}")

        # Auf Antwort warten (max 10 Sekunden)
        timeout = 10
        start_time = time.time()
        while not response_received and (time.time() - start_time) < timeout:
            time.sleep(0.5)

    except Exception as e:
        print(f"Fehler: {str(e)}")
    finally:
        client.loop_stop()
        client.disconnect()

    if not response_received:
        print("\nTimeout: Keine Antwort erhalten")
    return received_response


if __name__ == "__main__":

    existing_uuid = "abc123xyz001"
    new_uuid = str(uuid.uuid4())

    print("Teste mit existierender UUID:")
    test_uuid_query(existing_uuid)

    print("\n\nTeste mit nicht existierender UUID:")
    test_uuid_query(new_uuid)
