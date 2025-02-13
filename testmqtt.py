import paho.mqtt.client as mqtt
import json
import time
import uuid
import random
import re
import threading

BROKER = "localhost"
PORT = 1883
USERNAME = "admin"
PASSWORD = "admin"
REQUEST_TOPIC = "rust/topic"
RESPONSE_TOPIC_BASE = "rust/response/"
received_response = None
response_received = False
response_time = None
client_id = None

# Dictionary to store chunks of split messages
split_messages = {}
early_chunks_buffer = {}

def get_rust_client_id(timeout=10):
    global client_id
    client = mqtt.Client()
    event = threading.Event()

    def on_connect(client, userdata, flags, rc):
        client.subscribe("rust/clients")

    def on_message(client, userdata, msg):
        global client_id
        try:
            payload = json.loads(msg.payload)
            if payload.get("type") == "client_connect":
                client_id = payload.get("client_id")
                event.set()
        except:
            pass

    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(USERNAME, PASSWORD)
    client.connect(BROKER, PORT)
    client.loop_start()
    
    if event.wait(timeout):
        print(f"🔑 Rust Client-ID empfangen: {client_id}")
    else:
        print("Timeout: Keine Client-ID empfangen")
    client.loop_stop()
    client.disconnect()
    return client_id

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"✅ Verbunden mit Broker (Code: {rc})")
    else:
        print(f"❌ Verbindungsfehler (Code: {rc})")

def on_message(client, userdata, msg):
    global received_response, response_received, split_messages, early_chunks_buffer
    
    try:
        payload = json.loads(msg.payload.decode())
        
        # Split-Nachricht verarbeiten
        split_pattern = re.compile(r'(.*)/split/(\d+)/(\d+)')
        match = split_pattern.match(msg.topic)
        if match:
            base_topic = match.group(1)
            chunk_index = int(match.group(2))
            total_chunks = int(match.group(3))
            
            print(f"\n📦 Split-Teil {chunk_index}/{total_chunks} empfangen")
            
            # Nachrichtenzusammenführung
            message_id = f"{base_topic}_{total_chunks}"
            if message_id not in split_messages:
                # Prüfe frühe Chunks
                buffer_key = f"{base_topic}_{total_chunks}"
                if buffer_key in early_chunks_buffer:
                    split_messages[message_id] = {
                        "total": total_chunks,
                        "chunks": early_chunks_buffer[buffer_key],
                        "base": base_topic,
                        "received": len(early_chunks_buffer[buffer_key])
                    }
                    del early_chunks_buffer[buffer_key]
                else:
                    split_messages[message_id] = {
                        "total": total_chunks,
                        "chunks": {},
                        "base": base_topic,
                        "received": 0
                    }
            
            split_messages[message_id]["chunks"][chunk_index] = payload.get("chunk", "")
            split_messages[message_id]["received"] += 1
            
            # Prüfe ob vollständig
            if split_messages[message_id]["received"] == total_chunks:
                print(f"🧩 Alle Teile für {message_id} empfangen")
                reassemble_message(message_id)
                del split_messages[message_id]
            return
        
        # Normale Nachricht
        print(f"\n📨 Antwort auf {msg.topic}:")
        print(json.dumps(payload, indent=2))
        received_response = payload
        response_received = True

    except json.JSONDecodeError:
        print(f"⚠️ Ungültiges JSON: {msg.payload}")

def reassemble_message(message_id):
    global received_response, response_received
    data = split_messages[message_id]
    chunks = [data["chunks"][i] for i in sorted(data["chunks"])]
    full_msg = "".join(chunks)
    
    try:
        result = json.loads(full_msg)
        print(f"✅ Nachricht zusammengesetzt ({len(full_msg)} Zeichen)")
        received_response = result
        response_received = True
        save_response_to_json(result)
    except Exception as e:
        print(f"⚠️ Fehler beim Zusammenbauen: {str(e)}")

def save_response_to_json(data, filename="response.json"):
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    print(f"💾 Antwort gespeichert in {filename}")

def run_mqtt_test(query_data, response_suffix, timeout=15):
    global received_response, response_received
    received_response = None
    response_received = False
    
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(USERNAME, PASSWORD)
    
    try:
        client.connect(BROKER, PORT)
        client.loop_start()
        
        response_topic = f"{RESPONSE_TOPIC_BASE}{client_id}/{response_suffix}"
        split_wildcard = f"{response_topic}/split/#"
        
        client.subscribe(response_topic)
        client.subscribe(split_wildcard)
        print(f"🔔 Abonniert auf: {response_topic} und {split_wildcard}")
        
        time.sleep(1)
        client.publish(REQUEST_TOPIC, json.dumps(query_data))
        print(f"\n📤 Anfrage gesendet: {json.dumps(query_data, indent=2)}")
        
        start = time.time()
        while not response_received and (time.time() - start) < timeout:
            time.sleep(0.1)
            
    finally:
        client.loop_stop()
        client.disconnect()
    
    if response_received:
        return received_response
    else:
        print("⌛ Timeout: Keine Antwort erhalten")
        return None

# Testfunktionen
def test_uuid(uuid):
    return run_mqtt_test(
        {"type": "uuid", "data": uuid},
        f"uuid/{uuid}"
    )

def test_all():
    return run_mqtt_test(
        {"type": "all"},
        "all"
    )

def test_color(color):
    return run_mqtt_test(
        {"type": "color", "data": color},
        f"color/{color}"
    )

def test_time_range(start, end):
    return run_mqtt_test(
        {"type": "time_range", "start": start, "end": end},
        f"time_range/{start}_{end}"
    )

def test_temperature_humidity(temp, hum):
    return run_mqtt_test(
        {"type": "temperature_humidity", "temperature": temp, "humidity": hum},
        f"env/{temp}_{hum}"
    )

def test_timestamp(ts):
    return run_mqtt_test(
        {"type": "timestamp", "data": ts},
        f"timestamp/{ts}"
    )

def test_energy_cost(cost):
    return run_mqtt_test(
        {"type": "energy_cost", "data": cost},
        f"energy/cost/{cost}"
    )

def test_energy_consume(consume):
    return run_mqtt_test(
        {"type": "energy_consume", "data": consume},
        f"energy/consume/{consume}"
    )

if __name__ == "__main__":
    # Client-ID ermitteln
    rust_client_id = get_rust_client_id()
    if not rust_client_id:
        exit(1)
    
    # Testmenü
    print("\n🔍 MQTT Test Suite für Neo4j Abfragen")
    print("1. UUID Abfrage")
    print("2. Alle Nodes")
    print("3. Farbe")
    print("4. Zeitbereich")
    print("5. Temperatur/Luftfeuchtigkeit")
    print("6. Zeitstempel")
    print("7. Energiekosten")
    print("8. Energieverbrauch")
    print("9. Alle Tests")
    
    choice = input("➡️  Wahl: ")
    
    if choice == "1":
        uuid = input("UUID (default: abc123xyz001): ") or "abc123xyz001"
        test_uuid(uuid)
    elif choice == "2":
        test_all()
    elif choice == "3":
        test_color(input("Farbe (default: red): ") or "red")
    elif choice == "4":
        test_time_range(
            input("Start (default: 2025-01-01T00:00:00Z): ") or "2025-01-01T00:00:00Z",
            input("Ende (default: 2025-03-01T00:00:00Z): ") or "2025-03-01T00:00:00Z"
        )
    elif choice == "5":
        test_temperature_humidity(
            float(input("Temperatur (default: 22.5): ") or 22.5),
            float(input("Luftfeuchtigkeit (default: 45.0): ") or 45.0)
        )
    elif choice == "6":
        test_timestamp(input("Zeitstempel (default: 2025-02-15T12:30:00Z): ") or "2025-02-15T12:30:00Z")
    elif choice == "7":
        test_energy_cost(float(input("Kosten (default: 0.25): ") or 0.25))
    elif choice == "8":
        test_energy_consume(float(input("Verbrauch (default: 150.0): ") or 150.0))
    elif choice == "9":
        print("\n🚀 Starte Kompletttest...")
        test_uuid("abc123xyz001")
        test_all()
        test_color("blue")
        test_time_range("2025-01-01T00:00:00Z", "2025-03-01T00:00:00Z")
        test_temperature_humidity(22.5, 45.0)
        test_timestamp("2025-02-15T12:30:00Z")
        test_energy_cost(0.25)
        test_energy_consume(150.0)
        print("\n✅ Alle Tests abgeschlossen")
    else:
        print("Ungültige Auswahl")