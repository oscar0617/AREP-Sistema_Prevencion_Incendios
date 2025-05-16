import paho.mqtt.client as mqtt
import ssl
import json
import time
import random
from datetime import datetime, timezone

AWS_HOST = "a2703k4q09iv28-ats.iot.us-east-1.amazonaws.com"
AWS_PORT = 8883
CLIENT_ID = "ProyectoFinal"
CA_FILE = "AmazonRootCA1.pem"
CERT_FILE = "certificado cert.crt" 
KEY_FILE = "e4f8d5735c7606466d040166f5013212db3bcf8f6baf0881ae9a5b8d33ba378c-private.pem.key" 

TOPIC = "iot/thermostat/data"
QOS = 1
SENSOR_ID = "ProyectoFinal"
UNIDAD_TEMPERATURA = "C"

NUMERO_DE_SOLICITUDES = 1000  

def on_connect(client, userdata, flags, rc, properties=None):
    """
    0: Conexión exitosa
    1: Conexión rechazada - versión de protocolo incorrecta
    2: Conexión rechazada - identificador de cliente inválido
    3. Conexión rechazada - servidor no disponible
    4: Conexión rechazada - nombre de usuario o contraseña incorrectos
    5: Conexión rechazada - no autorizado
    """
    if rc == 0:
        print(f"Conectado exitosamente al broker MQTT (ID: {CLIENT_ID})")
    else:
        print(f"Error al conectar, código de resultado: {rc}")
def on_publish(client, userdata, mid, properties=None, reason_code=None):
    if reason_code is not None:
        print(f"Mensaje {mid} publicado con QoS {QOS}. Reason code: {reason_code}")
    else:
        print(f"Mensaje {mid} publicado con QoS {QOS}.")


def on_disconnect(client, userdata, rc, properties=None):
    print(f"Desconectado del broker con código de resultado: {rc}")
    if rc != 0:
        print("Desconexión inesperada.")

def on_log(client, userdata, level, buf):
    print(f"Log: {buf}")

def publicar_datos():
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1, client_id=CLIENT_ID)
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.on_disconnect = on_disconnect
    try:
        client.tls_set(
            ca_certs=CA_FILE,
            certfile=CERT_FILE,
            keyfile=KEY_FILE,
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
            ciphers=None
        )
        print("Certificados SSL/TLS cargados correctamente.")
    except FileNotFoundError as e:
        print(f"Error: No se encontró uno de los archivos de certificado: {e}")
        print("Por favor, asegúrate de que los archivos CA_FILE, CERT_FILE y KEY_FILE están en la ubicación correcta.")
        return
    except Exception as e:
        print(f"Error al configurar TLS: {e}")
        return
    try:
        print(f"Intentando conectar a {AWS_HOST}:{AWS_PORT}...")
        client.connect(AWS_HOST, AWS_PORT, keepalive=60)
    except Exception as e:
        print(f"Error al conectar: {e}")
        return
    client.loop_start()
    time.sleep(2)

    if not client.is_connected():
        print("No se pudo establecer la conexión después de la espera. Abortando.")
        client.loop_stop()
        return
    for i in range(NUMERO_DE_SOLICITUDES):
        timestamp_utc = datetime.now(timezone.utc).isoformat()
        temperatura = round(random.uniform(20.0, 300.0), 2)
        payload = {
            "sensor_id": SENSOR_ID,
            "timestamp": timestamp_utc,
            "temperatura": temperatura,
            "unidad": UNIDAD_TEMPERATURA
        }
        payload_json = json.dumps(payload)
        print(f"Publicando mensaje {i+1}/{NUMERO_DE_SOLICITUDES}: {payload_json}")
        result = client.publish(TOPIC, payload_json, qos=QOS)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"Mensaje {result.mid} encolado para ser enviado.")
        else:
            print(f"Error al encolar el mensaje: {mqtt.error_string(result.rc)}")
        time.sleep(1) 
    print("Esperando a que se completen las publicaciones...")
    time.sleep(NUMERO_DE_SOLICITUDES * 0.5 + 2)
    print("Deteniendo el cliente MQTT...")
    client.loop_stop()
    client.disconnect()
    print("Cliente MQTT desconectado.")

if __name__ == '__main__':
    publicar_datos()