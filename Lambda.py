import json
import base64
import os
import boto3
from datetime import datetime

SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME') 
TEMPERATURE_THRESHOLD = 35.0 

sns_client = boto3.client('sns')
s3_client = boto3.client('s3')

def create_text_report(sensor_id, temperatura, threshold, timestamp_str):
    print("TXT REPORT GENERATION: Creando reporte de texto.")
    try:
        if timestamp_str.endswith('Z'):
             ts_parse = timestamp_str.replace('Z', '+00:00')
        elif '+' not in timestamp_str[-6:]:
             ts_parse = timestamp_str + '+00:00'
        else:
             ts_parse = timestamp_str
        dt_obj = datetime.fromisoformat(ts_parse)
        formatted_ts = dt_obj.strftime('%Y-%m-%d %H:%M:%S UTC')
    except Exception as ts_ex:
        print(f"TXT REPORT WARNING: No se pudo formatear timestamp '{timestamp_str}'. Error: {ts_ex}")
        formatted_ts = timestamp_str 

    report_content = (
        f"--- Alerta de Temperatura Elevada ---\n\n"
        f"Sensor ID:      {sensor_id}\n"
        f"Timestamp:      {formatted_ts}\n"
        f"Temperatura:    {temperatura}°C\n"
        f"Umbral:         {threshold}°C\n\n"
        f"------------------------------------\n"
    )
    return report_content

def lambda_handler(event, context):
    print("LAMBDA HANDLER: Iniciando ejecución.")
    print(f"LAMBDA HANDLER: ARN del Topic SNS: {SNS_TOPIC_ARN}")
    print(f"LAMBDA HANDLER: Nombre Bucket S3: {S3_BUCKET_NAME}")

    alert_messages_sent = 0
    reports_uploaded = 0
    processed_record_count = 0
    records_in_event = event.get('Records', [])
    num_records = len(records_in_event)
    print(f"LAMBDA HANDLER: Número de registros Kinesis en el evento: {num_records}")

    if not records_in_event:
        print("LAMBDA HANDLER: No se encontraron registros Kinesis. Terminando.")
        return {'statusCode': 200, 'body': json.dumps('Evento recibido sin registros Kinesis.')}

    print("LAMBDA HANDLER: Iniciando bucle para procesar registros...")
    for i, record in enumerate(records_in_event):
        print(f"\n--- Procesando Registro #{i+1} ---")
        try:
            print("RECORD PROCESSING: Intentando obtener y decodificar datos Kinesis...")
            payload_decoded = base64.b64decode(record.get('kinesis', {}).get('data', b'')).decode('utf-8')
            sensor_data = json.loads(payload_decoded)
            print(f"RECORD PROCESSING: Datos del sensor parseados: {sensor_data}")
            processed_record_count += 1

            temperatura = sensor_data.get('temperatura')
            sensor_id = sensor_data.get('sensor_id', 'Desconocido')
            timestamp_iso = sensor_data.get('timestamp', datetime.utcnow().isoformat()+'Z') # Usa timestamp o ahora
            print(f"RECORD PROCESSING: Sensor ID={sensor_id}, Temperatura={temperatura}")
            if isinstance(temperatura, (int, float)) and temperatura > TEMPERATURE_THRESHOLD:
                print(f"RECORD PROCESSING: ¡ALERTA DETECTADA! T={temperatura}°C > Umbral={TEMPERATURE_THRESHOLD}°C.")
                report_content_str = None
                try:
                    report_content_str = create_text_report(sensor_id, temperatura, TEMPERATURE_THRESHOLD, timestamp_iso)
                except Exception as report_e:
                    print(f"RECORD PROCESSING ERROR: Fallo al generar reporte de texto: {report_e}")

                if report_content_str and S3_BUCKET_NAME:
                    now = datetime.utcnow()
                    # Cambiar extensión a .txt
                    s3_key = f"alertas/{now.strftime('%Y/%m/%d')}/{sensor_id}_{now.strftime('%Y%m%d_%H%M%S%f')}.txt"
                    print(f"RECORD PROCESSING: Intentando subir TXT a S3: Bucket={S3_BUCKET_NAME}, Key={s3_key}")
                    try:
                        s3_client.put_object(
                            Bucket=S3_BUCKET_NAME,
                            Key=s3_key,
                            Body=report_content_str.encode('utf-8'), # Codificar string a bytes
                            ContentType='text/plain' # Indicar que es texto plano
                        )
                        print(f"RECORD PROCESSING: Reporte TXT subido exitosamente a S3.")
                        reports_uploaded += 1
                    except Exception as s3_e:
                        # Este error puede ocurrir si LabRole no tiene permiso s3:PutObject
                        print(f"RECORD PROCESSING ERROR: Fallo al subir TXT a S3: {s3_e}")

                if SNS_TOPIC_ARN:
                    subject = f"ALERTA de Temperatura Elevada: {sensor_id}"
                    message = (
                        f"Se detectó una temperatura elevada en el sensor '{sensor_id}'.\n\n"
                        f"Temperatura registrada: {temperatura}°C (Umbral: {TEMPERATURE_THRESHOLD}°C)\n"
                        f"Timestamp (aprox): {timestamp_iso}\n"
                        f"Por favor, tome las acciones necesarias."
                    )
                    try:
                        response = sns_client.publish(TopicArn=SNS_TOPIC_ARN, Message=message, Subject=subject)
                        print(f"RECORD PROCESSING: Mensaje de alerta publicado en SNS. MessageId: {response.get('MessageId')}")
                        alert_messages_sent += 1
                    except Exception as sns_e:
                        # Este error puede ocurrir si LabRole no tiene permiso sns:Publish
                        print(f"RECORD PROCESSING ERROR: Fallo al publicar en SNS: {sns_e}")
                else:
                     print("RECORD PROCESSING ERROR: Variable de entorno SNS_TOPIC_ARN no configurada.")

            else:
                print(f"RECORD PROCESSING: Temperatura normal o dato inválido (T={temperatura}).")

        except Exception as e:
            print(f"RECORD PROCESSING ERROR: Error inesperado procesando registro: {e}")

    print(f"\nLAMBDA HANDLER: Fin del bucle. {processed_record_count}/{num_records} registros procesados.")
    print(f"LAMBDA HANDLER: {alert_messages_sent} alertas enviadas a SNS.")
    print(f"LAMBDA HANDLER: {reports_uploaded} reportes TXT subidos a S3.")
    print("LAMBDA HANDLER: Finalizando ejecución.")

    return {
        'statusCode': 200,
        'body': json.dumps(f'Procesados {processed_record_count}/{num_records} registros. {alert_messages_sent} alertas SNS. {reports_uploaded} reportes TXT a S3.')
    }