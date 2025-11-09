import boto3
import uuid
import os
import json
from datetime import datetime

def lambda_handler(event, context):
    try:
        # Entrada (json)
        log_info = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Solicitud recibida",
                "event": event,
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        print(json.dumps(log_info))
        
        # Validación de datos de entrada
        if 'body' not in event:
            raise ValueError("El campo 'body' es requerido en el evento")
        
        tenant_id = event['body']['tenant_id']
        pelicula_datos = event['body']['pelicula_datos']
        nombre_tabla = os.environ["TABLE_NAME"]
        
        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }
        
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)
        
        # Salida exitosa (json)
        log_success = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Película creada exitosamente",
                "pelicula": pelicula,
                "dynamodb_response": {
                    "HTTPStatusCode": response['ResponseMetadata']['HTTPStatusCode'],
                    "RequestId": response['ResponseMetadata']['RequestId']
                },
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        print(json.dumps(log_success))
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'pelicula': pelicula,
                'mensaje': 'Película creada exitosamente'
            })
        }
        
    except KeyError as e:
        # Error de clave faltante
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": f"Campo requerido faltante: {str(e)}",
                "error_type": "KeyError",
                "event": event,
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'error': f"Campo requerido faltante: {str(e)}"
            })
        }
        
    except ValueError as e:
        # Error de validación
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": str(e),
                "error_type": "ValueError",
                "event": event,
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'error': str(e)
            })
        }
        
    except Exception as e:
        # Error genérico
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": f"Error inesperado: {str(e)}",
                "error_type": type(e).__name__,
                "event": event,
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'error': f"Error interno del servidor: {str(e)}"
            })
        }
