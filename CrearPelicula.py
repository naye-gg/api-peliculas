import boto3
import uuid
import os
import json
from datetime import datetime
from decimal import Decimal

def lambda_handler(event, context):
    try:
        # Entrada (json)
        log_info = {
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Solicitud recibida",
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        print(json.dumps(log_info))
        
        # Parsear el body si viene como string (Lambda Proxy)
        if 'body' not in event:
            raise ValueError("El campo 'body' es requerido en el evento")
        
        body = event['body']
        if isinstance(body, str):
            body = json.loads(body)
        
        tenant_id = body['tenant_id']
        pelicula_datos = body['pelicula_datos']
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
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        print(json.dumps(log_success))
        
        # Return simplificado - solo datos serializables
        return {
            'statusCode': 200,
            'body': json.dumps({
                'mensaje': 'Película creada exitosamente',
                'pelicula': pelicula
            })
        }
        
    except KeyError as e:
        log_error = {
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": f"Campo requerido faltante: {str(e)}",
                "error_type": "KeyError",
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 400,
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
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 400,
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
                "timestamp": datetime.utcnow().isoformat()
            }
        }
        print(json.dumps(log_error))
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': f"Error interno del servidor: {str(e)}"
            })
        }
