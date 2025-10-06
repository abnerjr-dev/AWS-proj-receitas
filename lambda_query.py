import json
import boto3
import os
from datetime import datetime
import logging
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["DYNAMODB_TABLE"])


def convert_decimals(obj):
    """Converte objetos Decimal para tipos nativos Python para serialização JSON"""
    if isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    elif isinstance(obj, Decimal):
        # Converte Decimal para float ou int
        return float(obj) if obj % 1 != 0 else int(obj)
    else:
        return obj


def lambda_handler(event, context):
    print("=== QUERY EVENT ===")
    print(f"HTTP Method: {event.get('httpMethod')}")
    print(f"Query params: {event.get('queryStringParameters')}")

    try:
        # Handle OPTIONS preflight
        if event.get("httpMethod") == "OPTIONS":
            return {
                "statusCode": 200,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET, OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
                },
                "body": "",
            }

        # Handle GET request
        if event.get("httpMethod") == "GET":
            return handle_get_request(event)
        else:
            return error_response("Método não permitido", 405)

    except Exception as e:
        print(f"Query handler error: {e}")
        return error_response(f"Erro interno na consulta: {str(e)}")


def handle_get_request(event):
    """Handle GET request for results"""
    try:
        query_params = event.get("queryStringParameters") or {}
        request_id = query_params.get("requestId", "")

        print(f"Query parameters: {query_params}")
        print(f"RequestId: '{request_id}'")

        if not request_id:
            return error_response("Parâmetro requestId é obrigatório")

        request_id = request_id.strip()

        print(f"Querying DynamoDB for: {request_id}")

        # Consulta DynamoDB
        response = table.get_item(Key={"requestId": request_id})

        if "Item" not in response:
            print("Item not found in DynamoDB")
            return error_response("Solicitação não encontrada ou expirada", 404)

        item = response["Item"]
        status = item.get("status", "UNKNOWN")

        print(f"Item found, status: {status}")
        print(f"Item keys: {list(item.keys())}")

        cleaned_item = convert_decimals(item)

        # Prepara resposta
        response_data = {
            "requestId": request_id,
            "status": status,
            "createdAt": cleaned_item.get("createdAt"),
            "updatedAt": cleaned_item.get("updatedAt"),
        }

        if status == "COMPLETED":
            response_data["ingredients"] = cleaned_item.get("ingredients", [])
            response_data["recipes"] = cleaned_item.get("recipes", "")
            response_data["completedAt"] = cleaned_item.get("completedAt")
            print("Returning COMPLETED result")

        elif status == "ERROR":
            response_data["errorMessage"] = cleaned_item.get(
                "errorMessage", "Erro desconhecido"
            )
            print("Returning ERROR result")

        else:
            # Status intermediários
            response_data["message"] = get_status_message(status)
            print(f"Returning {status} result")

        return success_response(response_data)

    except Exception as e:
        print(f"GET request error: {e}")
        return error_response(f"Erro ao consultar resultado: {str(e)}")


def get_status_message(status):
    """Mensagens amigáveis para cada status"""
    messages = {
        "PROCESSING": "Sua imagem está sendo processada...",
        "ANALYZING_IMAGE": "🔍 Analisando ingredientes na imagem...",
        "GENERATING_RECIPES": "👩‍🍳 Gerando receitas personalizadas...",
        "UPLOADING": "📤 Fazendo upload da imagem...",
        "QUEUED": "⏳ Na fila de processamento...",
    }
    return messages.get(status, "Processando sua solicitação...")


def success_response(data):
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
        },
        "body": json.dumps(
            {"success": True, "data": data, "timestamp": datetime.utcnow().isoformat()}
        ),
    }


def error_response(message, status_code=400):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
        },
        "body": json.dumps(
            {
                "success": False,
                "error": message,
                "timestamp": datetime.utcnow().isoformat(),
            }
        ),
    }
