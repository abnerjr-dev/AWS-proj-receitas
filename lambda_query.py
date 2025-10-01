import json
import boto3
import os
from datetime import datetime, timedelta

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(os.environ["DYNAMODB_TABLE"])


def lambda_handler(event, context):
    print("Query event:", json.dumps(event))

    try:
        # Handle CORS preflight request
        if event.get("httpMethod") == "OPTIONS":
            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET, OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
                },
                "body": json.dumps({"message": "CORS preflight"}),
            }

        # Extrai requestId dos query parameters
        query_params = event.get("queryStringParameters", {}) or {}
        request_id = query_params.get("requestId")

        if not request_id:
            return error_response("Parâmetro requestId é obrigatório", 400)

        # Consulta resultado no DynamoDB
        response = table.get_item(Key={"requestId": request_id})

        if "Item" not in response:
            return error_response("Solicitação não encontrada", 404)

        item = response["Item"]
        status = item.get("status", "UNKNOWN")

        # Prepara resposta baseada no status
        response_data = {
            "requestId": request_id,
            "status": status,
            "createdAt": item.get("createdAt"),
            "updatedAt": item.get("updatedAt"),
        }

        if status == "COMPLETED":
            response_data.update(
                {
                    "ingredients": item.get("ingredients", []),
                    "recipes": item.get("recipes", ""),
                    "processingTime": calculate_processing_time(
                        item.get("createdAt"), item.get("updatedAt")
                    ),
                }
            )
        elif status == "ERROR":
            response_data.update(
                {"errorMessage": item.get("errorMessage", "Erro desconhecido")}
            )
        else:
            response_data.update(
                {
                    "message": get_status_message(status),
                    "estimatedWaitTime": get_estimated_wait_time(status),
                }
            )

        return success_response(response_data)

    except Exception as e:
        print(f"Error querying result: {str(e)}")
        return error_response(f"Erro na consulta: {str(e)}", 500)


def calculate_processing_time(created_at, updated_at):
    """Calcula o tempo total de processamento"""
    try:
        if created_at and updated_at:
            created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            updated = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            duration = updated - created
            return f"{duration.total_seconds():.1f} segundos"
        return "N/A"
    except:
        return "N/A"


def get_status_message(status):
    messages = {
        "PROCESSING": "Sua imagem está na fila de processamento...",
        "ANALYZING_IMAGE": "🔍 Analisando ingredientes na imagem...",
        "GENERATING_RECIPES": "👨‍🍳 Gerando receitas personalizadas...",
        "COMPLETED": "✅ Processamento concluído!",
        "ERROR": "❌ Erro no processamento",
        "UNKNOWN": "Status desconhecido",
    }
    return messages.get(status, "Processando sua solicitação...")


def get_estimated_wait_time(status):
    estimates = {
        "PROCESSING": "10-30 segundos",
        "ANALYZING_IMAGE": "5-15 segundos",
        "GENERATING_RECIPES": "5-10 segundos",
    }
    return estimates.get(status, "Alguns segundos")


def success_response(data):
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
        },
        "body": json.dumps(data),
    }


def error_response(message, status_code=500):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
        },
        "body": json.dumps({"error": message}),
    }
