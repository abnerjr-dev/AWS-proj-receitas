import json
import boto3
import uuid
import base64
from datetime import datetime
import os
import logging

# Configuração de logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Clients AWS
s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")

# Configurações
table = dynamodb.Table(os.environ["DYNAMODB_TABLE"])
queue_url = os.environ["SQS_QUEUE_URL"]
bucket_name = os.environ["PROCESSING_BUCKET"]


def lambda_handler(event, context):
    logger.info(
        "Event received",
        extra={
            "http_method": event.get("httpMethod"),
            "path": event.get("path"),
            "content_type": event.get("headers", {}).get("Content-Type", ""),
            "content_length": len(event.get("body", "")),
        },
    )

    try:
        # Log completo para debugging
        print("=== EVENT DETAILS ===")
        print(f"HTTP Method: {event.get('httpMethod')}")
        print(f"Content-Type: {event.get('headers', {}).get('Content-Type')}")
        print(f"Body isBase64: {event.get('isBase64Encoded', False)}")
        print(f"Body length: {len(event.get('body', ''))}")

        # Handle preflight OPTIONS
        if event.get("httpMethod") == "OPTIONS":
            return handle_options_request()

        # Handle POST request
        if event.get("httpMethod") == "POST":
            return handle_post_request(event)
        else:
            return error_response("Método não permitido", 405)

    except Exception as e:
        logger.error("Handler error", extra={"error": str(e)})
        return error_response(f"Erro interno: {str(e)}")


def handle_options_request():
    """Handle CORS preflight"""
    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
            "Access-Control-Max-Age": "86400",
        },
        "body": "",
    }


def handle_post_request(event):
    """Handle POST request - VERSÃO TOLERANTE"""
    try:
        body = event.get("body", "")
        is_base64 = event.get("isBase64Encoded", False)
        content_type = event.get("headers", {}).get("Content-Type") or event.get(
            "headers", {}
        ).get("content-type", "")

        logger.info(
            "Processing POST request",
            extra={
                "content_type": content_type,
                "is_base64": is_base64,
                "body_length": len(body),
            },
        )

        if not content_type:
            logger.warning("Content-Type header missing, attempting to detect format")
            # Tenta detectar se é JSON
            if body.strip().startswith("{") or "image" in body:
                content_type = "application/json"
                logger.info("Auto-detected as JSON")
            else:
                return error_response("Header Content-Type é obrigatório")

        # Decodifica se necessário
        if is_base64:
            try:
                body = base64.b64decode(body)
                logger.info("Body base64 decoded")
            except Exception as e:
                logger.error("Base64 decode failed", extra={"error": str(e)})
                return error_response("Formato base64 inválido")

        # Processa baseado no Content-Type
        if "application/json" in content_type:
            return handle_json_body(body, is_base64)
        else:
            return error_response(f"Content-Type não suportado: {content_type}")

    except Exception as e:
        logger.error("POST handling error", extra={"error": str(e)})
        return error_response(f"Erro ao processar requisição: {str(e)}")


def handle_json_body(body, was_base64):
    """Processa corpo JSON"""
    try:
        # Se body ainda é bytes, decode para string
        if isinstance(body, bytes):
            body_str = body.decode("utf-8")
        else:
            body_str = body

        body_data = json.loads(body_str)

        # Extrai a imagem (pode ser base64 ou data URL)
        image_data = body_data.get("image", "")

        if not image_data:
            return error_response("Campo 'image' não encontrado no JSON")

        logger.info(
            "JSON body parsed",
            extra={
                "has_image": bool(image_data),
                "image_data_length": len(image_data),
                "has_fileName": "fileName" in body_data,
            },
        )

        # Processa a imagem base64
        return process_base64_image(image_data, body_data.get("fileName", "image.jpg"))

    except json.JSONDecodeError as e:
        logger.error("JSON decode error", extra={"error": str(e)})
        return error_response("JSON inválido no corpo da requisição")
    except Exception as e:
        logger.error("JSON body processing error", extra={"error": str(e)})
        return error_response(f"Erro ao processar JSON: {str(e)}")


def process_base64_image(image_data, file_name):
    """Processa imagem em base64"""
    try:
        # Remove data URL prefix se existir
        if image_data.startswith("data:image"):
            parts = image_data.split(",")
            if len(parts) == 2:
                image_data = parts[1]
                logger.info("Removed data URL prefix")

        # Valida se é base64 válido
        try:
            image_bytes = base64.b64decode(image_data)
            logger.info(
                "Base64 decoded successfully",
                extra={
                    "original_length": len(image_data),
                    "decoded_length": len(image_bytes),
                },
            )
        except Exception as e:
            logger.error("Base64 decode failed", extra={"error": str(e)})
            return error_response("Formato base64 inválido")

        # Valida tamanho mínimo
        if len(image_bytes) < 100:  # 100 bytes mínimo
            return error_response("Imagem muito pequena ou vazia")

        # Valida formato da imagem
        if not is_valid_image_format(image_bytes):
            return error_response("Formato de imagem não suportado. Use JPEG ou PNG")

        # Continua com o processamento
        return process_image_upload(image_bytes, file_name)

    except Exception as e:
        logger.error("Base64 image processing error", extra={"error": str(e)})
        return error_response(f"Erro ao processar imagem: {str(e)}")


def is_valid_image_format(image_bytes):
    """Valida se é JPEG ou PNG válido"""
    try:
        # JPEG magic numbers
        jpeg_signatures = [b"\xff\xd8\xff", b"\xff\xd8"]
        # PNG magic number
        png_signature = b"\x89PNG\r\n\x1a\n"

        is_jpeg = any(image_bytes.startswith(sig) for sig in jpeg_signatures)
        is_png = image_bytes.startswith(png_signature)

        logger.info(
            "Image format validation",
            extra={"is_jpeg": is_jpeg, "is_png": is_png, "is_valid": is_jpeg or is_png},
        )

        return is_jpeg or is_png
    except Exception as e:
        logger.error("Image format validation error", extra={"error": str(e)})
        return False


def process_image_upload(image_bytes, file_name):
    """Processa upload da imagem e inicia pipeline"""
    try:
        # Gera ID único
        request_id = str(uuid.uuid4())
        s3_key = f"images/{request_id}.jpg"

        logger.info(
            "Starting image processing",
            extra={
                "request_id": request_id,
                "image_size": len(image_bytes),
                "s3_key": s3_key,
            },
        )

        # Upload para S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=image_bytes,
            ContentType="image/jpeg",
            Metadata={
                "request_id": request_id,
                "original_name": file_name,
                "processed_at": datetime.utcnow().isoformat(),
            },
        )

        # Salva estado no DynamoDB
        table.put_item(
            Item={
                "requestId": request_id,
                "status": "PROCESSING",
                "createdAt": datetime.utcnow().isoformat(),
                "imageS3Key": s3_key,
                "imageSize": len(image_bytes),
                "updatedAt": datetime.utcnow().isoformat(),
                "expireAt": int(datetime.utcnow().timestamp()) + 3600,
            }
        )

        # Envia para SQS
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(
                {
                    "requestId": request_id,
                    "imageS3Key": s3_key,
                    "bucketName": bucket_name,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            ),
        )

        logger.info("Pipeline started successfully", extra={"request_id": request_id})

        return success_response(
            {
                "requestId": request_id,
                "status": "PROCESSING",
                "message": "Imagem recebida e em processamento",
            }
        )

    except Exception as e:
        logger.error("Image processing pipeline error", extra={"error": str(e)})
        return error_response(f"Erro ao iniciar processamento: {str(e)}")


def success_response(data):
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
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
            "Access-Control-Allow-Methods": "POST, OPTIONS",
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
