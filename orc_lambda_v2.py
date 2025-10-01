import json
import boto3
import uuid
from datetime import datetime
import os
import base64
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")

table = dynamodb.Table(os.environ["DYNAMODB_TABLE"])
queue_url = os.environ["SQS_QUEUE_URL"]
bucket_name = os.environ["PROCESSING_BUCKET"]

# CORS headers for all responses
CORS_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,Content-Type",
}


def lambda_handler(event, context):
    logger.info("=== LAMBDA ORCHESTRATOR STARTED ===")
    logger.info("chegou na Lambda Orchestrator!")

    # Handle CORS preflight requests
    if event.get("httpMethod") == "OPTIONS":
        logger.info("Handling CORS preflight request")
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({"message": "CORS preflight"}),
        }

    try:
        # Safe event debugging - NEVER use json.dumps on entire event
        logger.info("=== SAFE EVENT DEBUGGING ===")
        logger.info(f"1. BASIC EVENT INFO:")
        logger.info(f"   Resource: {event.get('resource')}")
        logger.info(f"   Path: {event.get('path')}")
        logger.info(f"   HTTP Method: {event.get('httpMethod')}")
        logger.info(f"   isBase64Encoded: {event.get('isBase64Encoded')}")

        # Safe headers logging
        logger.info(f"2. HEADERS:")
        headers = event.get("headers", {})
        for key, value in headers.items():
            if isinstance(value, str) and len(value) < 100:
                logger.info(f"   {key}: {value}")

        # Handle body SAFELY
        if "body" in event:
            logger.info(f"3. BODY ANALYSIS:")
            logger.info(f"   Body exists: Yes")
            logger.info(f"   Body type: {type(event['body'])}")
            body_length = len(event["body"]) if event.get("body") else 0
            logger.info(f"   Body length: {body_length}")

            if event.get("isBase64Encoded", False):
                logger.info(f"   Body is base64 encoded")
                try:
                    decoded_bytes = base64.b64decode(event["body"])
                    logger.info(f"   Decoded size: {len(decoded_bytes)} bytes")

                    # Check if it's image data
                    if len(decoded_bytes) > 4:
                        magic_bytes = decoded_bytes[:4]
                        if magic_bytes.startswith(b"\xff\xd8"):
                            logger.info(f"   Body is JPEG image data")
                        elif magic_bytes.startswith(b"\x89PNG"):
                            logger.info(f"   Body is PNG image data")
                        elif b"multipart/form-data" in magic_bytes:
                            logger.info(f"   Body is multipart form data")
                        else:
                            logger.info(f"   Body is binary data (unknown type)")

                    # Try to decode as text for multipart form data
                    try:
                        body_text = decoded_bytes.decode("utf-8")
                        if len(body_text) < 10000:  # Reasonable size for form data
                            logger.info(f"   Body decoded as text successfully")
                            # Look for multipart indicators
                            if "multipart/form-data" in body_text.lower():
                                logger.info(f"   Contains multipart/form-data")
                            if "filename=" in body_text:
                                logger.info(f"   Contains file upload")
                    except UnicodeDecodeError:
                        logger.info(f"   Body is binary data (cannot decode as text)")

                except Exception as e:
                    logger.error(f"   Decoding error: {str(e)}")
                    return error_response(f"Body decoding error: {str(e)}")
            else:
                # Body is not base64 encoded
                body = event["body"]
                if isinstance(body, str) and len(body) < 1000:
                    logger.info(f"   Body (text): {body[:500]}")
                else:
                    logger.info(
                        f"   Body is large or binary: {len(body) if body else 0} chars"
                    )

        logger.info("=== END SAFE DEBUGGING ===")

        # Process the request
        if "body" in event:
            logger.info("Starting request processing...")

            if event.get("isBase64Encoded", False):
                # Decode base64 and handle as text for multipart form data
                try:
                    decoded_bytes = base64.b64decode(event["body"])
                    body = decoded_bytes.decode("utf-8")
                    logger.info("Body successfully decoded from base64 to UTF-8")
                except Exception as e:
                    logger.error(f"Error decoding body: {str(e)}")
                    return error_response(f"Error decoding request body: {str(e)}")
            else:
                body = event["body"]

            return process_multipart_form_data(body, event.get("headers", {}))
        else:
            return error_response("No body in request")

    except Exception as e:
        logger.error(f"Error in lambda handler: {str(e)}")
        return error_response(f"Internal error: {str(e)}")


def process_multipart_form_data(body, headers):
    """Process multipart form data with safe logging"""
    logger.info("=== MULTIPART FORM PROCESSING START ===")

    logger.info(f"Content-Type: {headers.get('content-type', 'Unknown')}")
    logger.info(f"Body length: {len(body) if body else 0}")

    # Safe body sampling for debugging
    if body and len(body) > 500:
        logger.info(f"Body sample (first 500 chars): {body[:500]}...")
    elif body:
        logger.info(f"Body: {body}")
    else:
        logger.info("Body is empty")

    try:
        # Parse multipart form data
        lines = body.split("\r\n")
        image_data = None
        filename = None
        original_filename = None

        logger.info(f"Multipart lines: {len(lines)}")

        for i, line in enumerate(lines):
            if "filename=" in line:
                # Extract filename - handle ASCII encoding
                original_filename = line.split("filename=")[1].replace('"', "")
                # Create ASCII-safe filename for S3
                filename = original_filename.encode("ascii", "ignore").decode()

                logger.info(f"Found file upload: {original_filename}")
                logger.info(f"Safe filename for S3: {filename}")

                # Find the image data (starts a few lines after filename)
                image_start = i + 3  # Skip headers after filename
                if image_start < len(lines):
                    # Join the remaining lines as image data
                    image_data = "\r\n".join(lines[image_start:])
                    # Remove the final boundary
                    if "------" in image_data:
                        image_data = image_data.split("------")[0]
                    logger.info(
                        f"Image data extracted, length: {len(image_data) if image_data else 0}"
                    )
                break

        if not image_data or not filename:
            logger.error("No image data or filename found in multipart form")
            return error_response("Nenhuma imagem encontrada no upload")

        return process_image_upload(image_data, filename, original_filename)

    except Exception as e:
        logger.error(f"Error processing multipart form: {str(e)}")
        return error_response(f"Erro ao processar formulário: {str(e)}")


def process_image_upload(image_data, safe_filename, original_filename):
    """Process image upload with safe S3 filename"""
    logger.info("=== IMAGE UPLOAD PROCESSING START ===")

    try:
        request_id = str(uuid.uuid4())
        file_name = f"images/{request_id}_{safe_filename}"

        logger.info(f"Request ID: {request_id}")
        logger.info(f"S3 File Key: {file_name}")
        logger.info(f"Original filename: {original_filename}")
        logger.info(f"Safe filename: {safe_filename}")

        # Clean the image data - remove any remaining boundary text
        clean_image_data = image_data.strip()
        if clean_image_data.endswith("--"):
            clean_image_data = clean_image_data[:-2].strip()

        logger.info(f"Cleaned image data length: {len(clean_image_data)}")

        # Handle base64 image data if it's data URL format
        if isinstance(clean_image_data, str) and clean_image_data.startswith(
            "data:image"
        ):
            logger.info("Detected data URL format, extracting base64 data...")
            clean_image_data = clean_image_data.split(",")[1]

        # Convert to bytes for S3 upload
        if isinstance(clean_image_data, str):
            logger.info("Converting string image data to bytes...")
            try:
                image_bytes = base64.b64decode(clean_image_data)
                logger.info(f"Image bytes size: {len(image_bytes)}")
            except Exception as e:
                logger.error(f"Base64 decode error: {str(e)}")
                # If it's not base64, try direct encoding
                image_bytes = clean_image_data.encode("utf-8")
        else:
            image_bytes = clean_image_data

        # Upload to S3 with safe metadata
        logger.info(f"Uploading to S3 bucket: {bucket_name}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=image_bytes,
            ContentType="image/jpeg",
            Metadata={
                "original-filename": original_filename.encode(
                    "ascii", "xmlcharrefreplace"
                ).decode(),
                "request-id": request_id,
            },
        )

        image_url = f"s3://{bucket_name}/{file_name}"
        logger.info(f"Image uploaded successfully: {image_url}")

        # Save to DynamoDB
        logger.info("Saving to DynamoDB...")
        table.put_item(
            Item={
                "requestId": request_id,
                "status": "PROCESSING",
                "createdAt": datetime.utcnow().isoformat(),
                "imageS3Key": file_name,
                "originalFilename": original_filename,
                "safeFilename": safe_filename,
                "imageUrl": image_url,
                "updatedAt": datetime.utcnow().isoformat(),
            }
        )

        # Send to SQS
        logger.info("Sending message to SQS...")
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(
                {
                    "requestId": request_id,
                    "imageS3Key": file_name,
                    "bucketName": bucket_name,
                    "imageUrl": image_url,
                    "originalFilename": original_filename,
                }
            ),
        )

        logger.info("=== IMAGE UPLOAD PROCESSING COMPLETED ===")

        return success_response(
            {
                "requestId": request_id,
                "status": "PROCESSING",
                "message": "Imagem recebida e em processamento",
            }
        )

    except Exception as e:
        logger.error(f"Error processing image upload: {str(e)}")
        return error_response(f"Erro ao processar imagem: {str(e)}")


def success_response(data):
    """Return successful API response with CORS headers"""
    response = {"statusCode": 200, "headers": CORS_HEADERS, "body": json.dumps(data)}
    logger.info(f"Returning success response: {data}")
    return response


def error_response(message, status_code=500):
    """Return error API response with CORS headers"""
    response = {
        "statusCode": status_code,
        "headers": CORS_HEADERS,
        "body": json.dumps({"error": message}),
    }
    logger.error(f"Returning error response ({status_code}): {message}")
    return response
