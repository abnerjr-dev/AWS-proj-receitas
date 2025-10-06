import json
import boto3
import os
from datetime import datetime
import logging
from decimal import Decimal

# Configuração de logging estruturado
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Clients AWS
rekognition = boto3.client("rekognition")
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
bedrock = boto3.client("bedrock-runtime")

# Configurações
table = dynamodb.Table(os.environ["DYNAMODB_TABLE"])
model_arn = os.environ["REKOGNITION_MODEL_ARN"]

# Configuração do Modelo Bedrock (Free Tier)
BEDROCK_MODEL_ID = "amazon.titan-text-lite-v1"  # ✅ Free Tier


def lambda_handler(event, context):
    logger.info(
        "SQS event received",
        extra={
            "record_count": len(event.get("Records", [])),
            "message_ids": [r.get("messageId") for r in event.get("Records", [])],
        },
    )

    try:
        for record in event["Records"]:
            message_body = json.loads(record["body"])
            request_id = message_body["requestId"]

            logger.info("🟡 STARTING PROCESSING", extra={"request_id": request_id})

            # Atualiza status no DynamoDB
            update_dynamo_status(request_id, "ANALYZING_IMAGE")
            logger.info("🟡 STATUS UPDATED TO ANALYZING_IMAGE")

            # Detecta ingredientes na imagem
            ingredients = detect_ingredients(message_body)
            logger.info("🟡 INGREDIENTS DETECTED", extra={"count": len(ingredients)})

            # Gera receitas usando Bedrock (Free Tier)
            recipes = generate_recipes_with_bedrock(ingredients, request_id)
            logger.info(
                "🟡 RECIPES GENERATED", extra={"length": len(recipes) if recipes else 0}
            )

            # Salva resultado no DynamoDB
            update_dynamo_result(request_id, ingredients, recipes)
            logger.info("🟢 PROCESSING COMPLETED")

        return {"statusCode": 200, "body": "Processamento concluído"}

    except Exception as e:
        logger.error(
            "Error processing SQS message",
            extra={
                "error": str(e),
                "error_type": type(e).__name__,
                "request_id": locals().get("request_id", "unknown"),
            },
        )

        if "request_id" in locals():
            update_dynamo_error(request_id, str(e))

        return {
            "statusCode": 200,
            "body": "Erro processado - mensagem removida da fila",
        }


def convert_floats_to_decimals(obj):
    """Converte recursivamente todos os floats para Decimals"""
    if isinstance(obj, dict):
        return {k: convert_floats_to_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimals(v) for v in obj]
    elif isinstance(obj, float):
        return Decimal(str(obj))
    else:
        return obj


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


def generate_recipes_with_bedrock(ingredients, request_id):
    """Gera receitas usando Amazon Bedrock - COM FALLBACK MELHORADO"""
    try:
        valid_ingredients = []
        for ing in ingredients:
            if ing.get("source") != "fallback" or ing.get("confidence", 0) > 0.1:
                valid_ingredients.append(ing)

        if not valid_ingredients:
            logger.warning("No valid ingredients, using creative fallback")
            return generate_creative_fallback_recipe()

        ingredient_list = ", ".join([ing["name"] for ing in valid_ingredients])

        logger.info(
            "DEBUG - Ingredients for recipe generation",
            extra={
                "all_ingredients": ingredients,
                "valid_ingredients": valid_ingredients,
                "ingredient_list": ingredient_list,
            },
        )

        # Prompt otimizado para modelos free tier
        prompt = f"""
Instruções: Gere uma receita simples baseada nos ingredientes: {ingredient_list}

Formato da resposta:
NOME DA RECEITA
Tempo de preparo: 
Dificuldade: 
Ingredientes:
- 
Modo de preparo:
1. 
2. 

Mantenha a resposta concisa e direta. Use apenas os ingredientes listados quando possível.
"""

        logger.info(
            "Generating recipes with Bedrock Free Tier",
            extra={
                "request_id": request_id,
                "model_id": BEDROCK_MODEL_ID,
                "ingredient_count": len(ingredients),
            },
        )

        # Configuração para Titan Text (Free Tier)
        body = {
            "inputText": prompt,
            "textGenerationConfig": {
                "maxTokenCount": 500,
                "stopSequences": [],
                "temperature": 0.7,
                "topP": 0.9,
            },
        }

        # Invoca o modelo Bedrock
        response = bedrock.invoke_model(modelId=BEDROCK_MODEL_ID, body=json.dumps(body))

        # Processa a resposta
        response_body = json.loads(response["body"].read())
        recipes = response_body["results"][0]["outputText"]

        logger.info(
            "Recipes generated successfully with Free Tier",
            extra={
                "request_id": request_id,
                "model_used": BEDROCK_MODEL_ID,
                "response_length": len(recipes),
            },
        )

        return recipes.strip()

    except bedrock.exceptions.AccessDeniedException:
        logger.error(
            "Access denied to Bedrock model", extra={"model_id": BEDROCK_MODEL_ID}
        )
        return "Erro: Acesso ao modelo de IA não autorizado. Verifique as permissões do Bedrock."

    except bedrock.exceptions.ThrottlingException:
        logger.warning(
            "Bedrock throttling occurred", extra={"model_id": BEDROCK_MODEL_ID}
        )
        return "Sistema ocupado. Por favor, tente novamente em alguns instantes."

    except Exception as e:
        logger.error(
            "Error generating recipes with Bedrock",
            extra={
                "error": str(e),
                "error_type": type(e).__name__,
                "model_id": BEDROCK_MODEL_ID,
                "request_id": request_id,
            },
        )
        return f"Receita gerada automaticamente com os ingredientes: {ingredient_list}. Sugestão: refogue os ingredientes e tempere a gosto."


def generate_creative_fallback_recipe():
    """Gera uma receita criativa quando não há ingredientes detectados"""
    return """
**Receita Surpresa da Geladeira**

Tempo: 15 minutos
Dificuldade: Fácil

Ingredientes sugeridos:
- Vegetais que você tenha na geladeira
- Temperos a gosto (alho, cebola, ervas)
- Azeite ou óleo
- Sal e pimenta

Modo de preparo:
1. Verifique quais vegetais você tem disponível
2. Pique os vegetais em pedaços uniformes
3. Aqueça uma panela com azeite
4. Refogue os vegetais começando pelos mais firmes
5. Tempere com sal, pimenta e suas ervas favoritas
6. Sirva como acompanhamento ou com arroz

Dica: Esta é uma receita flexível! Use o que você tiver disponível.
"""


def update_dynamo_status(request_id, status):
    """Atualiza status no DynamoDB"""
    try:
        table.update_item(
            Key={"requestId": request_id},
            UpdateExpression="SET #status = :status, updatedAt = :now",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": status,
                ":now": datetime.utcnow().isoformat(),
            },
        )
    except Exception as e:
        logger.error(
            "Error updating DynamoDB status",
            extra={"request_id": request_id, "status": status, "error": str(e)},
        )
        raise


def detect_ingredients(message):
    """Detecta ingredientes usando Rekognition Custom Labels - COM FALLBACK"""
    try:
        if "imageS3Key" in message:
            s3_key = message["imageS3Key"]
            bucket_name = message["bucketName"]

            logger.info(
                "Detecting ingredients from S3",
                extra={
                    "s3_bucket": bucket_name,
                    "s3_key": s3_key,
                    "model_arn": model_arn,
                },
            )

            try:
                response = rekognition.detect_custom_labels(
                    ProjectVersionArn=model_arn,
                    Image={"S3Object": {"Bucket": bucket_name, "Name": s3_key}},
                    MinConfidence=60.0,
                )

                ingredients = process_rekognition_results(response)

                if ingredients:
                    logger.info(
                        "Ingredients detected successfully",
                        extra={
                            "ingredients_count": len(ingredients),
                            "ingredients": [ing["name"] for ing in ingredients],
                        },
                    )
                    return ingredients
                else:
                    logger.warning("No ingredients detected by Rekognition")
                    return generate_fallback_ingredients()

            except rekognition.exceptions.ResourceNotFoundException:
                logger.error("Rekognition model not found or not trained")
                return generate_fallback_ingredients()
            except Exception as e:
                logger.error("Rekognition API error", extra={"error": str(e)})
                return generate_fallback_ingredients()

        else:
            logger.warning("No imageS3Key in message, using fallback")
            return generate_fallback_ingredients()

    except Exception as e:
        logger.error("Error in ingredient detection", extra={"error": str(e)})
        return generate_fallback_ingredients()


def generate_fallback_ingredients():
    """Gera ingredientes de fallback quando Rekognition falha"""
    fallback_ingredients = [
        {"name": "vegetais variados", "confidence": 0.5, "source": "fallback"},
        {"name": "ingredientes frescos", "confidence": 0.5, "source": "fallback"},
    ]
    logger.info(
        "Using fallback ingredients",
        extra={"fallback_count": len(fallback_ingredients)},
    )
    return fallback_ingredients


def process_rekognition_results(response):
    """Processa e formata resultados do Rekognition"""
    ingredients = []
    for label in response.get("CustomLabels", []):
        if label["Confidence"] > 70.0:
            ingredient = {
                "name": label["Name"],
                "confidence": Decimal(str(label["Confidence"])).quantize(
                    Decimal("0.01")
                ),
                "geometry": convert_floats_to_decimals(label.get("Geometry", {})),
            }
            ingredients.append(ingredient)

    # Ordena por confiança
    ingredients.sort(key=lambda x: x["confidence"], reverse=True)
    return ingredients


def update_dynamo_result(request_id, ingredients, recipes):
    """Atualiza DynamoDB com resultados finais"""
    try:
        logger.info(
            "DEBUG - Data before DynamoDB save",
            extra={
                "ingredients_type": type(ingredients),
                "ingredients_sample": str(ingredients)[:200] if ingredients else None,
                "recipes_type": type(recipes),
                "request_id": request_id,
            },
        )

        # Converte todos os dados antes de salvar
        safe_ingredients = convert_floats_to_decimals(ingredients)
        safe_recipes = recipes

        table.update_item(
            Key={"requestId": request_id},
            UpdateExpression="SET #status = :status, ingredients = :ing, recipes = :rec, updatedAt = :now, completedAt = :now",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "COMPLETED",
                ":ing": safe_ingredients,
                ":rec": safe_recipes,
                ":now": datetime.utcnow().isoformat(),
            },
        )
    except Exception as e:
        logger.error(
            "Error updating DynamoDB with results",
            extra={"request_id": request_id, "error": str(e)},
        )
        raise


def update_dynamo_error(request_id, error_message):
    """Atualiza DynamoDB com erro"""
    try:
        table.update_item(
            Key={"requestId": request_id},
            UpdateExpression="SET #status = :status, errorMessage = :err, updatedAt = :now",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":status": "ERROR",
                ":err": error_message,
                ":now": datetime.utcnow().isoformat(),
            },
        )
    except Exception as e:
        logger.error(
            "Error updating DynamoDB with error",
            extra={
                "request_id": request_id,
                "update_error": str(e),
                "original_error": error_message,
            },
        )
        raise
