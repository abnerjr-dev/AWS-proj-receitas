import json
import boto3
import os
from datetime import datetime
import urllib.request

rekognition = boto3.client("rekognition")
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
bedrock = boto3.client("bedrock-runtime")

table = dynamodb.Table(os.environ["DYNAMODB_TABLE"])
model_arn = os.environ["REKOGNITION_MODEL_ARN"]


def lambda_handler(event, context):
    print("SQS Event received:", json.dumps(event))

    processed_count = 0
    errors = []

    try:
        for record in event["Records"]:
            try:
                message_body = json.loads(record["body"])
                request_id = message_body["requestId"]

                print(f"Processing request: {request_id}")

                # Atualiza status para análise
                table.update_item(
                    Key={"requestId": request_id},
                    UpdateExpression="SET #status = :status, updatedAt = :now",
                    ExpressionAttributeNames={"#status": "status"},
                    ExpressionAttributeValues={
                        ":status": "ANALYZING_IMAGE",
                        ":now": datetime.utcnow().isoformat(),
                    },
                )

                # Detecta ingredientes
                ingredients = detect_ingredients(message_body)

                # Atualiza status para geração de receitas
                table.update_item(
                    Key={"requestId": request_id},
                    UpdateExpression="SET #status = :status, ingredients = :ing, updatedAt = :now",
                    ExpressionAttributeNames={"#status": "status"},
                    ExpressionAttributeValues={
                        ":status": "GENERATING_RECIPES",
                        ":ing": ingredients,
                        ":now": datetime.utcnow().isoformat(),
                    },
                )

                # Gera receitas
                recipes = generate_recipes_with_bedrock(ingredients)

                # Salva resultado final
                table.update_item(
                    Key={"requestId": request_id},
                    UpdateExpression="SET #status = :status, recipes = :rec, updatedAt = :now",
                    ExpressionAttributeNames={"#status": "status"},
                    ExpressionAttributeValues={
                        ":status": "COMPLETED",
                        ":rec": recipes,
                        ":now": datetime.utcnow().isoformat(),
                    },
                )

                processed_count += 1
                print(f"Successfully processed request: {request_id}")

            except Exception as e:
                error_msg = f"Error processing record {record.get('messageId', 'unknown')}: {str(e)}"
                errors.append(error_msg)
                print(error_msg)

                # Marca como erro no DynamoDB
                if "request_id" in locals():
                    table.update_item(
                        Key={"requestId": request_id},
                        UpdateExpression="SET #status = :status, errorMessage = :err, updatedAt = :now",
                        ExpressionAttributeNames={"#status": "status"},
                        ExpressionAttributeValues={
                            ":status": "ERROR",
                            ":err": str(e),
                            ":now": datetime.utcnow().isoformat(),
                        },
                    )

        result = {"processedCount": processed_count, "errors": errors}

        if errors:
            return {"statusCode": 207, "body": json.dumps(result)}  # Multi-status
        else:
            return {"statusCode": 200, "body": json.dumps(result)}

    except Exception as e:
        print(f"Fatal error processing batch: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


def detect_ingredients(message):
    """Detecta ingredientes usando Rekognition Custom Labels"""
    try:
        image = {}

        if "imageS3Key" in message:
            # Imagem do S3
            image = {
                "S3Object": {
                    "Bucket": message["bucketName"],
                    "Name": message["imageS3Key"],
                }
            }
        elif "imageUrl" in message:
            # Imagem por URL
            image_bytes = download_image(message["imageUrl"])
            image = {"Bytes": image_bytes}
        else:
            raise ValueError("Nenhuma fonte de imagem especificada")

        response = rekognition.detect_custom_labels(
            ProjectVersionArn=model_arn, Image=image, MinConfidence=70.0
        )

        # Processa resultados
        ingredients = []
        for label in response["CustomLabels"]:
            if label["Confidence"] > 70.0:
                ingredient = {
                    "name": label["Name"],
                    "confidence": round(label["Confidence"], 2),
                    "geometry": label.get("Geometry", {}),
                }
                ingredients.append(ingredient)

        # Ordena por confiança
        ingredients.sort(key=lambda x: x["confidence"], reverse=True)

        print(
            f"Detected {len(ingredients)} ingredients: {[ing['name'] for ing in ingredients]}"
        )
        return ingredients

    except rekognition.exceptions.ResourceNotFoundException:
        raise Exception(
            "Modelo do Rekognition não encontrado. Verifique se o modelo está treinado e deployado."
        )
    except Exception as e:
        print(f"Error in Rekognition detection: {str(e)}")
        raise e


def download_image(image_url):
    """Baixa imagem de URL"""
    try:
        with urllib.request.urlopen(image_url) as response:
            return response.read()
    except Exception as e:
        raise Exception(f"Erro ao baixar imagem da URL: {str(e)}")


def generate_recipes_with_bedrock(ingredients):
    """Gera receitas usando Amazon Bedrock"""
    try:
        if not ingredients:
            return "Nenhum ingrediente detectado. Tente uma foto mais clara dos ingredientes."

        ingredient_list = ", ".join(
            [ing["name"] for ing in ingredients[:10]]
        )  # Limita a 10 ingredientes

        prompt = f"""
        Com base nos seguintes ingredientes detectados: {ingredient_list}
        
        Gere 2 receitas criativas, práticas e deliciosas. Para cada receita, inclua:
        
        **NOME DA RECEITA**
        - Tempo de preparo estimado
        - Dificuldade (Fácil, Médio, Difícil)
        - Ingredientes necessários (incluindo quantidades)
        - Modo de preparo passo a passo
        - Dicas e variações
        
        Formate a resposta de forma clara e organizada. Use ** para títulos e - para listas.
        Se algum ingrediente essencial estiver faltando, sugira substituições.
        Seja criativo e incentive o usuário a cozinhar!
        """

        # Configuração para Claude model
        body = {
            "prompt": f"\n\nHuman: {prompt}\n\nAssistant:",
            "max_tokens_to_sample": 2000,
            "temperature": 0.7,
            "top_p": 0.9,
            "stop_sequences": ["\n\nHuman:"],
        }

        response = bedrock.invoke_model(
            modelId="anthropic.claude-v2", body=json.dumps(body)
        )

        response_body = json.loads(response["body"].read())
        recipes = response_body["completion"].strip()

        print("Recipes generated successfully")
        return recipes

    except Exception as e:
        print(f"Error generating recipes with Bedrock: {str(e)}")
        return error_response(
            f"**Erro ao gerar receitas**\n\nDesculpe, houve um erro ao gerar as receitas. Os ingredientes detectados foram: {ingredient_list}. Tente novamente mais tarde."
        )


def error_response(message):
    return {
        "statusCode": 400,
        "body": json.dumps({"error": message}),
        "headers": {"Content-Type": "application/json"},
    }
