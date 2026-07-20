<img width="728" height="449" alt="Arquitetura-draw io" src="https://github.com/user-attachments/assets/f2eeb500-d76e-4b26-8e8a-6161c09c31be" />

# 🍳 ChefIA

Aplicação serverless que recebe fotos de ingredientes e sugere receitas usando apenas o que foi identificado na imagem.

## Como funciona

1. O usuário envia uma foto pelo frontend (S3)
2. API Gateway recebe a requisição e dispara Lambda 1
3. Lambda 1 salva a imagem no S3 Bucket e registra o pedido no DynamoDB
4. A mensagem vai para uma fila SQS
5. Lambda 2 consome a fila, usa Rekognition para identificar os ingredientes e Bedrock para gerar receitas
6. Lambda 3 salva o resultado e o usuário recebe as sugestões

## Stack

| Serviço | Função |
|---|---|
| **S3** | Frontend estático + armazenamento de imagens |
| **API Gateway** | Endpoint REST |
| **Lambda** | Processamento serverless |
| **SQS** | Fila assíncrona entre upload e IA |
| **Rekognition** | Detecção de objetos na imagem |
| **Bedrock** | Geração de receitas por IA |
| **DynamoDB** | Metadados e status dos pedidos |

## Possíveis melhorias

- [ ] Cache de receitas no DynamoDB para ingredientes repetidos
- [ ] Notificação em tempo real (WebSocket) quando o processamento terminar
- [ ] Validação de imagem antes do envio
- [ ] Suporte a múltiplos idiomas
- [ ] Histórico de receitas por usuário

## Autor

Abner


# Possíveis melhorias:
1. Usar Cloudfront para distribuição do site estático
2. Treinar mais o modelo custom Lables para gerar mais coonfiança
3. substituir o DynamoDB por um serviço de cache (Elasticache)
4. Permitir que usuario digite ingredientes alem da foto
