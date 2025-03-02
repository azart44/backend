import json
import boto3
from botocore.exceptions import ClientError
import base64
import logging
from decimal import Decimal
import os
import datetime
import traceback

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Variables d'environnement
TABLE_NAME = os.environ.get('USERS_TABLE', 'chordora-users')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'chordora-users')
DEFAULT_PROFILE_IMAGE_KEY = os.environ.get('DEFAULT_PROFILE_IMAGE_KEY', 'public/default-profile')

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE_NAME)
s3 = boto3.client('s3')

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def get_cors_headers():
    return {
        'Access-Control-Allow-Origin': 'http://localhost:3000',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'POST,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

def lambda_handler(event, context):
    logger.info(f"Événement reçu: {json.dumps(event)}")
    cors_headers = get_cors_headers()
    
    if event['httpMethod'] == 'OPTIONS':
        logger.info("Requête OPTIONS reçue")
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('Preflight request successful')
        }
    
    try:
        # Extraction de l'ID utilisateur du token JWT
        user_id = event['requestContext']['authorizer']['claims']['sub']
        logger.info(f"ID utilisateur extrait: {user_id}")
        
        if event['httpMethod'] == 'POST':
            return handle_update_profile(event, cors_headers, user_id)
        else:
            return {
                'statusCode': 405,
                'headers': cors_headers,
                'body': json.dumps('Method Not Allowed')
            }
    except KeyError as e:
        logger.error(f"Impossible d'extraire l'ID utilisateur du token JWT: {str(e)}")
        return {
            'statusCode': 401,
            'headers': cors_headers,
            'body': json.dumps('Unauthorized: Unable to extract user ID')
        }
    except Exception as e:
        # Capture et log toutes les exceptions non gérées
        logger.error(f"Erreur non gérée: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,  # Important d'inclure les en-têtes CORS même en cas d'erreur
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }

def handle_update_profile(event, cors_headers, user_id):
    logger.info(f"Début de handle_update_profile pour l'utilisateur: {user_id}")
    try:
        body = json.loads(event['body'])
        profile_data = body['profileData']
        
        logger.info(f"Données de profil reçues: {json.dumps(profile_data)}")

        # S'assurer que l'utilisateur ne peut pas modifier l'ID utilisateur
        profile_data['userId'] = user_id

        # Vérifier si l'utilisateur existe déjà
        existing_user = table.get_item(Key={'userId': user_id}).get('Item')
        
        if not existing_user:
            logger.info(f"Création d'un nouveau profil utilisateur pour {user_id}")
            # Si aucune image n'est fournie, utiliser l'image par défaut
            profile_data['profileImageUrl'] = f"https://{BUCKET_NAME}.s3.amazonaws.com/{DEFAULT_PROFILE_IMAGE_KEY}"
            profile_data['profileCompleted'] = False
            profile_data['createdAt'] = int(datetime.datetime.now().timestamp())
        else:
            logger.info(f"Mise à jour du profil existant pour {user_id}")
            # Fusionner les données existantes avec les nouvelles
            profile_data = {**existing_user, **profile_data}
        
        # Ajouter un timestamp de mise à jour
        profile_data['updatedAt'] = int(datetime.datetime.now().timestamp())

        # Traiter l'image de profil si présente
        if 'profileImageBase64' in profile_data:
            try:
                image_data = profile_data['profileImageBase64']
                # Vérifier si l'image est déjà en format base64 avec en-tête ou non
                if ',' in image_data:
                    # Format: data:image/png;base64,BASE64_DATA
                    image_content = base64.b64decode(image_data.split(',')[1])
                    content_type = image_data.split(',')[0].split(':')[1].split(';')[0]
                else:
                    # Format: BASE64_DATA
                    image_content = base64.b64decode(image_data)
                    content_type = 'image/png'  # Valeur par défaut
                
                # Uploader l'image dans S3
                s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=f"public/users/{user_id}/profile-image",
                    Body=image_content,
                    ContentType=content_type
                )
                profile_data['profileImageUrl'] = f"https://{BUCKET_NAME}.s3.amazonaws.com/public/users/{user_id}/profile-image"
                logger.info(f"Image de profil mise à jour pour l'utilisateur {user_id}")
            except ClientError as e:
                logger.error(f"Erreur lors de l'upload de l'image: {str(e)}")
            except Exception as e:
                logger.error(f"Erreur inattendue lors du traitement de l'image: {str(e)}")
            
            # Supprimer les données base64 pour économiser de l'espace dans DynamoDB
            del profile_data['profileImageBase64']
        else:
            # Si aucune image n'est fournie pour un nouveau profil ou lors d'une mise à jour
            if not profile_data.get('profileImageUrl'):
                profile_data['profileImageUrl'] = f"https://{BUCKET_NAME}.s3.amazonaws.com/{DEFAULT_PROFILE_IMAGE_KEY}"

        # Enregistrer les données dans DynamoDB
        logger.info(f"Mise à jour du profil dans DynamoDB pour l'utilisateur {user_id}")
        table.put_item(Item=profile_data)

        # Récupérer le profil mis à jour pour confirmer
        updated_profile = table.get_item(Key={'userId': user_id})['Item']
        logger.info(f"Profil mis à jour récupéré: {json.dumps(updated_profile, cls=DecimalEncoder)}")

        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'message': 'Profile updated successfully',
                'updatedProfile': updated_profile
            }, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Erreur lors de la mise à jour du profil: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'message': f'Error updating profile: {str(e)}',
                'updatedProfile': None
            })
        }