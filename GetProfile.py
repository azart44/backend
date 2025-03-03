import json
import os
import boto3
import base64
import logging
from decimal import Decimal
import traceback
from datetime import datetime, timedelta

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Variables d'environnement
TABLE_NAME = os.environ.get('USERS_TABLE', 'chordora-users')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'chordora-users')
DEFAULT_IMAGE_KEY = os.environ.get('DEFAULT_IMAGE_KEY', 'public/default-profile.jpg')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE_NAME)
s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def generate_presigned_url(bucket, object_key, expiration=3600):
    """
    Génère une URL présignée pour accéder à un objet S3
    """
    try:
        # Vérifier si l'objet existe avant de générer l'URL
        try:
            s3_resource.Object(bucket, object_key).load()
        except Exception as e:
            logger.warning(f"L'objet S3 {object_key} n'existe pas: {str(e)}")
            return None
            
        # Générer l'URL présignée
        response = s3.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket,
                'Key': object_key
            },
            ExpiresIn=expiration
        )
        logger.info(f"URL présignée générée pour {object_key}: {response[:100]}...")
        return response
    except Exception as e:
        logger.error(f"Erreur lors de la génération de l'URL présignée: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def check_image_exists(bucket, user_id):
    """
    Vérifie si une image de profil existe pour l'utilisateur et retourne son chemin
    """
    try:
        # Tester différentes extensions de fichier possibles
        for ext in ['.jpg', '.png', '.jpeg', '.webp', '.gif', '']:
            profile_image_key = f"public/users/{user_id}/profile-image{ext}"
            try:
                s3_resource.Object(bucket, profile_image_key).load()
                logger.info(f"Image de profil trouvée: {profile_image_key}")
                return profile_image_key
            except Exception:
                continue  # Essayer l'extension suivante
        
        logger.info(f"Aucune image de profil trouvée pour l'utilisateur {user_id}")
        return None
    except Exception as e:
        logger.error(f"Erreur lors de la vérification de l'image: {str(e)}")
        return None

def convert_dynamodb_to_profile(item):
    """
    Convertit un élément DynamoDB en profil utilisateur structuré.
    Gère à la fois les objets DynamoDB natifs et les dictionnaires JSON standards.
    """
    try:
        # Vérifier si l'item est déjà au format dictionnaire standard
        profile = {}
        
        # Champs de base
        profile['userId'] = item.get('userId', '')
        profile['email'] = item.get('email', '')
        profile['username'] = item.get('username', '') or f"User_{profile['userId'][-6:]}"
        profile['bio'] = item.get('bio', '')
        profile['userType'] = item.get('userType', '')
        profile['experienceLevel'] = item.get('experienceLevel', '')
        profile['location'] = item.get('location', '')
        profile['software'] = item.get('software', '')
        profile['musicalMood'] = item.get('musicalMood', '')
        
        # Champs de type liste
        profile['musicGenres'] = item.get('musicGenres', [])
        profile['tags'] = item.get('tags', [])
        profile['equipment'] = item.get('equipment', [])
        profile['favoriteArtists'] = item.get('favoriteArtists', [])
        
        # Champs d'URLs et d'images
        # Ne pas utiliser directement l'URL stockée, mais générer une URL présignée
        profile['profileImageUrl'] = ''  # On va la remplir ci-dessous
        profile['profileImageBase64'] = item.get('profileImageBase64', '')
        profile['bannerImageUrl'] = item.get('bannerImageUrl', '')
        
        # Liens sociaux
        profile['socialLinks'] = item.get('socialLinks', {})
        
        # Flags et timestamps
        profile['profileCompleted'] = item.get('profileCompleted', False)
        profile['createdAt'] = item.get('createdAt', 0)
        profile['updatedAt'] = item.get('updatedAt', 0)
        
        # Générer une URL présignée pour l'image de profil
        user_id = profile['userId']
        
        # Vérifier si l'utilisateur a une image de profil (utiliser le chemin stocké ou chercher)
        profile_image_key = None
        if 'profileImageUrl' in item and item['profileImageUrl']:
            # Essayer d'extraire le chemin S3 de l'URL stockée
            try:
                stored_url = item['profileImageUrl']
                if BUCKET_NAME in stored_url and 'amazonaws.com' in stored_url:
                    # Extraire la clé du chemin complet
                    parts = stored_url.split(f"{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/")
                    if len(parts) > 1:
                        profile_image_key = parts[1]
                        logger.info(f"Clé d'image extraite de l'URL stockée: {profile_image_key}")
                else:
                    # Si ce n'est pas une URL S3 classique, utiliser comme clé directement
                    profile_image_key = stored_url
                    logger.info(f"Utilisation de l'URL stockée comme clé: {profile_image_key}")
            except Exception as e:
                logger.error(f"Erreur lors de l'extraction de la clé S3: {str(e)}")
                # On continuera avec la recherche ci-dessous
        
        # Si on n'a pas pu extraire la clé, essayer de chercher l'image
        if not profile_image_key:
            profile_image_key = check_image_exists(BUCKET_NAME, user_id)
            logger.info(f"Résultat de la recherche d'image: {profile_image_key}")
        
        # Générer l'URL présignée ou utiliser l'image par défaut
        if profile_image_key:
            # Générer l'URL présignée pour cette image
            presigned_url = generate_presigned_url(BUCKET_NAME, profile_image_key)
            if presigned_url:
                profile['profileImageUrl'] = presigned_url
                logger.info(f"URL présignée générée pour {user_id}: {presigned_url[:50]}...")
            else:
                logger.error(f"Impossible de générer une URL présignée pour {profile_image_key}")
                # Utiliser l'image par défaut en cas d'échec
                presigned_url = generate_presigned_url(BUCKET_NAME, DEFAULT_IMAGE_KEY)
                if presigned_url:
                    profile['profileImageUrl'] = presigned_url
        else:
            # Utiliser l'image par défaut
            presigned_url = generate_presigned_url(BUCKET_NAME, DEFAULT_IMAGE_KEY)
            if presigned_url:
                profile['profileImageUrl'] = presigned_url
                logger.info(f"URL de l'image par défaut utilisée pour {user_id}")
        
        return profile
    except Exception as e:
        logger.error(f"Erreur de conversion du profil: {str(e)}")
        logger.error(traceback.format_exc())
        # Au lieu de retourner None, retournons un profil minimal
        user_id = item.get('userId', '')
        return {
            'userId': user_id,
            'email': 'error@conversion.failed',
            'username': f"User_{user_id[-6:]}",
            'profileCompleted': False
        }

def get_cors_headers(event):
    """
    Génère les en-têtes CORS dynamiques basés sur l'origine de la requête.
    """
    # Obtenez l'origine de la requête si elle existe
    origin = None
    if 'headers' in event and event['headers']:
        origin = event['headers'].get('origin') or event['headers'].get('Origin')
    
    # Définir l'origine autorisée
    allowed_origin = origin if origin else 'http://localhost:3000'
    
    # Ne jamais utiliser '*' avec credentials
    return {
        'Access-Control-Allow-Origin': allowed_origin,
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

def lambda_handler(event, context):
    """
    Gestionnaire principal de la Lambda pour récupérer un profil utilisateur.
    """
    logger.info(f"Événement reçu: {json.dumps(event)}")
    cors_headers = get_cors_headers(event)

    # Requête OPTIONS pour CORS
    if event['httpMethod'] == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('Preflight request successful')
        }

    try:
        # Extraire l'ID utilisateur du chemin ou des paramètres
        path_parameters = event.get('pathParameters', {}) or {}
        query_parameters = event.get('queryStringParameters', {}) or {}
        
        user_id = path_parameters.get('userId') or query_parameters.get('userId')

        # Si aucun ID fourni, essayer d'utiliser l'ID de l'utilisateur authentifié
        if not user_id:
            try:
                user_id = event['requestContext']['authorizer']['claims']['sub']
                logger.info(f"Utilisation de l'ID utilisateur authentifié: {user_id}")
            except KeyError:
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps('User ID is required')
                }
            
        logger.info(f"Récupération du profil pour userId: {user_id}")

        # Récupérer l'élément dans DynamoDB
        response = table.get_item(Key={'userId': user_id})
        
        if 'Item' not in response:
            logger.warn(f"Aucun profil trouvé pour l'utilisateur: {user_id}")
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps('Profil utilisateur non trouvé')
            }

        # Convertir l'élément DynamoDB en profil structuré
        profile = convert_dynamodb_to_profile(response['Item'])
        
        if not profile:
            logger.error(f"Échec de la conversion du profil pour: {user_id}")
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps('Erreur lors du traitement du profil')
            }

        # Retourner le profil
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(profile, cls=DecimalEncoder)
        }

    except Exception as e:
        logger.error(f"Erreur non gérée: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'message': 'Erreur interne du serveur',
                'error': str(e)
            })
        }