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
        # Vérifie d'abord si l'objet existe dans S3
        try:
            s3_resource.Object(bucket, object_key).load()
            object_exists = True
        except Exception:
            object_exists = False
            logger.warning(f"L'objet {object_key} n'existe pas dans le bucket {bucket}")
            
        if not object_exists:
            # Si l'image de profil n'existe pas, utiliser l'image par défaut
            object_key = DEFAULT_IMAGE_KEY
            
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

def convert_dynamodb_to_profile(item):
    """
    Convertit un élément DynamoDB en profil utilisateur structuré.
    Version plus robuste avec vérification des types.
    """
    try:
        # Log l'item en entrée pour le débogage
        logger.info(f"Item à convertir: {json.dumps(item, default=str)}")
        
        # Vérifie si l'item est déjà au format JSON standard (pas de types DynamoDB)
        if isinstance(item.get('userId'), str):
            # L'item est déjà un dictionnaire simple, pas besoin de conversion complexe
            profile = {
                'userId': item.get('userId', ''),
                'email': item.get('email', ''),
                'username': item.get('username', ''),  # Ajout du champ username
                'bio': item.get('bio', ''),
                'userType': item.get('userType', ''),
                'experienceLevel': item.get('experienceLevel', ''),
                'musicGenres': item.get('musicGenres', []),
                'tags': item.get('tags', []),
                'socialLinks': item.get('socialLinks', {}),
                'profileImageUrl': item.get('profileImageUrl', ''),
                'profileCompleted': item.get('profileCompleted', False),
                'location': item.get('location', ''),
                'software': item.get('software', ''),
                'musicalMood': item.get('musicalMood', ''),
                'musicGenre': item.get('musicGenre', ''),
                'favoriteArtists': item.get('favoriteArtists', []),
                'updatedAt': item.get('updatedAt', 0)
            }
            
            # Si username est vide, utiliser une valeur par défaut
            if not profile['username']:
                profile['username'] = f"User_{profile['userId'][-6:]}"
            
            # Générer l'URL présignée pour l'image de profil
            user_id = profile['userId']
            profile_image_key = f"public/users/{user_id}/profile-image"
            presigned_url = generate_presigned_url(BUCKET_NAME, profile_image_key)
            if presigned_url:
                profile['profileImageUrl'] = presigned_url
            
            return profile
        
        # Fonction de sécurité pour extraire les valeurs avec gestion des erreurs
        def safe_extract(item_dict, key, attr_type, default):
            try:
                if key not in item_dict:
                    return default
                if attr_type not in item_dict[key]:
                    return default
                return item_dict[key][attr_type]
            except Exception as e:
                logger.warning(f"Erreur d'extraction pour {key}.{attr_type}: {str(e)}")
                return default
        
        # Version plus sûre avec vérification des types pour DynamoDB
        profile = {
            'userId': safe_extract(item, 'userId', 'S', ''),
            'email': safe_extract(item, 'email', 'S', ''),
            'username': safe_extract(item, 'username', 'S', ''),  # Ajout du champ username
            'bio': safe_extract(item, 'bio', 'S', ''),
            'userType': safe_extract(item, 'userType', 'S', ''),
            'experienceLevel': safe_extract(item, 'experienceLevel', 'S', ''),
            
            # Conversion des listes DynamoDB avec vérification
            'musicGenres': [
                genre.get('S', '') 
                for genre in safe_extract(item, 'musicGenres', 'L', [])
                if genre.get('S')
            ],
            'tags': [
                tag.get('S', '') 
                for tag in safe_extract(item, 'tags', 'L', [])
                if tag.get('S')
            ],
            
            # Conversion des liens sociaux avec vérification
            'socialLinks': {
                k: v.get('S', '') 
                for k, v in safe_extract(item, 'socialLinks', 'M', {}).items()
            },
            
            'profileImageUrl': safe_extract(item, 'profileImageUrl', 'S', ''),
            'profileCompleted': safe_extract(item, 'profileCompleted', 'BOOL', False),
            
            # Champs additionnels
            'location': safe_extract(item, 'location', 'S', ''),
            'software': safe_extract(item, 'software', 'S', ''),
            'musicalMood': safe_extract(item, 'musicalMood', 'S', ''),
            'musicGenre': safe_extract(item, 'musicGenre', 'S', ''),
            
            'favoriteArtists': [
                artist.get('S', '') 
                for artist in safe_extract(item, 'favoriteArtists', 'L', [])
                if artist.get('S')
            ],
        }
        
        # Si username est vide, utiliser une valeur par défaut basée sur l'ID
        if not profile['username']:
            profile['username'] = f"User_{profile['userId'][-6:]}"
        
        # Traitement spécial pour updatedAt qui peut être un nombre
        try:
            profile['updatedAt'] = int(safe_extract(item, 'updatedAt', 'N', 0))
        except (ValueError, TypeError):
            profile['updatedAt'] = 0
            
        # Générer l'URL présignée pour l'image de profil
        user_id = profile['userId']
        profile_image_key = f"public/users/{user_id}/profile-image"
        presigned_url = generate_presigned_url(BUCKET_NAME, profile_image_key)
        if presigned_url:
            profile['profileImageUrl'] = presigned_url
            
        return profile
    except Exception as e:
        logger.error(f"Erreur de conversion du profil: {str(e)}")
        logger.error(traceback.format_exc())
        # Au lieu de retourner None, retournons un profil minimal
        user_id = item.get('userId', {}).get('S', '') if isinstance(item.get('userId'), dict) else item.get('userId', '')
        return {
            'userId': user_id,
            'email': 'error@conversion.failed',
            'username': f"User_{user_id[-6:]}",  # Valeur par défaut pour username
            'profileCompleted': False
        }

def get_cors_headers():
    """
    Génère les en-têtes CORS pour les réponses.
    """
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Access-Control-Allow-Credentials': 'true',
        'Content-Type': 'application/json'
    }

def lambda_handler(event, context):
    """
    Gestionnaire principal de la Lambda pour récupérer un profil utilisateur.
    """
    logger.info(f"Événement reçu: {json.dumps(event)}")
    cors_headers = get_cors_headers()

    # Requête OPTIONS pour CORS
    if event['httpMethod'] == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('Preflight request successful')
        }

    try:
        # Extraire l'ID utilisateur du chemin ou des paramètres
        user_id = (
            event.get('pathParameters', {}).get('userId') or
            event.get('queryStringParameters', {}).get('userId')
        )

        if not user_id:
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

        # Log du profil final pour débogage
        logger.info(f"Profil à renvoyer: {json.dumps(profile, cls=DecimalEncoder)}")

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