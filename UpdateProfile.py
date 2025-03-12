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
DEFAULT_PROFILE_IMAGE_KEY = os.environ.get('DEFAULT_PROFILE_IMAGE_KEY', 'public/default-profile.jpg')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE_NAME)
s3 = boto3.client('s3')

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def get_mime_type(image_content):
    """
    Détermine le type MIME à partir des données binaires de l'image
    """
    if image_content[:2] == b'\xff\xd8':
        return 'image/jpeg'
    elif image_content[:8] == b'\x89PNG\r\n\x1a\n':
        return 'image/png'
    elif image_content[:6] in (b'GIF87a', b'GIF89a'):
        return 'image/gif'
    elif image_content[:4] == b'RIFF' and image_content[8:12] == b'WEBP':
        return 'image/webp'
    else:
        return 'image/jpeg'  # Par défaut

def get_cors_headers(event):
    """
    Génère les en-têtes CORS dynamiques basés sur l'origine de la requête.
    """
    origin = None
    if 'headers' in event and event['headers']:
        origin = event['headers'].get('origin') or event['headers'].get('Origin')
    
    allowed_origin = origin if origin else 'http://localhost:3000'
    
    return {
        'Access-Control-Allow-Origin': allowed_origin,
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

def lambda_handler(event, context):
    logger.info(f"Événement reçu: {json.dumps(event)}")
    cors_headers = get_cors_headers(event)
    
    if event['httpMethod'] == 'OPTIONS':
        logger.info("Requête OPTIONS reçue")
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('Preflight request successful')
        }
    
    try:
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
        logger.error(f"Erreur non gérée: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }

def sanitize_profile_data(profile_data):
    """
    Assainit et valide les données du profil avant de les stocker
    """
    sanitized = {}
    
    for field in ['userId', 'email', 'username', 'bio', 'userType', 
                  'experienceLevel', 'location', 'software', 'musicalMood', 'availabilityStatus']:
        if field in profile_data:
            if field == 'bio' and profile_data.get(field) and len(profile_data[field]) > 150:
                sanitized[field] = profile_data[field][:150]
            else:
                sanitized[field] = profile_data.get(field)
    
    for field in ['musicGenres', 'tags', 'equipment', 'favoriteArtists']:
        if field in profile_data and isinstance(profile_data[field], list):
            if field in ['musicGenres', 'tags'] and len(profile_data[field]) > 3:
                sanitized[field] = profile_data[field][:3]
            else:
                sanitized[field] = profile_data[field]
    
    if 'socialLinks' in profile_data and isinstance(profile_data['socialLinks'], dict):
        sanitized_links = {}
        for platform, url in profile_data['socialLinks'].items():
            if isinstance(url, str) and (url == '' or url.startswith('http')):
                sanitized_links[platform] = url
        
        if sanitized_links:
            sanitized['socialLinks'] = sanitized_links
    
    for field in ['profileCompleted']:
        if field in profile_data:
            sanitized[field] = bool(profile_data[field])
    
    if 'profileImageBase64' in profile_data:
        sanitized['profileImageBase64'] = profile_data['profileImageBase64']
        
    if 'profileImageUrl' in profile_data:
        sanitized['profileImageUrl'] = profile_data['profileImageUrl']
    
    return sanitized

def handle_update_profile(event, cors_headers, user_id):
    logger.info(f"Début de handle_update_profile pour l'utilisateur: {user_id}")
    try:
        body = json.loads(event['body'])
        profile_data = body['profileData']
        
        logger.info(f"Données de profil reçues: {json.dumps({k: '...' if k == 'profileImageBase64' else v for k, v in profile_data.items()})}")
        logger.info(f"Profil contient une image? {'profileImageBase64' in profile_data}")

        profile_data['userId'] = user_id
        
        sanitized_profile_data = sanitize_profile_data(profile_data)
        logger.info(f"Données de profil assainies: {json.dumps({k: '...' if k == 'profileImageBase64' else v for k, v in sanitized_profile_data.items()})}")

        existing_user = table.get_item(Key={'userId': user_id}).get('Item')
        
        if not existing_user:
            logger.info(f"Création d'un nouveau profil utilisateur pour {user_id}")
            if not sanitized_profile_data.get('profileImageUrl') and 'profileImageBase64' not in sanitized_profile_data:
                sanitized_profile_data['profileImageUrl'] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{DEFAULT_PROFILE_IMAGE_KEY}"
            
            sanitized_profile_data['profileCompleted'] = True
            sanitized_profile_data['createdAt'] = int(datetime.datetime.now().timestamp())
            
            logger.info(f"Nouveau profil créé et marqué comme complété pour {user_id}")
        else:
            logger.info(f"Mise à jour du profil existant pour {user_id}")
            logger.info(f"URL d'image existante: {existing_user.get('profileImageUrl')}")
            
            sanitized_profile_data['profileCompleted'] = existing_user.get('profileCompleted', True)
            
            if sanitized_profile_data['profileCompleted']:
                logger.info(f"Le profil de {user_id} est déjà marqué comme complété")
            else:
                logger.info(f"Le profil de {user_id} n'était pas marqué comme complété, cela ne devrait pas arriver")

        sanitized_profile_data['updatedAt'] = int(datetime.datetime.now().timestamp())

        if 'profileImageBase64' in sanitized_profile_data:
            try:
                image_data = sanitized_profile_data['profileImageBase64']
                logger.info(f"Traitement d'image - Longueur des données: {len(image_data)} caractères")
                logger.info(f"Début des données d'image: {image_data[:50]}...")
                
                if ',' in image_data:
                    header, encoded = image_data.split(',', 1)
                    image_content = base64.b64decode(encoded)
                    content_type = header.split(':')[1].split(';')[0]
                    logger.info(f"Image avec en-tête détectée. Type MIME: {content_type}")
                else:
                    try:
                        image_content = base64.b64decode(image_data)
                        content_type = get_mime_type(image_content)
                        logger.info(f"Image sans en-tête détectée. Type MIME détecté: {content_type}")
                    except Exception as e:
                        logger.error(f"Erreur lors du décodage base64: {str(e)}")
                        raise
                
                extension = '.jpg'
                if content_type == 'image/png':
                    extension = '.png'
                elif content_type == 'image/webp':
                    extension = '.webp'
                elif content_type == 'image/gif':
                    extension = '.gif'
                elif content_type == 'image/jpeg':
                    extension = '.jpg'
                logger.info(f"Extension de fichier déterminée: {extension}")
                
                image_key = f"public/users/{user_id}/profile-image{extension}"
                logger.info(f"Chemin de stockage dans S3: {image_key}")
                
                logger.info(f"Tentative d'upload dans S3 - Bucket: {BUCKET_NAME}, Key: {image_key}")
                upload_response = s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=image_key,
                    Body=image_content,
                    ContentType=content_type,
                )
                logger.info(f"Réponse S3: {upload_response}")
                
                direct_url = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{image_key}"
                logger.info(f"URL générée: {direct_url}")
                
                sanitized_profile_data['profileImageUrl'] = direct_url
                logger.info(f"profileImageUrl mise à jour dans les données: {sanitized_profile_data.get('profileImageUrl')}")
                
            except Exception as e:
                logger.error(f"Erreur lors du traitement de l'image: {str(e)}")
                logger.error(traceback.format_exc())
            
            logger.info("Suppression des données profileImageBase64 pour économiser de l'espace")
            del sanitized_profile_data['profileImageBase64']
        else:
            logger.info("Aucune image base64 trouvée dans les données")
            if not sanitized_profile_data.get('profileImageUrl'):
                logger.info(f"Aucune URL d'image trouvée, utilisation de l'image par défaut")
                sanitized_profile_data['profileImageUrl'] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{DEFAULT_PROFILE_IMAGE_KEY}"

        logger.info(f"URL finale de l'image avant sauvegarde: {sanitized_profile_data.get('profileImageUrl')}")

        logger.info(f"Mise à jour du profil dans DynamoDB pour l'utilisateur {user_id}")
        table.put_item(Item=sanitized_profile_data)

        try:
            updated_item = table.get_item(Key={'userId': user_id})
            updated_profile = updated_item.get('Item', {})
            logger.info(f"URL de l'image après sauvegarde: {updated_profile.get('profileImageUrl')}")
            
            if updated_profile.get('profileImageUrl') != sanitized_profile_data.get('profileImageUrl'):
                logger.error("ALERTE: L'URL de l'image dans DynamoDB ne correspond pas à celle attendue!")
        except Exception as e:
            logger.error(f"Erreur lors de la récupération du profil mis à jour: {str(e)}")

        logger.info(f"État final de profileCompleted pour {user_id}: {sanitized_profile_data['profileCompleted']}")
        
        # Vérifier si le statut de disponibilité est stocké correctement
        logger.info(f"Statut de disponibilité stocké: {sanitized_profile_data.get('availabilityStatus')}")

        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'message': 'Profile updated successfully',
                'updatedProfile': updated_item.get('Item', {})
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
