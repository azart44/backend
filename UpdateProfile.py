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
    # Vérifier les signatures de fichiers courants
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
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
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
            'headers': cors_headers,
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }

def sanitize_profile_data(profile_data):
    """
    Assainit et valide les données du profil avant de les stocker
    """
    sanitized = {}
    
    # Champs de base - Chaînes de caractères
    for field in ['userId', 'email', 'username', 'bio', 'userType', 
                  'experienceLevel', 'location', 'software', 'musicalMood']:
        if field in profile_data:
            # Limiter la longueur des chaînes
            if field == 'bio' and profile_data.get(field) and len(profile_data[field]) > 150:
                sanitized[field] = profile_data[field][:150]
            else:
                sanitized[field] = profile_data.get(field)
    
    # Champs de listes - Vérifier et nettoyer les listes
    for field in ['musicGenres', 'tags', 'equipment', 'favoriteArtists']:
        if field in profile_data and isinstance(profile_data[field], list):
            # Limiter le nombre d'éléments pour certains champs
            if field in ['musicGenres', 'tags'] and len(profile_data[field]) > 3:
                sanitized[field] = profile_data[field][:3]
            else:
                sanitized[field] = profile_data[field]
    
    # Traitement spécial pour socialLinks (maps)
    if 'socialLinks' in profile_data and isinstance(profile_data['socialLinks'], dict):
        sanitized_links = {}
        for platform, url in profile_data['socialLinks'].items():
            # Validation basique des URLs
            if isinstance(url, str) and (url == '' or url.startswith('http')):
                sanitized_links[platform] = url
        
        if sanitized_links:
            sanitized['socialLinks'] = sanitized_links
    
    # Champs booléens
    for field in ['profileCompleted']:
        if field in profile_data:
            sanitized[field] = bool(profile_data[field])
    
    # Préserver le champ profileImageBase64 s'il existe
    if 'profileImageBase64' in profile_data:
        sanitized['profileImageBase64'] = profile_data['profileImageBase64']
        
    # Préserver l'URL de l'image de profil si elle existe déjà
    if 'profileImageUrl' in profile_data:
        sanitized['profileImageUrl'] = profile_data['profileImageUrl']
    
    return sanitized

def handle_update_profile(event, cors_headers, user_id):
    logger.info(f"Début de handle_update_profile pour l'utilisateur: {user_id}")
    try:
        body = json.loads(event['body'])
        profile_data = body['profileData']
        
        # Log pour débogage sans exposer les données sensibles
        logger.info(f"Données de profil reçues: {json.dumps({k: '...' if k == 'profileImageBase64' else v for k, v in profile_data.items()})}")
        logger.info(f"Profil contient une image? {'profileImageBase64' in profile_data}")

        # S'assurer que l'utilisateur ne peut pas modifier l'ID utilisateur
        profile_data['userId'] = user_id
        
        # Assainir et valider les données du profil
        sanitized_profile_data = sanitize_profile_data(profile_data)
        logger.info(f"Données de profil assainies: {json.dumps({k: '...' if k == 'profileImageBase64' else v for k, v in sanitized_profile_data.items()})}")

        # Vérifier si l'utilisateur existe déjà
        existing_user = table.get_item(Key={'userId': user_id}).get('Item')
        
        if not existing_user:
            logger.info(f"Création d'un nouveau profil utilisateur pour {user_id}")
            # Si aucune image n'est fournie, utiliser l'image par défaut
            if not sanitized_profile_data.get('profileImageUrl') and 'profileImageBase64' not in sanitized_profile_data:
                sanitized_profile_data['profileImageUrl'] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{DEFAULT_PROFILE_IMAGE_KEY}"
            sanitized_profile_data['profileCompleted'] = False
            sanitized_profile_data['createdAt'] = int(datetime.datetime.now().timestamp())
        else:
            logger.info(f"Mise à jour du profil existant pour {user_id}")
            logger.info(f"URL d'image existante: {existing_user.get('profileImageUrl')}")
            
            # Fusionner les données existantes avec les nouvelles
            # Utiliser une approche sélective pour la fusion pour éviter d'écraser des champs existants
            merged_profile = {}
            
            # Conserver toutes les clés existantes
            for key, value in existing_user.items():
                merged_profile[key] = value
            
            # Mettre à jour ou ajouter les nouvelles valeurs
            for key, value in sanitized_profile_data.items():
                merged_profile[key] = value
            
            sanitized_profile_data = merged_profile
        
        # Ajouter un timestamp de mise à jour
        sanitized_profile_data['updatedAt'] = int(datetime.datetime.now().timestamp())

        # Traiter l'image de profil si présente
        if 'profileImageBase64' in sanitized_profile_data:
            try:
                image_data = sanitized_profile_data['profileImageBase64']
                logger.info(f"Traitement d'image - Longueur des données: {len(image_data)} caractères")
                logger.info(f"Début des données d'image: {image_data[:50]}...")
                
                # Vérifier si l'image est déjà en format base64 avec en-tête ou non
                if ',' in image_data:
                    # Format: data:image/png;base64,BASE64_DATA
                    header, encoded = image_data.split(',', 1)
                    image_content = base64.b64decode(encoded)
                    # Extraire le type MIME du header
                    content_type = header.split(':')[1].split(';')[0]
                    logger.info(f"Image avec en-tête détectée. Type MIME: {content_type}")
                else:
                    # Format: BASE64_DATA sans header
                    try:
                        image_content = base64.b64decode(image_data)
                        # Utiliser la fonction pour déterminer le type MIME
                        content_type = get_mime_type(image_content)
                        logger.info(f"Image sans en-tête détectée. Type MIME détecté: {content_type}")
                    except Exception as e:
                        logger.error(f"Erreur lors du décodage base64: {str(e)}")
                        raise
                
                # Déterminer l'extension basée sur le type MIME
                extension = '.jpg'  # Par défaut
                if content_type == 'image/png':
                    extension = '.png'
                elif content_type == 'image/webp':
                    extension = '.webp'
                elif content_type == 'image/gif':
                    extension = '.gif'
                elif content_type == 'image/jpeg':
                    extension = '.jpg'
                logger.info(f"Extension de fichier déterminée: {extension}")
                
                # Construire le chemin de stockage AVEC l'extension
                image_key = f"public/users/{user_id}/profile-image{extension}"
                logger.info(f"Chemin de stockage dans S3: {image_key}")
                
                # Uploader l'image dans S3
                logger.info(f"Tentative d'upload dans S3 - Bucket: {BUCKET_NAME}, Key: {image_key}")
                upload_response = s3.put_object(
                    Bucket=BUCKET_NAME,
                    Key=image_key,
                    Body=image_content,
                    ContentType=content_type,
                )
                logger.info(f"Réponse S3: {upload_response}")
                
                # Construire l'URL complète pour l'image - IMPORTANT
                direct_url = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{image_key}"
                logger.info(f"URL générée: {direct_url}")
                
                # Mettre à jour l'URL dans les données
                sanitized_profile_data['profileImageUrl'] = direct_url
                logger.info(f"profileImageUrl mise à jour dans les données: {sanitized_profile_data.get('profileImageUrl')}")
                
            except Exception as e:
                logger.error(f"Erreur lors du traitement de l'image: {str(e)}")
                logger.error(traceback.format_exc())
            
            # Supprimer les données base64 pour économiser de l'espace dans DynamoDB
            logger.info("Suppression des données profileImageBase64 pour économiser de l'espace")
            del sanitized_profile_data['profileImageBase64']
        else:
            logger.info("Aucune image base64 trouvée dans les données")
            # Vérifier si l'URL d'image est déjà définie
            if not sanitized_profile_data.get('profileImageUrl'):
                logger.info(f"Aucune URL d'image trouvée, utilisation de l'image par défaut")
                sanitized_profile_data['profileImageUrl'] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{DEFAULT_PROFILE_IMAGE_KEY}"

        # Ajouter un log final avant d'enregistrer dans DynamoDB
        logger.info(f"URL finale de l'image avant sauvegarde: {sanitized_profile_data.get('profileImageUrl')}")

        # Enregistrer les données dans DynamoDB
        logger.info(f"Mise à jour du profil dans DynamoDB pour l'utilisateur {user_id}")
        table.put_item(Item=sanitized_profile_data)

        # Récupérer le profil mis à jour pour confirmer
        try:
            updated_item = table.get_item(Key={'userId': user_id})
            updated_profile = updated_item.get('Item', {})
            logger.info(f"URL de l'image après sauvegarde: {updated_profile.get('profileImageUrl')}")
            
            # Vérifier que l'URL est correcte
            if updated_profile.get('profileImageUrl') != sanitized_profile_data.get('profileImageUrl'):
                logger.error("ALERTE: L'URL de l'image dans DynamoDB ne correspond pas à celle attendue!")
        except Exception as e:logger.error(f"Erreur lors de la récupération du profil mis à jour: {str(e)}")

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