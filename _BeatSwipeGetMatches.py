import json
import boto3
import logging
import traceback
import os
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# Variables d'environnement
MATCHES_TABLE = os.environ.get('MATCHES_TABLE', 'chordora-beat-matches')
USERS_TABLE = os.environ.get('USERS_TABLE', 'chordora-users')
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'chordora-users')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# Tables DynamoDB
matches_table = dynamodb.Table(MATCHES_TABLE)
users_table = dynamodb.Table(USERS_TABLE)
tracks_table = dynamodb.Table(TRACKS_TABLE)

# Classe pour l'encodage des décimaux en JSON
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def get_cors_headers(event):
    """
    Génère les en-têtes CORS dynamiques basés sur l'origine de la requête.
    """
    origin = None
    if 'headers' in event and event['headers']:
        origin = event['headers'].get('origin') or event['headers'].get('Origin')
    
    allowed_origin = origin if origin else 'https://app.chordora.com'
    
    return {
        'Access-Control-Allow-Origin': allowed_origin,
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

def generate_presigned_url_for_track_cover(bucket, key):
    """
    Génère une URL présignée sécurisée pour une image de couverture de track
    
    Args:
        bucket (str): Nom du bucket S3
        key (str): Chemin complet de l'image dans S3
    
    Returns:
        str: URL présignée de l'image
    """
    try:
        # Générer l'URL présignée
        presigned_url = s3.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket, 
                'Key': key,
                'ResponseContentType': 'image/png',  # Ajuster selon le type réel (png dans votre exemple)
                'ResponseContentDisposition': 'inline'  # Pour affichage direct
            },
            ExpiresIn=86400  # URL valide 24 heures
        )
        
        # Vérifier que l'URL n'est pas vide
        if not presigned_url:
            logger.error(f"URL présignée générée vide pour la clé: {key}")
            return f"https://{bucket}.s3.{AWS_REGION}.amazonaws.com/{key}"
        
        logger.info(f"URL présignée générée pour la clé {key}: {presigned_url[:50]}...")
        return presigned_url
    
    except Exception as e:
        logger.error(f"Erreur lors de la génération de l'URL présignée pour {key}: {str(e)}")
        
        # Fallback à une URL non signée si la génération échoue
        return f"https://{bucket}.s3.{AWS_REGION}.amazonaws.com/{key}"

def generate_presigned_url_for_profile_image(bucket, key):
    """
    Génère une URL présignée sécurisée pour une image de profil
    
    Args:
        bucket (str): Nom du bucket S3
        key (str): Chemin complet de l'image dans S3
    
    Returns:
        str: URL présignée de l'image
    """
    try:
        # Générer l'URL présignée
        presigned_url = s3.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket, 
                'Key': key,
                'ResponseContentType': 'image/jpeg',  # Ajuster selon le type réel
                'ResponseContentDisposition': 'inline'  # Pour affichage direct
            },
            ExpiresIn=86400  # URL valide 24 heures
        )
        
        # Vérifier que l'URL n'est pas vide
        if not presigned_url:
            logger.error(f"URL présignée générée vide pour la clé: {key}")
            return f"https://{bucket}.s3.{AWS_REGION}.amazonaws.com/{key}"
        
        logger.info(f"URL présignée générée pour la clé {key}: {presigned_url[:50]}...")
        return presigned_url
    
    except Exception as e:
        logger.error(f"Erreur lors de la génération de l'URL présignée pour {key}: {str(e)}")
        
        # Fallback à une URL non signée si la génération échoue
        return f"https://{bucket}.s3.{AWS_REGION}.amazonaws.com/{key}"

def lambda_handler(event, context):
    """Récupère les matches BeatSwipe pour un utilisateur"""
    logger.info(f"Événement reçu: {json.dumps(event)}")
    cors_headers = get_cors_headers(event)
    
    # Requête OPTIONS pour CORS
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('Preflight request successful')
        }
    
    try:
        # Vérification de l'authentification
        if 'requestContext' not in event or 'authorizer' not in event['requestContext']:
            return {
                'statusCode': 401,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Unauthorized: Missing authentication'})
            }
        
        # Récupérer l'ID de l'utilisateur du token JWT
        user_id = event['requestContext']['authorizer']['claims']['sub']
        logger.info(f"User ID: {user_id}")
        
        # Récupérer le profil utilisateur pour vérifier son rôle
        user_response = users_table.get_item(Key={'userId': user_id})
        if 'Item' not in user_response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'User profile not found'})
            }
        
        user_profile = user_response['Item']
        user_type = user_profile.get('userType', '').lower()
        
        # Déterminer si on doit récupérer les matches en tant qu'artiste ou beatmaker
        index_name = None
        key_condition = None
        
        if user_type == 'rappeur':
            # Artiste recherche ses matches
            index_name = 'artist_id-timestamp-index'
            key_condition = Key('artist_id').eq(user_id)
        elif user_type in ['beatmaker', 'loopmaker']:
            # Beatmaker recherche ses matches
            index_name = 'beatmaker_id-timestamp-index'
            key_condition = Key('beatmaker_id').eq(user_id)
        else:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Invalid user type'})
            }
        
        # Récupérer les matches
        matches_response = matches_table.query(
            IndexName=index_name,
            KeyConditionExpression=key_condition,
            ScanIndexForward=False  # Trier par timestamp décroissant (le plus récent d'abord)
        )
        
        matches = matches_response.get('Items', [])
        logger.info(f"Nombre de matches trouvés: {len(matches)}")
        
        # Enrichir les matches avec les informations
        enriched_matches = []
        
        for match in matches:
            try:
                track_id = match.get('track_id')
                artist_id = match.get('artist_id')
                beatmaker_id = match.get('beatmaker_id')
                
                # Récupérer les détails de la piste
                track_response = tracks_table.get_item(Key={'track_id': track_id})
                track = track_response.get('Item', {})
                
                # Récupérer les détails de l'artiste
                artist_response = users_table.get_item(Key={'userId': artist_id})
                artist = artist_response.get('Item', {})
                
                # Récupérer les détails du beatmaker
                beatmaker_response = users_table.get_item(Key={'userId': beatmaker_id})
                beatmaker = beatmaker_response.get('Item', {})
                
                # Générer l'URL présignée pour la couverture de la piste
                cover_url = None
                if track.get('cover_image_path'):
                    cover_url = generate_presigned_url_for_track_cover(
                        BUCKET_NAME, 
                        track['cover_image_path']
                    )
                
                # Générer des URLs présignées pour les images de profil
                artist_profile_image = None
                if artist.get('profileImagePath'):
                    artist_profile_image = generate_presigned_url_for_profile_image(
                        BUCKET_NAME, 
                        artist['profileImagePath']
                    )
                
                beatmaker_profile_image = None
                if beatmaker.get('profileImagePath'):
                    beatmaker_profile_image = generate_presigned_url_for_profile_image(
                        BUCKET_NAME, 
                        beatmaker['profileImagePath']
                    )
                
                # Créer un objet de match enrichi
                enriched_match = {
                    'match_id': match.get('match_id'),
                    'timestamp': match.get('timestamp'),
                    'status': match.get('status'),
                    'track': {
                        'track_id': track_id,
                        'title': track.get('title', 'Unknown Track'),
                        'genre': track.get('genre', 'Unknown'),
                        'bpm': track.get('bpm'),
                        'cover_image': cover_url or f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/public/default-cover.jpg"
                    },
                    'artist': {
                        'user_id': artist_id,
                        'username': artist.get('username', 'Unknown Artist'),
                        'profile_image_url': artist_profile_image or f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/public/default-profile.jpg"
                    },
                    'beatmaker': {
                        'user_id': beatmaker_id,
                        'username': beatmaker.get('username', 'Unknown Producer'),
                        'profile_image_url': beatmaker_profile_image or f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/public/default-profile.jpg"
                    }
                }
                
                enriched_matches.append(enriched_match)
                
            except Exception as match_error:
                logger.error(f"Erreur lors du traitement d'un match: {str(match_error)}")
                logger.error(traceback.format_exc())
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'matches': enriched_matches,
                'count': len(enriched_matches)
            }, cls=DecimalEncoder)
        }
        
    except Exception as e:
        logger.error(f"Erreur non gérée: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'message': f'Internal server error: {str(e)}'
            })
        }
