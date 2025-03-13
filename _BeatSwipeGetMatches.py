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
s3_resource = boto3.resource('s3')

# Variables d'environnement
MATCHES_TABLE = os.environ.get('MATCHES_TABLE', 'chordora-beat-matches')
USERS_TABLE = os.environ.get('USERS_TABLE', 'chordora-users')
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'chordora-users')
DEFAULT_IMAGE_KEY = os.environ.get('DEFAULT_IMAGE_KEY', 'public/default-profile.jpg')
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

def file_exists_in_s3(bucket, key):
    """Vérifie si un fichier existe dans S3"""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

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

def lambda_handler(event, context):
    """
    Gestionnaire principal pour les matchs BeatSwipe
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
        # Vérification de l'authentification
        if 'requestContext' not in event or 'authorizer' not in event['requestContext']:
            return {
                'statusCode': 401,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Unauthorized: Missing authentication'})
            }
        
        # Récupérer l'ID de l'utilisateur du token JWT
        user_id = event['requestContext']['authorizer']['claims']['sub']
        
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
            index_name = 'artist_id-timestamp-index'
            key_condition = Key('artist_id').eq(user_id)
        elif user_type in ['beatmaker', 'loopmaker']:
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
        
        # Enrichir les matches avec les informations sur les pistes et les utilisateurs
        enriched_matches = []
        
        for match in matches:
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
            
            # Traitement des URLs pour l'image de profil de l'artiste
            artist_profile_image = None
            if 'profileImageUrl' in artist:
                artist_profile_image = artist['profileImageUrl']
            else:
                # Chercher une image de profil dans S3
                profile_image_key = f"public/users/{artist_id}/profile-image"
                # Essayer différentes extensions
                for ext in ['.jpg', '.jpeg', '.png', '.webp', '']:
                    if file_exists_in_s3(BUCKET_NAME, profile_image_key + ext):
                        artist_profile_image = generate_presigned_url(BUCKET_NAME, profile_image_key + ext, 86400)
                        break
            
            # Traitement des URLs pour l'image de profil du beatmaker
            beatmaker_profile_image = None
            if 'profileImageUrl' in beatmaker:
                beatmaker_profile_image = beatmaker['profileImageUrl']
            else:
                # Chercher une image de profil dans S3
                profile_image_key = f"public/users/{beatmaker_id}/profile-image"
                # Essayer différentes extensions
                for ext in ['.jpg', '.jpeg', '.png', '.webp', '']:
                    if file_exists_in_s3(BUCKET_NAME, profile_image_key + ext):
                        beatmaker_profile_image = generate_presigned_url(BUCKET_NAME, profile_image_key + ext, 86400)
                        break
            
            # Gestion des URLs pour l'image de couverture de la piste
            cover_image = None
            if 'cover_image' in track:
                # Vérifier si c'est déjà une URL complète
                if track['cover_image'].startswith('http'):
                    cover_image = track['cover_image']
                else:
                    # Sinon, générer une URL présignée
                    cover_image = generate_presigned_url(BUCKET_NAME, track['cover_image'], 86400)
            elif 'cover_image_path' in track:
                cover_image = generate_presigned_url(BUCKET_NAME, track['cover_image_path'], 86400)
            
            # Créer un objet de match enrichi (ne pas inclure les URLs audio, elles seront générées
            # lors de la première lecture pour éviter l'expiration)
            enriched_match = {
                'match_id': match.get('match_id'),
                'timestamp': match.get('timestamp'),
                'status': match.get('status', 'new'),
                'track': {
                    'track_id': track_id,
                    'title': track.get('title', 'Unknown Track'),
                    'genre': track.get('genre', 'Unknown'),
                    'bpm': track.get('bpm'),
                    'cover_image': cover_image,
                    # Ne pas inclure l'URL présignée ici, elle sera générée quand nécessaire
                    'file_path': track.get('file_path')  # Stocker le chemin du fichier audio
                },
                'artist': {
                    'user_id': artist_id,
                    'username': artist.get('username', 'Unknown Artist'),
                    'profile_image_url': artist_profile_image
                },
                'beatmaker': {
                    'user_id': beatmaker_id,
                    'username': beatmaker.get('username', 'Unknown Producer'),
                    'profile_image_url': beatmaker_profile_image
                }
            }
            
            enriched_matches.append(enriched_match)
        
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
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }
