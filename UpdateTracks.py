
import json
import boto3
import os
import uuid
import datetime
import base64
import logging
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import traceback
import re

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Classe pour l'encodage des décimaux en JSON
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# Fonction pour obtenir les en-têtes CORS
def get_cors_headers():
    return {
        'Access-Control-Allow-Origin': 'http://localhost:3000',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

# Fonction pour déterminer le type MIME à partir des données d'image
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

# Initialisation des clients AWS
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Variables d'environnement
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'chordora-users')
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")
    
    http_method = event['httpMethod']
    cors_headers = get_cors_headers()
    
    if http_method == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('OK')
        }
    
    try:
        # Vérification de l'authentification
        if 'requestContext' not in event or 'authorizer' not in event['requestContext'] or 'claims' not in event['requestContext']['authorizer']:
            logger.error("Informations d'authentification manquantes")
            return {
                'statusCode': 401,
                'headers': cors_headers,
                'body': json.dumps('Unauthorized: Missing authentication information')
            }
        
        user_id = event['requestContext']['authorizer']['claims']['sub']
        logger.info(f"User ID extracted: {user_id}")
    except Exception as auth_error:
        logger.error(f"Error extracting authentication: {str(auth_error)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 401,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Authentication error: {str(auth_error)}'})
        }
    
    try:
        if http_method == 'GET':
            if 'pathParameters' in event and event['pathParameters'] and 'trackId' in event['pathParameters']:
                return handle_get_track(event, user_id, cors_headers)
            else:
                return handle_get_all_tracks(event, user_id, cors_headers)
        elif http_method == 'POST':
            return handle_post(event, user_id, cors_headers)
        elif http_method == 'PUT':
            return handle_put(event, user_id, cors_headers)
        elif http_method == 'DELETE':
            return handle_delete(event, user_id, cors_headers)
        else:
            logger.warning(f"Unsupported HTTP method: {http_method}")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Unsupported HTTP method'})
            }
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }

def handle_get_all_tracks(event, user_id, cors_headers):
    logger.info(f"Handling GET request for all tracks, user_id: {user_id}")
    try:
        table = dynamodb.Table(TRACKS_TABLE)
        response = table.scan(
            FilterExpression=Attr('user_id').eq(user_id)
        )
        
        tracks = response.get('Items', [])
        logger.info(f"Found {len(tracks)} tracks for user {user_id}")
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(tracks, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Error in handle_get_all_tracks: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving tracks: {str(e)}'})
        }

def handle_get_track(event, user_id, cors_headers):
    logger.info("Handling GET request for a specific track")
    try:
        track_id = event['pathParameters']['trackId']
        
        table = dynamodb.Table(TRACKS_TABLE)
        response = table.get_item(Key={'track_id': track_id})
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Track not found'})
            }
        
        track = response['Item']
        
        if track['user_id'] != user_id:
            return {
                'statusCode': 403,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Not authorized to access this track'})
            }
        
        # Générer URL présignée pour le fichier audio
        presigned_url = s3.generate_presigned_url('get_object',
                                                  Params={'Bucket': BUCKET_NAME,
                                                          'Key': track['file_path']},
                                                  ExpiresIn=3600)
        
        track_info = {**track, 'presigned_url': presigned_url}
        
        # Si une image de couverture existe, générer une URL présignée pour celle-ci également
        if 'cover_image_path' in track and track['cover_image_path']:
            cover_presigned_url = s3.generate_presigned_url('get_object',
                                                            Params={'Bucket': BUCKET_NAME,
                                                                    'Key': track['cover_image_path']},
                                                            ExpiresIn=3600)
            track_info['cover_image'] = cover_presigned_url
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(track_info, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Error in handle_get_track: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving track: {str(e)}'})
        }

def handle_post(event, user_id, cors_headers):
    logger.info("Handling POST request")
    try:
        # Vérification du corps de la requête
        if 'body' not in event or not event['body']:
            logger.error("Missing request body")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Missing request body'})
            }
        
        try:
            body = json.loads(event['body'])
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON: {str(e)}")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Invalid JSON in request body'})
            }
        
        # Vérification des champs requis
        required_fields = ['fileName', 'fileType', 'title', 'genre', 'bpm']
        missing_fields = [field for field in required_fields if field not in body]
        
        if missing_fields:
            logger.error(f"Missing required fields: {missing_fields}")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': f'Missing required fields: {missing_fields}'})
            }
        
        file_name = body['fileName']
        file_type = body['fileType']
        title = body['title']
        genre = body['genre']
        
        # Conversion et validation du BPM
        try:
            bpm = int(body['bpm'])
            if bpm <= 0:
                logger.error(f"Invalid BPM value: {bpm}")
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'message': 'BPM must be a positive number'})
                }
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing BPM: {str(e)}")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'BPM must be a valid number'})
            }
        
        # Génération d'un ID unique pour la piste
        track_id = str(uuid.uuid4())
        
        # Construction du chemin S3
        # Format de chemin simplifié
        s3_key = f"tracks/{user_id}/{track_id}/{file_name}"
        logger.info(f"S3 key for new track: {s3_key}")
        
        # Génération de l'URL présignée pour l'upload audio
        try:
            presigned_url = s3.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': BUCKET_NAME,
                    'Key': s3_key,
                    'ContentType': file_type
                },
                ExpiresIn=3600
            )
            logger.info(f"Generated presigned URL (truncated): {presigned_url[:50]}...")
        except Exception as s3_error:
            logger.error(f"Error generating presigned URL: {str(s3_error)}")
            logger.error(traceback.format_exc())
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'message': f'Error generating upload URL: {str(s3_error)}'})
            }
        
        # Traitement de l'image de couverture si présente
        cover_image_path = None
        
        if 'coverImageBase64' in body and body['coverImageBase64']:
            try:
                # Traiter l'image encodée en base64
                cover_image_data = body['coverImageBase64']
                cover_image_type = body.get('coverImageType', 'image/jpeg')
                
                # Extraire la partie base64 si le format est data:image/xxx;base64,
                if ',' in cover_image_data:
                    header, encoded = cover_image_data.split(',', 1)
                    image_content = base64.b64decode(encoded)
                    # Extraire le type MIME de l'en-tête si possible
                    if ';' in header and ':' in header:
                        cover_image_type = header.split(':')[1].split(';')[0]
                else:
                    try:
                        image_content = base64.b64decode(cover_image_data)
                        # Déterminer le type MIME à partir du contenu
                        cover_image_type = get_mime_type(image_content)
                    except Exception as e:
                        logger.error(f"Error decoding base64 image: {str(e)}")
                        # Continuer sans image de couverture en cas d'erreur
                        cover_image_path = None
                        image_content = None
                
                # Si l'image a été correctement décodée, l'enregistrer dans S3
                if image_content:
                    # Déterminer l'extension de fichier
                    extension = '.jpg'
                    if cover_image_type == 'image/png':
                        extension = '.png'
                    elif cover_image_type == 'image/webp':
                        extension = '.webp'
                    elif cover_image_type == 'image/gif':
                        extension = '.gif'
                    
                    cover_image_filename = f"cover{extension}"
                    cover_image_path = f"tracks/{user_id}/{track_id}/{cover_image_filename}"
                    
                    logger.info(f"Uploading cover image to S3: {cover_image_path}")
                    
                    # Upload de l'image de couverture vers S3
                    s3.put_object(
                        Bucket=BUCKET_NAME,
                        Key=cover_image_path,
                        Body=image_content,
                        ContentType=cover_image_type
                    )
                    
                    logger.info(f"Cover image uploaded successfully")
            except Exception as image_error:
                logger.error(f"Error processing cover image: {str(image_error)}")
                logger.error(traceback.format_exc())
                # Continuer sans image de couverture en cas d'erreur
                cover_image_path = None