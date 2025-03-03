import json
import boto3
import logging
from decimal import Decimal
import os
import traceback

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Variables d'environnement
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'chordora-users')

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')
tracks_table = dynamodb.Table(TRACKS_TABLE)
s3 = boto3.client('s3')

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def get_cors_headers():
    """
    Renvoie les en-têtes CORS standard
    """
    return {
        'Access-Control-Allow-Origin': 'http://localhost:3000',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

def generate_presigned_urls(tracks):
    """
    Génère des URLs présignées pour chaque piste
    """
    tracks_with_urls = []
    for track in tracks:
        try:
            if 'file_path' in track:
                presigned_url = s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': BUCKET_NAME, 'Key': track['file_path']},
                    ExpiresIn=3600  # URL valide 1 heure
                )
                track_with_url = {**track, 'presigned_url': presigned_url}
            else:
                track_with_url = {**track, 'error': 'Missing file path'}
            
            tracks_with_urls.append(track_with_url)
        except Exception as s3_error:
            logger.error(f"Erreur lors de la génération de l'URL présignée: {str(s3_error)}")
            track_with_url = {**track, 'error': 'Could not generate presigned URL'}
            tracks_with_urls.append(track_with_url)
    
    return tracks_with_urls

def lambda_handler(event, context):
    logger.info(f"Événement reçu: {json.dumps(event)}")
    cors_headers = get_cors_headers()
    
    # Gestion des requêtes OPTIONS (pre-flight CORS)
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('Preflight request successful')
        }
    
    try:
        # Extraction de l'ID utilisateur
        if 'requestContext' not in event or 'authorizer' not in event['requestContext']:
            return {
                'statusCode': 401,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Unauthorized: Missing authentication'})
            }
        
        user_id = event['requestContext']['authorizer']['claims']['sub']
        logger.info(f"Récupération des pistes pour l'utilisateur: {user_id}")
        
        # Récupérer les paramètres de requête
        query_params = event.get('queryStringParameters', {}) or {}
        genre = query_params.get('genre')
        
        # Préparer les paramètres de requête DynamoDB
        query_params = {
            'IndexName': 'user_id-index',  # Utiliser l'index secondaire global
            'KeyConditionExpression': 'user_id = :uid',
            'ExpressionAttributeValues': {
                ':uid': user_id
            }
        }
        
        # Ajouter un filtre par genre si spécifié
        if genre:
            query_params['FilterExpression'] = 'genre = :genre'
            query_params['ExpressionAttributeValues'][':genre'] = genre
        
        # Exécuter la requête
        try:
            response = tracks_table.query(**query_params)
            tracks = response.get('Items', [])
            
            logger.info(f"Nombre de pistes trouvées: {len(tracks)}")
            
            # Générer des URLs présignées pour chaque piste
            tracks_with_urls = generate_presigned_urls(tracks)
            
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps(tracks_with_urls, cls=DecimalEncoder)
            }
        
        except Exception as query_error:
            logger.error(f"Erreur lors de la requête: {str(query_error)}")
            logger.error(traceback.format_exc())
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'message': f'Erreur de requête: {str(query_error)}'})
            }
    
    except Exception as e:
        logger.error(f"Erreur non gérée: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Erreur interne: {str(e)}'})
        }