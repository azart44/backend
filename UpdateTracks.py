import json
import boto3
import os
import uuid
from boto3.dynamodb.conditions import Key, Attr
import logging
from decimal import Decimal
import traceback

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

# Initialisation des clients AWS
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Variables d'environnement
BUCKET_NAME = os.environ['BUCKET_NAME']
TRACKS_TABLE = os.environ['TRACKS_TABLE']

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
        user_id = event['requestContext']['authorizer']['claims']['sub']
        logger.info(f"User ID extracted: {user_id}")
    except KeyError:
        logger.error("Unable to extract user ID from JWT token")
        return {
            'statusCode': 401,
            'headers': cors_headers,
            'body': json.dumps('Unauthorized: Unable to extract user ID')
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
                'body': json.dumps('Unsupported HTTP method')
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
        
        presigned_url = s3.generate_presigned_url('get_object',
                                                  Params={'Bucket': BUCKET_NAME,
                                                          'Key': track['file_path']},
                                                  ExpiresIn=3600)
        
        track_info = {**track, 'presigned_url': presigned_url}
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(track_info, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Error in handle_get_track: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving track: {str(e)}'})
        }

def handle_post(event, user_id, cors_headers):
    logger.info("Handling POST request")
    try:
        body = json.loads(event['body'])
        file_name = body['fileName']
        file_type = body['fileType']
        title = body['title']
        genre = body['genre']
        bpm = int(body['bpm'])
        
        track_id = str(uuid.uuid4())
        # Important: Utiliser l'ancien format de chemin qui fonctionnait
        s3_key = f"tracks/{track_id}/{file_name}"
        
        # Générer l'URL présignée avec les mêmes paramètres qu'avant
        presigned_url = s3.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': BUCKET_NAME,
                'Key': s3_key,
                'ContentType': file_type
            },
            ExpiresIn=3600
        )
        
        # Enregistrer les métadonnées dans DynamoDB
        table = d