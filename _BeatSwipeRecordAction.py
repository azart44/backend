import json
import boto3
import logging
import traceback
import os
import datetime
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')

# Variables d'environnement
SWIPES_TABLE = os.environ.get('SWIPES_TABLE', 'chordora-beat-swipes')
MATCHES_TABLE = os.environ.get('MATCHES_TABLE', 'chordora-beat-matches')
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')
USERS_TABLE = os.environ.get('USERS_TABLE', 'chordora-users')

# Tables DynamoDB
swipes_table = dynamodb.Table(SWIPES_TABLE)
matches_table = dynamodb.Table(MATCHES_TABLE)
tracks_table = dynamodb.Table(TRACKS_TABLE)
users_table = dynamodb.Table(USERS_TABLE)

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
        'Access-Control-Allow-Methods': 'POST,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

def lambda_handler(event, context):
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
        
        # Récupérer le body de la requête
        body = json.loads(event['body'])
        track_id = body.get('trackId')
        action = body.get('action')  # "right", "left", "down"
        
        if not track_id or not action:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Missing trackId or action'})
            }
        
        # Vérifier si l'action est valide
        if action not in ['right', 'left', 'down']:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Invalid action. Use "right", "left", or "down"'})
            }
        
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
        
        # Vérifier si l'utilisateur est un artiste
        if user_type != 'rappeur':
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'BeatSwipe is only available for artists'})
            }
        
        # Récupérer les détails de la piste
        track_response = tracks_table.get_item(Key={'track_id': track_id})
        if 'Item' not in track_response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Track not found'})
            }
        
        track = track_response['Item']
        beatmaker_id = track.get('user_id')
        
        # Horodatage actuel
        timestamp = int(datetime.datetime.now().timestamp())
        
        # Créer un ID unique pour le swipe
        swipe_id = f"{user_id}#{track_id}"
        
        # Enregistrer le swipe
        swipes_table.put_item(
            Item={
                'swipe_id': swipe_id,
                'user_id': user_id,
                'track_id': track_id,
                'action': action,
                'timestamp': timestamp
            }
        )
        
        # Si c'est un swipe à droite (like), créer un match
        if action == 'right':
            match_id = f"{user_id}#{beatmaker_id}#{track_id}"
            
            matches_table.put_item(
                Item={
                    'match_id': match_id,
                    'artist_id': user_id,
                    'beatmaker_id': beatmaker_id,
                    'track_id': track_id,
                    'timestamp': timestamp,
                    'status': 'new'
                }
            )
            
            # On pourrait aussi implémenter une notification au beatmaker ici
            
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'message': 'Match created successfully',
                    'match': {
                        'match_id': match_id,
                        'track_id': track_id,
                        'beatmaker_id': beatmaker_id,
                        'track_title': track.get('title', 'Unknown Track')
                    }
                })
            }
        
        # Si c'est un swipe vers le bas (ajout aux favoris), on pourrait implémenter
        # une logique supplémentaire ici
        if action == 'down':
            # Logique pour ajouter aux favoris
            # Cette partie serait implémentée dans un autre ticket
            pass
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'message': 'Swipe recorded successfully',
                'swipe': {
                    'user_id': user_id,
                    'track_id': track_id,
                    'action': action,
                    'timestamp': timestamp
                }
            })
        }
        
    except Exception as e:
        logger.error(f"Erreur non gérée: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }