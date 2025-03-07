# TrackPlays.py
import json
import boto3
import logging
import os
from decimal import Decimal

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Variables d'environnement
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')
tracks_table = dynamodb.Table(TRACKS_TABLE)

def get_cors_headers():
    return {
        'Access-Control-Allow-Origin': 'http://localhost:3000',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'POST,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

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
        # Récupérer l'ID utilisateur du token d'authentification (optionnel)
        user_id = None
        if 'requestContext' in event and 'authorizer' in event['requestContext']:
            user_id = event['requestContext']['authorizer']['claims']['sub']
            logger.info(f"Utilisateur authentifié: {user_id}")
        
        # Récupérer l'ID de la piste du corps de la requête
        body = json.loads(event['body'])
        track_id = body.get('trackId')
        
        if not track_id:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Missing trackId parameter'})
            }
        
        # Vérifier si la piste existe
        track_response = tracks_table.get_item(Key={'track_id': track_id})
        
        if 'Item' not in track_response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Track not found'})
            }
        
        # Incrémenter le compteur d'écoutes
        try:
            update_response = tracks_table.update_item(
                Key={'track_id': track_id},
                UpdateExpression='SET plays = if_not_exists(plays, :start) + :inc',
                ExpressionAttributeValues={
                    ':inc': 1,
                    ':start': 0
                },
                ReturnValues='UPDATED_NEW'
            )
            
            plays_count = update_response.get('Attributes', {}).get('plays', 1)
            logger.info(f"Compteur d'écoutes mis à jour pour la piste {track_id}. Nouveau compteur: {plays_count}")
            
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'message': 'Play count updated successfully',
                    'trackId': track_id,
                    'plays': plays_count
                })
            }
            
        except Exception as update_error:
            logger.error(f"Erreur lors de la mise à jour du compteur d'écoutes: {str(update_error)}")
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'message': f'Error updating play count: {str(update_error)}'})
            }
            
    except Exception as e:
        logger.error(f"Erreur non gérée: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }