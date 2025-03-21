# PlaysTracks.py
import json
import boto3
import logging
import os
import uuid
import datetime
from decimal import Decimal
from boto3.dynamodb.conditions import Key

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Encodeur personnalisé pour gérer les objets Decimal
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# Variables d'environnement
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')
PLAYS_HISTORY_TABLE = os.environ.get('PLAYS_HISTORY_TABLE', 'chordora-track-plays')

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')
tracks_table = dynamodb.Table(TRACKS_TABLE)
plays_history_table = dynamodb.Table(PLAYS_HISTORY_TABLE)

def get_cors_headers():
    return {
        'Access-Control-Allow-Origin': 'https://app.chordora.com',
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
        source = body.get('source', 'unknown')  # Source de l'écoute (profil, playlist, etc.)
        
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
        
        # Timestamp actuel
        timestamp = int(datetime.datetime.now().timestamp())
        
        # Vérifier si l'utilisateur a déjà écouté cette piste récemment
        cooldown_period = 3600  # 1 heure en secondes
        
        # Si utilisateur authentifié, vérifier son historique d'écoute récent
        if user_id:
            try:
                # Requête pour vérifier l'historique récent d'écoute pour cet utilisateur et cette piste
                response = plays_history_table.query(
                    IndexName='user_id-track_id-index',  # Assurez-vous que cet index existe
                    KeyConditionExpression=Key('user_id').eq(user_id) & Key('track_id').eq(track_id),
                    ScanIndexForward=False,  # Pour obtenir l'entrée la plus récente en premier
                    Limit=1  # Nous n'avons besoin que de l'entrée la plus récente
                )
                
                recent_plays = response.get('Items', [])
                
                if recent_plays:
                    last_play_time = recent_plays[0].get('timestamp', 0)
                    time_since_last_play = timestamp - last_play_time
                    
                    if time_since_last_play < cooldown_period:
                        logger.info(f"Écoute récente détectée pour l'utilisateur {user_id} sur la piste {track_id}. "
                                    f"Temps écoulé: {time_since_last_play} secondes (cooldown: {cooldown_period} secondes)")
                        
                        # L'utilisateur a déjà écouté cette piste récemment, ne pas compter une nouvelle écoute
                        return {
                            'statusCode': 200,
                            'headers': cors_headers,
                            'body': json.dumps({
                                'message': 'Play already counted recently',
                                'trackId': track_id,
                                'counted': False,
                                'timeSinceLastPlay': time_since_last_play,
                                'cooldownPeriod': cooldown_period
                            }, cls=DecimalEncoder)
                        }
            except Exception as query_error:
                # En cas d'erreur lors de la requête, on log l'erreur mais on continue
                logger.error(f"Erreur lors de la vérification de l'historique d'écoute: {str(query_error)}")
        
        # Générer un ID unique pour l'écoute
        play_id = str(uuid.uuid4())
        
        # 1. Enregistrer l'écoute dans la table d'historique
        play_record = {
            'play_id': play_id,
            'track_id': track_id,
            'timestamp': timestamp,
            'source': source
        }
        
        # Ajouter l'ID utilisateur si disponible
        if user_id:
            play_record['user_id'] = user_id
        
        # Ajouter des informations sur le client si disponibles
        if 'headers' in event:
            headers = event['headers'] or {}
            client_info = {}
            
            if 'User-Agent' in headers:
                client_info['user_agent'] = headers['User-Agent']
            if 'X-Forwarded-For' in headers:
                client_info['ip'] = headers['X-Forwarded-For']
                
            if client_info:
                play_record['client_info'] = client_info
        
        try:
            # Enregistrer dans la table d'historique
            plays_history_table.put_item(Item=play_record)
            logger.info(f"Écoute enregistrée avec succès dans l'historique. Play ID: {play_id}")
            
            # 2. Incrémenter le compteur d'écoutes dans la table principale
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
                    'playId': play_id,
                    'plays': plays_count,
                    'timestamp': timestamp,
                    'counted': True
                }, cls=DecimalEncoder)
            }
            
        except Exception as update_error:
            logger.error(f"Erreur lors de la mise à jour des compteurs d'écoutes: {str(update_error)}")
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
