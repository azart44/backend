import json
import boto3
import os
import logging
import datetime
import traceback
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')

# Variables d'environnement
FAVORITES_TABLE = os.environ.get('FAVORITES_TABLE', 'chordora-track-favorites')
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')

# Tables DynamoDB
favorites_table = dynamodb.Table(FAVORITES_TABLE)
tracks_table = dynamodb.Table(TRACKS_TABLE)

# Classe pour l'encodage des décimaux en JSON
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
        'Access-Control-Allow-Origin': 'https://app.chordora.com',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,POST,DELETE,OPTIONS',
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
        # Extraire l'ID de l'utilisateur
        user_id = event['requestContext']['authorizer']['claims']['sub']
        logger.info(f"User ID: {user_id}")
        
        # Méthode HTTP
        http_method = event['httpMethod']
        
        # Route spécifique pour track-favorites
        if http_method == 'GET':
            # Vérifier s'il y a des paramètres de chemin
            path_parameters = event.get('pathParameters', {}) or {}
            
            if path_parameters and 'trackId' in path_parameters:
                # Vérifier le statut de favori pour un track spécifique
                return check_favorite_status(event, user_id, cors_headers)
            else:
                # Récupérer tous les IDs de tracks favorites
                return get_user_favorite_ids(event, user_id, cors_headers)
        
        elif http_method == 'POST':
            # Ajouter un track aux favoris
            return add_favorite(event, user_id, cors_headers)
        
        elif http_method == 'DELETE':
            # Supprimer un track des favoris
            return remove_favorite(event, user_id, cors_headers)
        
        else:
            return {
                'statusCode': 405,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Method Not Allowed'})
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

def check_favorite_status(event, user_id, cors_headers):
    """
    Vérifie si un utilisateur a déjà ajouté une piste à ses favoris
    """
    try:
        track_id = event['pathParameters']['trackId']
        
        # Construire la clé primaire pour le favori
        favorite_id = f"{user_id}#{track_id}"
        
        # Vérifier si le favori existe
        response = favorites_table.get_item(Key={'favorite_id': favorite_id})
        
        is_favorite = 'Item' in response
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'isFavorite': is_favorite,
                'trackId': track_id
            })
        }
    except Exception as e:
        logger.error(f"Erreur lors de la vérification du statut de favori: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error checking favorite status: {str(e)}'})
        }

def get_user_favorite_ids(event, user_id, cors_headers):
    """
    Récupère les IDs des pistes marquées comme favorites par un utilisateur
    """
    try:
        # Requête pour trouver tous les favoris de l'utilisateur
        response = favorites_table.query(
            IndexName='user_id-index',
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
        
        favorites = response.get('Items', [])
        logger.info(f"Nombre de favoris trouvés: {len(favorites)}")
        
        # Récupérer uniquement les IDs des pistes favorites
        track_ids = [favorite['track_id'] for favorite in favorites]
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'trackIds': track_ids,
                'totalFavorites': len(favorites)
            })
        }
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des favoris: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving favorites: {str(e)}'})
        }

def add_favorite(event, user_id, cors_headers):
    """
    Ajoute une piste aux favoris d'un utilisateur
    """
    try:
        # Récupérer l'ID de la piste depuis le corps de la requête
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
        
        # Construire la clé primaire pour le favori
        favorite_id = f"{user_id}#{track_id}"
        
        # Vérifier si l'utilisateur a déjà ajouté cette piste à ses favoris
        favorite_response = favorites_table.get_item(Key={'favorite_id': favorite_id})
        
        if 'Item' in favorite_response:
            return {
                'statusCode': 409,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Track already in favorites'})
            }
        
        # Ajouter aux favoris
        timestamp = int(datetime.datetime.now().timestamp())
        
        favorites_table.put_item(
            Item={
                'favorite_id': favorite_id,
                'user_id': user_id,
                'track_id': track_id,
                'created_at': timestamp
            }
        )
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'message': 'Track added to favorites successfully',
                'trackId': track_id
            })
        }
    except Exception as e:
        logger.error(f"Erreur lors de l'ajout aux favoris: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error adding to favorites: {str(e)}'})
        }

def remove_favorite(event, user_id, cors_headers):
    """
    Supprime une piste des favoris d'un utilisateur
    """
    try:
        # Récupérer l'ID de la piste depuis les paramètres du chemin
        track_id = event['pathParameters']['trackId']
        
        if not track_id:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Missing trackId parameter'})
            }
        
        # Construire la clé primaire pour le favori
        favorite_id = f"{user_id}#{track_id}"
        
        # Vérifier si le favori existe
        response = favorites_table.get_item(Key={'favorite_id': favorite_id})
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Favorite not found'})
            }
        
        # Supprimer le favori
        favorites_table.delete_item(Key={'favorite_id': favorite_id})
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'message': 'Track removed from favorites successfully',
                'trackId': track_id
            }, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Erreur lors de la suppression d'un favori: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error removing from favorites: {str(e)}'})
        }
