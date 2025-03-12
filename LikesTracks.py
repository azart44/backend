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
LIKES_TABLE = os.environ.get('LIKES_TABLE', 'chordora-track-likes')
FAVORITES_TABLE = os.environ.get('FAVORITES_TABLE', 'chordora-track-favorites')  # Nouvelle table pour les favoris
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')

# Tables DynamoDB
likes_table = dynamodb.Table(LIKES_TABLE)
favorites_table = dynamodb.Table(FAVORITES_TABLE)  # Nouvelle référence à la table de favoris
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
        # Vérification de l'authentification
        if 'requestContext' not in event or 'authorizer' not in event['requestContext']:
            return {
                'statusCode': 401,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Unauthorized: Missing authentication'})
            }
        
        user_id = event['requestContext']['authorizer']['claims']['sub']
        logger.info(f"User ID: {user_id}")
        
        # Vérifier si c'est une requête pour les favoris
        is_favorite = False
        resource_path = event.get('resource', '')
        if '/track-favorites' in resource_path or '/track-favorites/' in resource_path:
            is_favorite = True
            logger.info("Requête pour les favoris détectée")
        
        # Déterminer quelle action effectuer en fonction de la méthode HTTP
        if event['httpMethod'] == 'GET':
            if 'pathParameters' in event and event['pathParameters'] and 'trackId' in event['pathParameters']:
                if is_favorite:
                    return check_favorite_status(event, user_id, cors_headers)
                else:
                    return check_like_status(event, user_id, cors_headers)
            else:
                if is_favorite:
                    return get_user_favorite_ids(event, user_id, cors_headers)
                else:
                    return get_user_like_ids(event, user_id, cors_headers)
        elif event['httpMethod'] == 'POST':
            if is_favorite:
                return add_favorite(event, user_id, cors_headers)
            else:
                return add_like(event, user_id, cors_headers)
        elif event['httpMethod'] == 'DELETE':
            if is_favorite:
                return remove_favorite(event, user_id, cors_headers)
            else:
                return remove_like(event, user_id, cors_headers)
        else:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Unsupported HTTP method'})
            }
            
    except Exception as e:
        logger.error(f"Erreur non gérée: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }

def check_like_status(event, user_id, cors_headers):
    """
    Vérifie si un utilisateur a déjà liké une piste spécifique
    """
    try:
        track_id = event['pathParameters']['trackId']
        
        # Construire la clé primaire pour le like
        like_id = f"{user_id}#{track_id}"
        
        # Vérifier si le like existe
        response = likes_table.get_item(Key={'like_id': like_id})
        
        is_liked = 'Item' in response
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'isLiked': is_liked,
                'trackId': track_id
            })
        }
    except Exception as e:
        logger.error(f"Erreur lors de la vérification du statut de like: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error checking like status: {str(e)}'})
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

def get_user_like_ids(event, user_id, cors_headers):
    """
    Récupère les IDs des pistes likées par un utilisateur
    """
    try:
        # Récupérer les paramètres de requête
        query_params = event.get('queryStringParameters', {}) or {}
        
        # Par défaut, récupérer les likes de l'utilisateur connecté
        target_user_id = query_params.get('userId', user_id)
        
        # Requête pour trouver tous les likes de l'utilisateur
        response = likes_table.query(
            IndexName='user_id-index',
            KeyConditionExpression=Key('user_id').eq(target_user_id)
        )
        
        likes = response.get('Items', [])
        logger.info(f"Nombre de likes trouvés: {len(likes)}")
        
        # Récupérer uniquement les IDs des pistes likées
        track_ids = [like['track_id'] for like in likes]
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'trackIds': track_ids,
                'totalLikes': len(likes)
            })
        }
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des likes: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving likes: {str(e)}'})
        }

def get_user_favorite_ids(event, user_id, cors_headers):
    """
    Récupère les IDs des pistes marquées comme favorites par un utilisateur
    """
    try:
        # Récupérer les paramètres de requête
        query_params = event.get('queryStringParameters', {}) or {}
        
        # Toujours récupérer les favoris de l'utilisateur connecté (les favoris sont privés)
        target_user_id = user_id
        
        # Requête pour trouver tous les favoris de l'utilisateur
        response = favorites_table.query(
            IndexName='user_id-index',
            KeyConditionExpression=Key('user_id').eq(target_user_id)
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

def add_like(event, user_id, cors_headers):
    """
    Ajoute un like à une piste
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
        
        # Construire la clé primaire pour le like
        like_id = f"{user_id}#{track_id}"
        
        # Vérifier si l'utilisateur a déjà liké cette piste
        like_response = likes_table.get_item(Key={'like_id': like_id})
        
        if 'Item' in like_response:
            return {
                'statusCode': 409,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Track already liked'})
            }
        
        # Ajouter le like
        timestamp = int(datetime.datetime.now().timestamp())
        
        likes_table.put_item(
            Item={
                'like_id': like_id,
                'user_id': user_id,
                'track_id': track_id,
                'created_at': timestamp
            }
        )
        
        # Incrémenter le compteur de likes de la piste
        try:
            tracks_table.update_item(
                Key={'track_id': track_id},
                UpdateExpression='SET likes = if_not_exists(likes, :start) + :inc',
                ExpressionAttributeValues={
                    ':inc': 1,
                    ':start': 0
                }
            )
        except Exception as update_error:
            logger.error(f"Erreur lors de la mise à jour du compteur de likes: {str(update_error)}")
            # Continuer malgré l'erreur pour au moins enregistrer le like
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'message': 'Track liked successfully',
                'trackId': track_id
            })
        }
    except Exception as e:
        logger.error(f"Erreur lors de l'ajout d'un like: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error adding like: {str(e)}'})
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
        
        # Pas besoin d'incrémenter un compteur car les favoris sont privés
        
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

def remove_like(event, user_id, cors_headers):
    """
    Supprime un like d'une piste
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
        
        # Construire la clé primaire pour le like
        like_id = f"{user_id}#{track_id}"
        
        # Vérifier si le like existe
        response = likes_table.get_item(Key={'like_id': like_id})
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Like not found'})
            }
        
        # Supprimer le like
        likes_table.delete_item(Key={'like_id': like_id})
        
        # Décrémenter le compteur de likes de la piste
        try:
            tracks_table.update_item(
                Key={'track_id': track_id},
                UpdateExpression='SET likes = if_not_exists(likes, :zero) - :dec',
                ExpressionAttributeValues={
                    ':dec': 1,
                    ':zero': 0
                },
                ConditionExpression='likes > :zero'
            )
        except Exception as update_error:
            logger.error(f"Erreur lors de la mise à jour du compteur de likes: {str(update_error)}")
            # Continuer malgré l'erreur pour au moins supprimer le like
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'message': 'Like removed successfully',
                'trackId': track_id
            }, cls=DecimalEncoder)  # Utiliser DecimalEncoder ici aussi
        }
    except Exception as e:
        logger.error(f"Erreur lors de la suppression d'un like: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error removing like: {str(e)}'})
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
        
        # Pas besoin de décrémenter un compteur car les favoris sont privés
        
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
