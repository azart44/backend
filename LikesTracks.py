import json
import boto3
import os
import logging
import datetime
import traceback
from boto3.dynamodb.conditions import Key, Attr

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')

# Variables d'environnement
LIKES_TABLE = os.environ.get('LIKES_TABLE', 'chordora-track-likes')
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')

# Tables DynamoDB
likes_table = dynamodb.Table(LIKES_TABLE)
tracks_table = dynamodb.Table(TRACKS_TABLE)

def get_cors_headers():
    """
    Renvoie les en-têtes CORS standard
    """
    return {
        'Access-Control-Allow-Origin': 'http://localhost:3000',
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
        
        # Déterminer quelle action effectuer en fonction de la méthode HTTP
        if event['httpMethod'] == 'GET':
            if 'pathParameters' in event and event['pathParameters'] and 'trackId' in event['pathParameters']:
                return check_like_status(event, user_id, cors_headers)
            else:
                return get_user_likes(event, user_id, cors_headers)
        elif event['httpMethod'] == 'POST':
            return add_like(event, user_id, cors_headers)
        elif event['httpMethod'] == 'DELETE':
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

def get_user_likes(event, user_id, cors_headers):
    """
    Récupère toutes les pistes likées par un utilisateur
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
        
        # Récupérer les IDs des pistes likées
        track_ids = [like['track_id'] for like in likes]
        
        # Préparer la réponse
        liked_tracks = []
        
        # Si des pistes ont été trouvées, récupérer leurs détails
        if track_ids:
            # BatchGetItem est limité à 100 items, diviser en chunks si nécessaire
            chunk_size = 100
            chunks = [track_ids[i:i + chunk_size] for i in range(0, len(track_ids), chunk_size)]
            
            for chunk in chunks:
                # Préparer les clés pour BatchGetItem
                keys = [{'track_id': track_id} for track_id in chunk]
                
                # Récupérer les détails des pistes en batch
                batch_response = dynamodb.batch_get_item(
                    RequestItems={
                        TRACKS_TABLE: {
                            'Keys': keys
                        }
                    }
                )
                
                # Ajouter les pistes récupérées à la liste
                if TRACKS_TABLE in batch_response.get('Responses', {}):
                    tracks = batch_response['Responses'][TRACKS_TABLE]
                    liked_tracks.extend(tracks)
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'likedTracks': liked_tracks,
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
            })
        }
    except Exception as e:
        logger.error(f"Erreur lors de la suppression d'un like: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error removing like: {str(e)}'})
        }