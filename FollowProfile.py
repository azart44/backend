import json
import boto3
import os
import time
import logging
from decimal import Decimal
import traceback
from boto3.dynamodb.conditions import Key, Attr

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Variables d'environnement
FOLLOWS_TABLE = os.environ.get('FOLLOWS_TABLE', 'chordora-follows')
USERS_TABLE = os.environ.get('USERS_TABLE', 'chordora-users')

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')
follows_table = dynamodb.Table(FOLLOWS_TABLE)
users_table = dynamodb.Table(USERS_TABLE)

# Classe pour l'encodage des décimaux en JSON
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def get_cors_headers():
    """Renvoie les en-têtes CORS standard"""
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

def lambda_handler(event, context):
    """Gestionnaire principal de la Lambda pour les opérations de suivi"""
    logger.info(f"Événement reçu: {json.dumps(event)}")
    cors_headers = get_cors_headers()
    
    # Requête OPTIONS pour CORS
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('Preflight request successful')
        }
    
    try:
        # Vérifier l'authentification pour les opérations qui la nécessitent
        auth_user_id = None
        if 'requestContext' in event and 'authorizer' in event['requestContext'] and 'claims' in event['requestContext']['authorizer']:
            auth_user_id = event['requestContext']['authorizer']['claims']['sub']
            logger.info(f"Utilisateur authentifié: {auth_user_id}")
        
        # Determiner l'opération à effectuer basée sur la route et la méthode HTTP
        path = event.get('path', '')
        http_method = event.get('httpMethod', '')
        
        # Router vers la fonction appropriée
        if path.endswith('/follows') and http_method == 'GET':
            return get_follow_counts(event, auth_user_id, cors_headers)
        elif path.endswith('/follows/following') and http_method == 'GET':
            return get_following(event, auth_user_id, cors_headers)
        elif path.endswith('/follows/followers') and http_method == 'GET':
            return get_followers(event, auth_user_id, cors_headers)
        elif path.endswith('/follows/status') and http_method == 'GET':
            return get_follow_status(event, auth_user_id, cors_headers)
        elif path.endswith('/follows') and http_method == 'POST':
            return follow_user(event, auth_user_id, cors_headers)
        elif path.endswith('/follows') and http_method == 'DELETE':
            return unfollow_user(event, auth_user_id, cors_headers)
        else:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Route non trouvée'})
            }
            
    except Exception as e:
        logger.error(f"Erreur non gérée: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'message': f'Erreur interne du serveur: {str(e)}'
            })
        }

def follow_user(event, auth_user_id, cors_headers):
    """Fonction pour suivre un utilisateur"""
    if not auth_user_id:
        return {
            'statusCode': 401,
            'headers': cors_headers,
            'body': json.dumps({'message': 'Non autorisé. Connexion requise.'})
        }
    
    # Extraire l'ID de l'utilisateur à suivre (followed)
    if 'body' not in event or not event['body']:
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'message': 'Corps de requête manquant. Veuillez spécifier followed_id'})
        }
    
    body = json.loads(event['body'])
    followed_id = body.get('followed_id')
    
    if not followed_id:
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'message': 'followed_id est requis'})
        }
    
    # Vérifier si l'utilisateur essaie de se suivre lui-même
    if auth_user_id == followed_id:
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'message': 'Vous ne pouvez pas vous suivre vous-même'})
        }
    
    # Vérifier si l'utilisateur à suivre existe
    user_response = users_table.get_item(Key={'userId': followed_id})
    if 'Item' not in user_response:
        return {
            'statusCode': 404,
            'headers': cors_headers,
            'body': json.dumps({'message': 'Utilisateur à suivre non trouvé'})
        }
    
    # Créer l'ID de suivi
    follow_id = f"{auth_user_id}#{followed_id}"
    
    # Vérifier si la relation existe déjà
    existing_follow = follows_table.get_item(Key={'follow_id': follow_id})
    if 'Item' in existing_follow:
        return {
            'statusCode': 409,
            'headers': cors_headers,
            'body': json.dumps({'message': 'Vous suivez déjà cet utilisateur'})
        }
    
    # Ajouter la relation de suivi
    timestamp = int(time.time())
    follows_table.put_item(Item={
        'follow_id': follow_id,
        'follower_id': auth_user_id,
        'followed_id': followed_id,
        'created_at': timestamp
    })
    
    return {
        'statusCode': 201,
        'headers': cors_headers,
        'body': json.dumps({
            'message': 'Suivi avec succès',
            'follow_id': follow_id,
            'follower_id': auth_user_id,
            'followed_id': followed_id,
            'created_at': timestamp
        }, cls=DecimalEncoder)
    }

def unfollow_user(event, auth_user_id, cors_headers):
    """Fonction pour ne plus suivre un utilisateur"""
    if not auth_user_id:
        return {
            'statusCode': 401,
            'headers': cors_headers,
            'body': json.dumps({'message': 'Non autorisé. Connexion requise.'})
        }
    
    # Extraire l'ID de l'utilisateur à ne plus suivre
    if 'body' not in event or not event['body']:
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'message': 'Corps de requête manquant. Veuillez spécifier followed_id'})
        }
    
    body = json.loads(event['body'])
    followed_id = body.get('followed_id')
    
    if not followed_id:
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'message': 'followed_id est requis'})
        }
    
    # Créer l'ID de suivi
    follow_id = f"{auth_user_id}#{followed_id}"
    
    # Vérifier si la relation existe
    existing_follow = follows_table.get_item(Key={'follow_id': follow_id})
    if 'Item' not in existing_follow:
        return {
            'statusCode': 404,
            'headers': cors_headers,
            'body': json.dumps({'message': 'Vous ne suivez pas cet utilisateur'})
        }
    
    # Supprimer la relation de suivi
    follows_table.delete_item(Key={'follow_id': follow_id})
    
    return {
        'statusCode': 200,
        'headers': cors_headers,
        'body': json.dumps({
            'message': 'Arrêt du suivi avec succès',
            'follower_id': auth_user_id,
            'followed_id': followed_id
        })
    }

def get_following(event, auth_user_id, cors_headers):
    """Fonction pour obtenir la liste des utilisateurs suivis"""
    # Extraire l'ID de l'utilisateur dont on veut voir les suivis
    query_params = event.get('queryStringParameters', {}) or {}
    user_id = query_params.get('userId') or auth_user_id
    
    if not user_id:
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'message': 'userId est requis dans les paramètres de requête ou une authentification'})
        }
    
    # Obtenir la liste des IDs suivis
    response = follows_table.query(
        IndexName='follower_id-index',
        KeyConditionExpression=Key('follower_id').eq(user_id)
    )
    
    follows = response.get('Items', [])
    
    # Extraction des IDs des utilisateurs suivis
    followed_ids = [follow['followed_id'] for follow in follows]
    
    # Si aucun suivi, retourner une liste vide
    if not followed_ids:
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'following': [],
                'count': 0
            })
        }
    
    # Récupérer les informations des utilisateurs suivis
    followed_users = []
    
    # BatchGetItem est limité à 100 éléments, donc on traite par lots si nécessaire
    batch_size = 100
    for i in range(0, len(followed_ids), batch_size):
        batch_ids = followed_ids[i:i + batch_size]
        request_items = {
            USERS_TABLE: {
                'Keys': [{'userId': id} for id in batch_ids]
            }
        }
        
        batch_response = dynamodb.batch_get_item(RequestItems=request_items)
        
        if USERS_TABLE in batch_response.get('Responses', {}):
            batch_users = batch_response['Responses'][USERS_TABLE]
            
            # Pour chaque utilisateur, préparer un objet simplifié avec les infos essentielles
            for user in batch_users:
                followed_users.append({
                    'userId': user['userId'],
                    'username': user.get('username', f"User_{user['userId'][-6:]}"),
                    'userType': user.get('userType', ''),
                    'profileImageUrl': user.get('profileImageUrl', ''),
                    'followDate': next((follow['created_at'] for follow in follows if follow['followed_id'] == user['userId']), None)
                })
    
    # Trier les utilisateurs par date de suivi (du plus récent au plus ancien)
    followed_users.sort(key=lambda x: x.get('followDate', 0), reverse=True)
    
    return {
        'statusCode': 200,
        'headers': cors_headers,
        'body': json.dumps({
            'following': followed_users,
            'count': len(followed_users)
        }, cls=DecimalEncoder)
    }

def get_followers(event, auth_user_id, cors_headers):
    """Fonction pour obtenir la liste des followers"""
    # Extraire l'ID de l'utilisateur dont on veut voir les followers
    query_params = event.get('queryStringParameters', {}) or {}
    user_id = query_params.get('userId') or auth_user_id
    
    if not user_id:
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'message': 'userId est requis dans les paramètres de requête ou une authentification'})
        }
    
    # Obtenir la liste des IDs des followers
    response = follows_table.query(
        IndexName='followed_id-index',
        KeyConditionExpression=Key('followed_id').eq(user_id)
    )
    
    follows = response.get('Items', [])
    
    # Extraction des IDs des followers
    follower_ids = [follow['follower_id'] for follow in follows]
    
    # Si aucun follower, retourner une liste vide
    if not follower_ids:
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'followers': [],
                'count': 0
            })
        }
    
    # Récupérer les informations des followers
    followers = []
    
    # BatchGetItem est limité à 100 éléments, donc on traite par lots si nécessaire
    batch_size = 100
    for i in range(0, len(follower_ids), batch_size):
        batch_ids = follower_ids[i:i + batch_size]
        request_items = {
            USERS_TABLE: {
                'Keys': [{'userId': id} for id in batch_ids]
            }
        }
        
        batch_response = dynamodb.batch_get_item(RequestItems=request_items)
        
        if USERS_TABLE in batch_response.get('Responses', {}):
            batch_users = batch_response['Responses'][USERS_TABLE]
            
            # Pour chaque utilisateur, préparer un objet simplifié avec les infos essentielles
            for user in batch_users:
                # Si l'utilisateur authentifié regarde la liste des followers,
                # on vérifie s'il suit également ces followers
                is_following = False
                if auth_user_id and auth_user_id != user['userId']:
                    check_follow = follows_table.get_item(Key={'follow_id': f"{auth_user_id}#{user['userId']}"})
                    is_following = 'Item' in check_follow
                
                followers.append({
                    'userId': user['userId'],
                    'username': user.get('username', f"User_{user['userId'][-6:]}"),
                    'userType': user.get('userType', ''),
                    'profileImageUrl': user.get('profileImageUrl', ''),
                    'followDate': next((follow['created_at'] for follow in follows if follow['follower_id'] == user['userId']), None),
                    'isFollowing': is_following
                })
    
    # Trier les followers par date de suivi (du plus récent au plus ancien)
    followers.sort(key=lambda x: x.get('followDate', 0), reverse=True)
    
    return {
        'statusCode': 200,
        'headers': cors_headers,
        'body': json.dumps({
            'followers': followers,
            'count': len(followers)
        }, cls=DecimalEncoder)
    }

def get_follow_status(event, auth_user_id, cors_headers):
    """Fonction pour vérifier le statut de suivi entre deux utilisateurs"""
    if not auth_user_id:
        return {
            'statusCode': 401,
            'headers': cors_headers,
            'body': json.dumps({'message': 'Non autorisé. Connexion requise.'})
        }
    
    # Extraire l'ID de l'utilisateur à vérifier
    query_params = event.get('queryStringParameters', {}) or {}
    target_id = query_params.get('targetId')
    
    if not target_id:
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'message': 'targetId est requis dans les paramètres de requête'})
        }
    
    # Vérifier si auth_user_id suit target_id
    follow_id = f"{auth_user_id}#{target_id}"
    response = follows_table.get_item(Key={'follow_id': follow_id})
    
    is_following = 'Item' in response
    
    # Vérifier si target_id suit auth_user_id (relation inverse)
    inverse_follow_id = f"{target_id}#{auth_user_id}"
    inverse_response = follows_table.get_item(Key={'follow_id': inverse_follow_id})
    
    is_followed_by = 'Item' in inverse_response
    
    # Construire la réponse
    result = {
        'isFollowing': is_following,
        'isFollowedBy': is_followed_by,
        'follower_id': auth_user_id,
        'followed_id': target_id
    }
    
    # Si la relation de suivi existe, ajouter des détails supplémentaires
    if is_following and 'Item' in response:
        follow_data = response['Item']
        result['followDate'] = follow_data.get('created_at')
    
    return {
        'statusCode': 200,
        'headers': cors_headers,
        'body': json.dumps(result, cls=DecimalEncoder)
    }

def get_follow_counts(event, auth_user_id, cors_headers):
    """Fonction pour obtenir le nombre de followers et de suivis d'un utilisateur"""
    # Extraire l'ID de l'utilisateur
    query_params = event.get('queryStringParameters', {}) or {}
    user_id = query_params.get('userId') or auth_user_id
    
    if not user_id:
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'message': 'userId est requis dans les paramètres de requête ou une authentification'})
        }
    
    # Obtenir le nombre de followers
    followers_response = follows_table.query(
        IndexName='followed_id-index',
        KeyConditionExpression=Key('followed_id').eq(user_id),
        Select='COUNT'
    )
    
    followers_count = followers_response.get('Count', 0)
    
    # Obtenir le nombre de suivis
    following_response = follows_table.query(
        IndexName='follower_id-index',
        KeyConditionExpression=Key('follower_id').eq(user_id),
        Select='COUNT'
    )
    
    following_count = following_response.get('Count', 0)
    
    return {
        'statusCode': 200,
        'headers': cors_headers,
        'body': json.dumps({
            'userId': user_id,
            'followersCount': followers_count,
            'followingCount': following_count
        })
    }