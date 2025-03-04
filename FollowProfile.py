import json
import boto3
import os
import uuid
import logging
from decimal import Decimal
import traceback
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration des tables DynamoDB
dynamodb = boto3.resource('dynamodb')
FOLLOWS_TABLE = os.environ.get('FOLLOWS_TABLE', 'chordora-follows')
USERS_TABLE = os.environ.get('USERS_TABLE', 'chordora-users')

follows_table = dynamodb.Table(FOLLOWS_TABLE)
users_table = dynamodb.Table(USERS_TABLE)

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def get_cors_headers(event):
    """Génère dynamiquement les en-têtes CORS"""
    origin = event.get('headers', {}).get('origin') or event.get('headers', {}).get('Origin') or 'http://localhost:3000'
    return {
        'Access-Control-Allow-Origin': origin,
        'Access-Control-Allow-Methods': 'GET,POST,DELETE,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Credentials': 'true'
    }

def lambda_handler(event, context):
    logger.info(f"Événement reçu: {json.dumps(event)}")
    
    # Gestion des requêtes OPTIONS
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': get_cors_headers(event),
            'body': json.dumps('Preflight request successful')
        }
    
    try:
        # Extraction de l'utilisateur authentifié
        auth_user_id = event['requestContext']['authorizer']['claims']['sub']
        logger.info(f"Utilisateur authentifié: {auth_user_id}")
    except KeyError:
        return {
            'statusCode': 401,
            'headers': get_cors_headers(event),
            'body': json.dumps('Non autorisé')
        }
    
    # Récupération des paramètres
    http_method = event.get('httpMethod')
    path = event.get('path', '')
    query_params = event.get('queryStringParameters', {}) or {}
    path_params = event.get('pathParameters', {}) or {}
    
    # Extraction de l'ID de l'utilisateur ciblé (si présent)
    target_user_id = path_params.get('userId')
    
    try:
        # Routing basé sur le chemin et la méthode
        if path.startswith('/follows/status') and http_method == 'GET':
            return get_follow_status(auth_user_id, target_user_id, get_cors_headers(event))
        
        elif path.startswith('/follows') and http_method == 'GET':
            return get_follow_counts(target_user_id or auth_user_id, get_cors_headers(event))
        
        elif path.startswith('/follows/followers') and http_method == 'GET':
            return get_followers(target_user_id or auth_user_id, get_cors_headers(event))
        
        elif path.startswith('/follows/following') and http_method == 'GET':
            return get_following(target_user_id or auth_user_id, get_cors_headers(event))
        
        elif path.startswith('/follows') and http_method == 'POST':
            body = json.loads(event.get('body', '{}'))
            target_user_id = body.get('followedId')
            return follow_user(auth_user_id, target_user_id, get_cors_headers(event))
        
        elif path.startswith('/follows') and http_method == 'DELETE':
            body = json.loads(event.get('body', '{}'))
            target_user_id = body.get('followedId')
            return unfollow_user(auth_user_id, target_user_id, get_cors_headers(event))
        
        else:
            return {
                'statusCode': 400,
                'headers': get_cors_headers(event),
                'body': json.dumps('Route non valide')
            }
    
    except Exception as e:
        logger.error(f"Erreur non gérée: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': get_cors_headers(event),
            'body': json.dumps({'message': f'Erreur serveur: {str(e)}'})
        }

def get_follow_status(auth_user_id, target_user_id, cors_headers):
    """Vérifier le statut de suivi entre deux utilisateurs"""
    if not target_user_id:
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps('ID utilisateur cible requis')
        }
    
    try:
        # Vérifier si l'utilisateur connecté suit l'utilisateur cible
        is_following_response = follows_table.query(
            IndexName='follower_id-followed_id-index',
            KeyConditionExpression=Key('follower_id').eq(auth_user_id) & Key('followed_id').eq(target_user_id)
        )
        
        # Vérifier si l'utilisateur cible suit l'utilisateur connecté
        is_followed_response = follows_table.query(
            IndexName='follower_id-followed_id-index',
            KeyConditionExpression=Key('follower_id').eq(target_user_id) & Key('followed_id').eq(auth_user_id)
        )
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'isFollowing': len(is_following_response['Items']) > 0,
                'isFollowedBy': len(is_followed_response['Items']) > 0,
                'follower_id': auth_user_id,
                'followed_id': target_user_id
            })
        }
    except Exception as e:
        logger.error(f"Erreur lors de la vérification du statut de suivi: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps(f'Erreur: {str(e)}')
        }

def get_follow_counts(user_id, cors_headers):
    """Récupérer les nombres de followers et de suivis"""
    try:
        # Compter les followers
        followers_count = follows_table.query(
            IndexName='followed_id-index',
            KeyConditionExpression=Key('followed_id').eq(user_id)
        )['Count']
        
        # Compter les suivis
        following_count = follows_table.query(
            IndexName='follower_id-index',
            KeyConditionExpression=Key('follower_id').eq(user_id)
        )['Count']
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'userId': user_id,
                'followersCount': followers_count,
                'followingCount': following_count
            })
        }
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des compteurs de suivi: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps(f'Erreur: {str(e)}')
        }

def get_followers(user_id, cors_headers):
    """Récupérer la liste des followers"""
    try:
        # Requête pour obtenir les followers
        followers_query = follows_table.query(
            IndexName='followed_id-index',
            KeyConditionExpression=Key('followed_id').eq(user_id)
        )
        
        # Récupérer les détails des utilisateurs followers
        followers = []
        for follow in followers_query['Items']:
            user_response = users_table.get_item(Key={'userId': follow['follower_id']})
            if 'Item' in user_response:
                user = user_response['Item']
                followers.append({
                    'userId': user['userId'],
                    'username': user.get('username', ''),
                    'profileImageUrl': user.get('profileImageUrl', ''),
                    'userType': user.get('userType', ''),
                    'followDate': follow['created_at']
                })
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'followers': followers,
                'count': len(followers)
            }, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des followers: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps(f'Erreur: {str(e)}')
        }

def get_following(user_id, cors_headers):
    """Récupérer la liste des utilisateurs suivis"""
    try:
        # Requête pour obtenir les utilisateurs suivis
        following_query = follows_table.query(
            IndexName='follower_id-index',
            KeyConditionExpression=Key('follower_id').eq(user_id)
        )
        
        # Récupérer les détails des utilisateurs suivis
        following = []
        for follow in following_query['Items']:
            user_response = users_table.get_item(Key={'userId': follow['followed_id']})
            if 'Item' in user_response:
                user = user_response['Item']
                following.append({
                    'userId': user['userId'],
                    'username': user.get('username', ''),
                    'profileImageUrl': user.get('profileImageUrl', ''),
                    'userType': user.get('userType', ''),
                    'followDate': follow['created_at']
                })
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'following': following,
                'count': len(following)
            }, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des utilisateurs suivis: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps(f'Erreur: {str(e)}')
        }

def follow_user(follower_id, followed_id, cors_headers):
    """Suivre un utilisateur"""
    if not followed_id:
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps('ID utilisateur à suivre requis')
        }
    
    if follower_id == followed_id:
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps('Impossible de se suivre soi-même')
        }
    
    try:
        # Vérifier si l'utilisateur existe
        target_user = users_table.get_item(Key={'userId': followed_id})
        if 'Item' not in target_user:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps('Utilisateur non trouvé')
            }
        
        # Vérifier si le suivi existe déjà
        existing_follow = follows_table.query(
            IndexName='follower_id-followed_id-index',
            KeyConditionExpression=Key('follower_id').eq(follower_id) & Key('followed_id').eq(followed_id)
        )
        
        if existing_follow['Count'] > 0:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps('Vous suivez déjà cet utilisateur')
            }
        
        # Créer l'entrée de suivi
        follows_table.put_item(Item={
            'follow_id': str(uuid.uuid4()),
            'follower_id': follower_id,
            'followed_id': followed_id,
            'created_at': int(time.time()),
            'updated_at': int(time.time())
        })
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('Utilisateur suivi avec succès')
        }
    
    except Exception as e:
        logger.error(f"Erreur lors du suivi de l'utilisateur: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps(f'Erreur: {str(e)}')
        }

def unfollow_user(follower_id, followed_id, cors_headers):
    """Ne plus suivre un utilisateur"""
    if not followed_id:
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps('ID utilisateur à ne plus suivre requis')
        }
    
    try:
        # Trouver et supprimer l'entrée de suivi
        follow_to_delete = follows_table.query(
            IndexName='follower_id-followed_id-index',
            KeyConditionExpression=Key('follower_id').eq(follower_id) & Key('followed_id').eq(followed_id)
        )
        
        if follow_to_delete['Count'] == 0:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps('Vous ne suivez pas cet utilisateur')
            }
        
        # Supprimer l'entrée
        follows_table.delete_item(
            Key={
                'follow_id': follow_to_delete['Items'][0]['follow_id']
            }
        )
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('Désabonnement réussi')
        }
    
    except Exception as e:
        logger.error(f"Erreur lors du désabonnement: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps(f'Erreur: {str(e)}')
        }