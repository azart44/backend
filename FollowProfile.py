import json
import boto3
import os
import logging
from decimal import Decimal
import datetime
import uuid
from boto3.dynamodb.conditions import Key, Attr

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Variables d'environnement
FOLLOWS_TABLE = os.environ.get('FOLLOWS_TABLE', 'chordora-follows')
USERS_TABLE = os.environ.get('USERS_TABLE', 'chordora-users')

# Initialisation des clients DynamoDB
dynamodb = boto3.resource('dynamodb')
follows_table = dynamodb.Table(FOLLOWS_TABLE)
users_table = dynamodb.Table(USERS_TABLE)

# Classe pour encoder les décimaux en JSON
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# En-têtes CORS
def get_cors_headers():
    return {
        'Access-Control-Allow-Origin': 'http://localhost:3000',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,POST,DELETE,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

def lambda_handler(event, context):
    """Point d'entrée principal de la fonction Lambda."""
    logger.info(f"Événement reçu: {json.dumps(event)}")
    
    # En-têtes CORS
    cors_headers = get_cors_headers()
    
    # Gestion des requêtes OPTIONS (CORS)
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('Preflight request successful')
        }
    
    # Récupérer l'ID utilisateur de l'authentification
    try:
        current_user_id = event['requestContext']['authorizer']['claims']['sub']
        logger.info(f"Utilisateur authentifié: {current_user_id}")
    except KeyError:
        logger.error("Impossible de récupérer l'ID utilisateur authentifié")
        return {
            'statusCode': 401,
            'headers': cors_headers,
            'body': json.dumps({'message': 'Unauthorized: User ID not found in token'})
        }
    
    # Identifier le type de route et de méthode
    http_method = event['httpMethod']
    path = event['path']
    logger.info(f"Méthode: {http_method}, Chemin: {path}")
    
    try:
        # ROUTE 1: /follow/following/{userId} - Obtenir les abonnements d'un utilisateur
        if '/follow/following/' in path and http_method == 'GET':
            user_id = event['pathParameters']['userId']
            logger.info(f"Récupération des abonnements pour l'utilisateur: {user_id}")
            return get_following(user_id, cors_headers)
        
        # ROUTE 2: /follow/followers/{userId} - Obtenir les abonnés d'un utilisateur
        elif '/follow/followers/' in path and http_method == 'GET':
            user_id = event['pathParameters']['userId']
            logger.info(f"Récupération des abonnés pour l'utilisateur: {user_id}")
            return get_followers(user_id, cors_headers)
        
        # ROUTE 3: /follow/status/{targetId} - Vérifier le statut de follow entre l'utilisateur courant et un autre
        elif '/follow/status/' in path and http_method == 'GET':
            target_id = event['pathParameters']['targetId']
            logger.info(f"Vérification du statut de follow entre {current_user_id} et {target_id}")
            return get_follow_status(current_user_id, target_id, cors_headers)
        
        # ROUTE 4: /follow/{userId} - Obtenir les compteurs de follow pour un utilisateur
        elif path.startswith('/follow/') and 'following' not in path and 'followers' not in path and 'status' not in path and http_method == 'GET':
            # Si l'URL est /follow sans userId, utiliser l'utilisateur courant
            if 'pathParameters' not in event or not event['pathParameters'] or 'userId' not in event['pathParameters']:
                user_id = current_user_id
            else:
                user_id = event['pathParameters']['userId']
            logger.info(f"Récupération des compteurs pour l'utilisateur: {user_id}")
            return get_follow_counts(user_id, cors_headers)
        
        # ROUTE 5: /follow - POST - Suivre un utilisateur
        elif path == '/follow' and http_method == 'POST':
            # Récupérer l'ID de l'utilisateur à suivre depuis le corps de la requête
            try:
                body = json.loads(event['body']) if event.get('body') else {}
                followed_id = body.get('followedId')
                
                if not followed_id:
                    return {
                        'statusCode': 400,
                        'headers': cors_headers,
                        'body': json.dumps({'message': 'Missing followedId in request body'})
                    }
                
                logger.info(f"L'utilisateur {current_user_id} suit {followed_id}")
                return follow_user(current_user_id, followed_id, cors_headers)
                
            except json.JSONDecodeError:
                logger.error("Erreur de parsing JSON")
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'message': 'Invalid JSON in request body'})
                }
        
        # ROUTE 6: /follow - DELETE - Ne plus suivre un utilisateur
        elif path == '/follow' and http_method == 'DELETE':
            # Récupérer l'ID de l'utilisateur à ne plus suivre depuis le corps de la requête
            try:
                body = json.loads(event['body']) if event.get('body') else {}
                followed_id = body.get('followedId')
                
                if not followed_id:
                    return {
                        'statusCode': 400,
                        'headers': cors_headers,
                        'body': json.dumps({'message': 'Missing followedId in request body'})
                    }
                
                logger.info(f"L'utilisateur {current_user_id} ne suit plus {followed_id}")
                return unfollow_user(current_user_id, followed_id, cors_headers)
                
            except json.JSONDecodeError:
                logger.error("Erreur de parsing JSON")
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'message': 'Invalid JSON in request body'})
                }
        
        # Si la route n'est pas reconnue
        logger.warning(f"Route non supportée: {path}, méthode: {http_method}")
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'message': 'Route not supported'})
        }
    
    except Exception as e:
        logger.error(f"Erreur dans la fonction lambda: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }

def get_following(user_id, cors_headers):
    """
    Récupère la liste des utilisateurs suivis par un utilisateur.
    Route: GET /follow/following/{userId}
    """
    try:
        # Utiliser l'index follower_id pour trouver tous les suivis de cet utilisateur
        response = follows_table.query(
            IndexName='follower_id-index',
            KeyConditionExpression=Key('follower_id').eq(user_id)
        )
        
        follows = response.get('Items', [])
        logger.info(f"Nombre d'abonnements trouvés: {len(follows)}")
        logger.debug(f"Données brutes: {json.dumps(follows)}")
        
        # Si aucun abonnement n'est trouvé, retourner une liste vide
        if not follows:
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'following': [],
                    'count': 0
                }, cls=DecimalEncoder)
            }
        
        # Récupérer les IDs des utilisateurs suivis
        followed_ids = [follow['followed_id'] for follow in follows]
        logger.info(f"IDs des utilisateurs suivis: {followed_ids}")
        
        # Récupérer les informations des profils
        following_profiles = []
        for followed_id in followed_ids:
            try:
                user_response = users_table.get_item(Key={'userId': followed_id})
                
                if 'Item' in user_response:
                    user = user_response['Item']
                    # Créer un profil simplifié pour l'API
                    profile = {
                        'userId': followed_id,
                        'username': user.get('username', f"User_{followed_id[:6]}"),
                        'userType': user.get('userType', ''),
                        'profileImageUrl': user.get('profileImageUrl', '')
                    }
                    following_profiles.append(profile)
                else:
                    logger.warning(f"Profil non trouvé pour l'utilisateur: {followed_id}")
            except Exception as e:
                logger.error(f"Erreur lors de la récupération du profil {followed_id}: {str(e)}")
        
        # Renvoyer le résultat
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'following': following_profiles,
                'count': len(following_profiles)
            }, cls=DecimalEncoder)
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des abonnements: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'message': f'Error retrieving following list: {str(e)}'
            })
        }

def get_followers(user_id, cors_headers):
    """
    Récupère la liste des abonnés d'un utilisateur.
    Route: GET /follow/followers/{userId}
    """
    try:
        # Utiliser l'index followed_id pour trouver tous les abonnés de cet utilisateur
        response = follows_table.query(
            IndexName='followed_id-index',
            KeyConditionExpression=Key('followed_id').eq(user_id)
        )
        
        follows = response.get('Items', [])
        logger.info(f"Nombre d'abonnés trouvés: {len(follows)}")
        
        # Si aucun abonné n'est trouvé, retourner une liste vide
        if not follows:
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'followers': [],
                    'count': 0
                }, cls=DecimalEncoder)
            }
        
        # Récupérer les IDs des abonnés
        follower_ids = [follow['follower_id'] for follow in follows]
        logger.info(f"IDs des abonnés: {follower_ids}")
        
        # Récupérer les informations des profils
        follower_profiles = []
        for follower_id in follower_ids:
            try:
                user_response = users_table.get_item(Key={'userId': follower_id})
                
                if 'Item' in user_response:
                    user = user_response['Item']
                    # Créer un profil simplifié pour l'API
                    profile = {
                        'userId': follower_id,
                        'username': user.get('username', f"User_{follower_id[:6]}"),
                        'userType': user.get('userType', ''),
                        'profileImageUrl': user.get('profileImageUrl', '')
                    }
                    follower_profiles.append(profile)
                else:
                    logger.warning(f"Profil non trouvé pour l'utilisateur: {follower_id}")
            except Exception as e:
                logger.error(f"Erreur lors de la récupération du profil {follower_id}: {str(e)}")
        
        # Renvoyer le résultat
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'followers': follower_profiles,
                'count': len(follower_profiles)
            }, cls=DecimalEncoder)
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des abonnés: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'message': f'Error retrieving followers list: {str(e)}'
            })
        }

def get_follow_status(follower_id, followed_id, cors_headers):
    """
    Vérifie le statut de follow entre deux utilisateurs.
    Route: GET /follow/status/{targetId}
    """
    try:
        # Vérifie si l'utilisateur suit la cible
        response1 = follows_table.get_item(Key={
            'follower_id': follower_id,
            'followed_id': followed_id
        })
        
        is_following = 'Item' in response1
        
        # Vérifie si la cible suit l'utilisateur
        response2 = follows_table.get_item(Key={
            'follower_id': followed_id,
            'followed_id': follower_id
        })
        
        is_followed_by = 'Item' in response2
        
        # Renvoyer le statut
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'isFollowing': is_following,
                'isFollowedBy': is_followed_by,
                'follower_id': follower_id,
                'followed_id': followed_id
            })
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la vérification du statut de follow: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'message': f'Error checking follow status: {str(e)}'
            })
        }

def get_follow_counts(user_id, cors_headers):
    """
    Récupère les compteurs de followers et following pour un utilisateur.
    Route: GET /follow/{userId}
    """
    try:
        # Compter les abonnés (followers)
        followers_response = follows_table.query(
            IndexName='followed_id-index',
            KeyConditionExpression=Key('followed_id').eq(user_id),
            Select='COUNT'
        )
        followers_count = followers_response.get('Count', 0)
        
        # Compter les abonnements (following)
        following_response = follows_table.query(
            IndexName='follower_id-index',
            KeyConditionExpression=Key('follower_id').eq(user_id),
            Select='COUNT'
        )
        following_count = following_response.get('Count', 0)
        
        # Renvoyer les compteurs
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
        logger.error(f"Erreur lors de la récupération des compteurs: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'message': f'Error retrieving follow counts: {str(e)}'
            })
        }

def follow_user(follower_id, followed_id, cors_headers):
    """
    Permet à un utilisateur d'en suivre un autre.
    Route: POST /follow
    """
    try:
        # Vérifier que l'utilisateur ne tente pas de se suivre lui-même
        if follower_id == followed_id:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({
                    'message': 'You cannot follow yourself'
                })
            }
        
        # Vérifier si la relation existe déjà
        existing = follows_table.get_item(Key={
            'follower_id': follower_id,
            'followed_id': followed_id
        })
        
        if 'Item' in existing:
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'message': 'Already following this user',
                    'isFollowing': True
                })
            }
        
        # Créer une nouvelle relation de follow
        timestamp = int(datetime.datetime.now().timestamp())
        follow_id = str(uuid.uuid4())
        
        follows_table.put_item(Item={
            'follow_id': follow_id,
            'follower_id': follower_id,
            'followed_id': followed_id,
            'created_at': timestamp,
            'updated_at': timestamp
        })
        
        return {
            'statusCode': 201,
            'headers': cors_headers,
            'body': json.dumps({
                'message': 'Successfully followed user',
                'follow_id': follow_id,
                'follower_id': follower_id,
                'followed_id': followed_id,
                'created_at': timestamp
            })
        }
    
    except Exception as e:
        logger.error(f"Erreur lors du suivi de l'utilisateur: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'message': f'Error following user: {str(e)}'
            })
        }

def unfollow_user(follower_id, followed_id, cors_headers):
    """
    Permet à un utilisateur de ne plus en suivre un autre.
    Route: DELETE /follow
    """
    try:
        # Vérifier si la relation existe
        existing = follows_table.get_item(Key={
            'follower_id': follower_id,
            'followed_id': followed_id
        })
        
        if 'Item' not in existing:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({
                    'message': 'Not following this user',
                    'isFollowing': False
                })
            }
        
        # Supprimer la relation
        follows_table.delete_item(Key={
            'follower_id': follower_id,
            'followed_id': followed_id
        })
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'message': 'Successfully unfollowed user',
                'follower_id': follower_id,
                'followed_id': followed_id
            })
        }
    
    except Exception as e:
        logger.error(f"Erreur lors du désabonnement de l'utilisateur: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'message': f'Error unfollowing user: {str(e)}'
            })
        }