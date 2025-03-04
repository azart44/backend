import json
import boto3
import os
import datetime
import logging
from boto3.dynamodb.conditions import Key, Attr
import traceback
from decimal import Decimal

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
    """Retourne les en-têtes CORS standard"""
    return {
        'Access-Control-Allow-Origin': 'http://localhost:3000',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

def lambda_handler(event, context):
    """
    Gestionnaire principal de la Lambda - traite toutes les opérations liées aux abonnements
    """
    logger.info(f"Événement reçu: {json.dumps(event)}")
    cors_headers = get_cors_headers()
    
    # Gestion CORS pre-flight
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('Preflight request successful')
        }
    
    # Vérification d'authentification
    try:
        auth_context = event['requestContext']['authorizer']['claims']
        follower_id = auth_context['sub']
        logger.info(f"Utilisateur authentifié: {follower_id}")
    except (KeyError, TypeError) as e:
        logger.error(f"Erreur d'authentification: {str(e)}")
        return {
            'statusCode': 401,
            'headers': cors_headers,
            'body': json.dumps({'message': 'Unauthorized: Authentication required'})
        }
    
    # Router vers les fonctions appropriées en fonction de la méthode et du chemin
    http_method = event['httpMethod']
    path = event.get('path', '').rstrip('/')
    path_parameters = event.get('pathParameters', {}) or {}
    
    try:
        if http_method == 'POST':
            # Suivre un utilisateur
            try:
                body = json.loads(event['body']) if event.get('body') else {}
                followed_id = body.get('followedId')
                
                if not followed_id:
                    return {
                        'statusCode': 400,
                        'headers': cors_headers,
                        'body': json.dumps({'message': 'followedId is required'})
                    }
                
                return follow_user(follower_id, followed_id, cors_headers)
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Erreur de traitement du body: {str(e)}")
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'message': f'Invalid request body: {str(e)}'})
                }
                
        elif http_method == 'DELETE':
            # Ne plus suivre un utilisateur
            try:
                body = json.loads(event['body']) if event.get('body') else {}
                followed_id = body.get('followedId')
                
                if not followed_id:
                    return {
                        'statusCode': 400,
                        'headers': cors_headers,
                        'body': json.dumps({'message': 'followedId is required'})
                    }
                
                return unfollow_user(follower_id, followed_id, cors_headers)
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Erreur de traitement du body: {str(e)}")
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'message': f'Invalid request body: {str(e)}'})
                }
        
        elif http_method == 'GET':
            # Vérifier le statut de suivi ou obtenir les liste des followers/following
            if path.endswith('/status') or '/status/' in path:
                if '/status/' in path:
                    target_id = path.split('/status/')[1]
                elif 'targetId' in path_parameters:
                    target_id = path_parameters['targetId']
                else:
                    return {
                        'statusCode': 400,
                        'headers': cors_headers,
                        'body': json.dumps({'message': 'Target ID is required'})
                    }
                    
                return get_follow_status(follower_id, target_id, cors_headers)
                
            elif path.endswith('/followers') or '/followers/' in path:
                if '/followers/' in path:
                    user_id = path.split('/followers/')[1]
                elif 'userId' in path_parameters:
                    user_id = path_parameters['userId']
                else:
                    user_id = follower_id
                    
                return get_followers(user_id, follower_id, cors_headers)
                
            elif path.endswith('/following') or '/following/' in path:
                if '/following/' in path:
                    user_id = path.split('/following/')[1]
                elif 'userId' in path_parameters:
                    user_id = path_parameters['userId']
                else:
                    user_id = follower_id
                    
                return get_following(user_id, follower_id, cors_headers)
                
            else:
                # Compteurs pour l'utilisateur spécifié ou l'utilisateur authentifié
                if '/' in path and path.split('/')[-1] not in ['follow', 'follows']:
                    user_id = path.split('/')[-1]
                elif 'userId' in path_parameters:
                    user_id = path_parameters['userId']
                else:
                    user_id = follower_id
                    
                return get_follow_counts(user_id, cors_headers)
                
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'message': 'Invalid request method or path'})
        }
        
    except Exception as e:
        logger.error(f"Erreur non gérée: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }

def follow_user(follower_id, followed_id, cors_headers):
    """
    Permet à un utilisateur d'en suivre un autre
    """
    # Vérifier que l'utilisateur ne se suit pas lui-même
    if follower_id == followed_id:
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'message': 'Cannot follow yourself'})
        }
    
    # Vérifier que l'utilisateur à suivre existe
    try:
        user_response = users_table.get_item(Key={'userId': followed_id})
        if 'Item' not in user_response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'User to follow not found'})
            }
    except Exception as e:
        logger.error(f"Erreur lors de la vérification de l'utilisateur à suivre: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error verifying user to follow: {str(e)}'})
        }
    
    # Vérifier si l'abonnement existe déjà
    follow_id = f"{follower_id}#{followed_id}"
    
    try:
        response = follows_table.get_item(Key={'follow_id': follow_id})
        
        if 'Item' in response:
            # L'abonnement existe déjà
            logger.info(f"L'utilisateur {follower_id} suit déjà {followed_id}")
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'message': 'Already following this user',
                    'isFollowing': True,
                    'followedId': followed_id,
                    'followerId': follower_id
                })
            }
        
        # Créer l'abonnement
        timestamp = int(datetime.datetime.now().timestamp())
        
        follows_table.put_item(
            Item={
                'follow_id': follow_id,
                'follower_id': follower_id,
                'followed_id': followed_id,
                'created_at': timestamp
            }
        )
        
        logger.info(f"L'utilisateur {follower_id} suit maintenant {followed_id}")
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'message': 'Successfully followed user',
                'isFollowing': True,
                'followedId': followed_id,
                'followerId': follower_id
            })
        }
    except Exception as e:
        logger.error(f"Erreur lors de l'ajout de l'abonnement: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error adding follow: {str(e)}'})
        }

def unfollow_user(follower_id, followed_id, cors_headers):
    """
    Permet à un utilisateur de ne plus en suivre un autre
    """
    follow_id = f"{follower_id}#{followed_id}"
    
    try:
        # Vérifier si l'abonnement existe
        response = follows_table.get_item(Key={'follow_id': follow_id})
        
        if 'Item' not in response:
            # L'abonnement n'existe pas
            logger.info(f"L'utilisateur {follower_id} ne suit pas {followed_id}")
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'message': 'Not following this user',
                    'isFollowing': False,
                    'followedId': followed_id,
                    'followerId': follower_id
                })
            }
        
        # Supprimer l'abonnement
        follows_table.delete_item(Key={'follow_id': follow_id})
        
        logger.info(f"L'utilisateur {follower_id} ne suit plus {followed_id}")
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'message': 'Successfully unfollowed user',
                'isFollowing': False,
                'followedId': followed_id,
                'followerId': follower_id
            })
        }
    except Exception as e:
        logger.error(f"Erreur lors de la suppression de l'abonnement: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error removing follow: {str(e)}'})
        }

def get_follow_status(follower_id, target_id, cors_headers):
    """
    Vérifie si un utilisateur en suit un autre et vice versa
    """
    try:
        # Vérifier si follower_id suit target_id
        follow_id = f"{follower_id}#{target_id}"
        response1 = follows_table.get_item(Key={'follow_id': follow_id})
        is_following = 'Item' in response1
        
        # Vérifier si target_id suit follower_id
        follow_id_reverse = f"{target_id}#{follower_id}"
        response2 = follows_table.get_item(Key={'follow_id': follow_id_reverse})
        is_followed_by = 'Item' in response2
        
        logger.info(f"Statut de suivi: {follower_id} -> {target_id}: {is_following}, {target_id} -> {follower_id}: {is_followed_by}")
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'isFollowing': is_following,
                'isFollowedBy': is_followed_by,
                'follower_id': follower_id,
                'followed_id': target_id
            })
        }
    except Exception as e:
        logger.error(f"Erreur lors de la vérification du statut de suivi: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error checking follow status: {str(e)}'})
        }

def get_follow_counts(user_id, cors_headers):
    """
    Obtient le nombre de followers et d'abonnements d'un utilisateur
    """
    try:
        # Vérifier que l'utilisateur existe
        user_response = users_table.get_item(Key={'userId': user_id})
        if 'Item' not in user_response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'User not found'})
            }
            
        # Compter les followers (ceux qui suivent l'utilisateur)
        followers_response = follows_table.query(
            IndexName='followed_id-index',
            KeyConditionExpression=Key('followed_id').eq(user_id)
        )
        followers_count = followers_response.get('Count', 0)
        
        # Compter les following (ceux que l'utilisateur suit)
        following_response = follows_table.query(
            IndexName='follower_id-index',
            KeyConditionExpression=Key('follower_id').eq(user_id)
        )
        following_count = following_response.get('Count', 0)
        
        logger.info(f"Compteurs pour {user_id}: {followers_count} abonnés, {following_count} abonnements")
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'userId': user_id,
                'followersCount': followers_count,
                'followingCount': following_count
            }, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Erreur lors du comptage des relations de suivi: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error counting follows: {str(e)}'})
        }

def get_followers(user_id, current_user_id, cors_headers):
    """
    Obtient la liste des followers d'un utilisateur
    """
    try:
        # Vérifier que l'utilisateur existe
        user_response = users_table.get_item(Key={'userId': user_id})
        if 'Item' not in user_response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'User not found'})
            }
            
        # Récupérer les followers
        followers_response = follows_table.query(
            IndexName='followed_id-index',
            KeyConditionExpression=Key('followed_id').eq(user_id)
        )
        
        followers_items = followers_response.get('Items', [])
        follower_ids = [item['follower_id'] for item in followers_items]
        
        # Récupérer les informations de profil des followers
        followers_profiles = []
        
        for follower_id in follower_ids:
            follower_response = users_table.get_item(Key={'userId': follower_id})
            if 'Item' in follower_response:
                follower = follower_response['Item']
                
                # Créer un objet profil simplifié
                profile = {
                    'userId': follower_id,
                    'username': follower.get('username', f"User_{follower_id[:6]}"),
                    'userType': follower.get('userType', ''),
                    'profileImageUrl': follower.get('profileImageUrl', '')
                }
                
                # Ajouter la date de suivi
                for item in followers_items:
                    if item['follower_id'] == follower_id:
                        profile['followDate'] = item.get('created_at')
                
                # Vérifier si l'utilisateur courant suit ce follower
                if current_user_id != follower_id:
                    follow_id = f"{current_user_id}#{follower_id}"
                    is_following_response = follows_table.get_item(Key={'follow_id': follow_id})
                    profile['isFollowing'] = 'Item' in is_following_response
                
                followers_profiles.append(profile)
        
        # Trier par date de suivi (le plus récent en premier)
        followers_profiles = sorted(
            followers_profiles, 
            key=lambda x: x.get('followDate', 0), 
            reverse=True
        )
        
        logger.info(f"Récupéré {len(followers_profiles)} followers pour {user_id}")
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'userId': user_id,
                'followers': followers_profiles,
                'count': len(followers_profiles)
            }, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des followers: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving followers: {str(e)}'})
        }

def get_following(user_id, current_user_id, cors_headers):
    """
    Obtient la liste des utilisateurs suivis par un utilisateur
    """
    try:
        # Vérifier que l'utilisateur existe
        user_response = users_table.get_item(Key={'userId': user_id})
        if 'Item' not in user_response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'User not found'})
            }
            
        # Récupérer les abonnements
        following_response = follows_table.query(
            IndexName='follower_id-index',
            KeyConditionExpression=Key('follower_id').eq(user_id)
        )
        
        following_items = following_response.get('Items', [])
        followed_ids = [item['followed_id'] for item in following_items]
        
        # Récupérer les informations de profil des utilisateurs suivis
        following_profiles = []
        
        for followed_id in followed_ids:
            followed_response = users_table.get_item(Key={'userId': followed_id})
            if 'Item' in followed_response:
                followed = followed_response['Item']
                
                # Créer un objet profil simplifié
                profile = {
                    'userId': followed_id,
                    'username': followed.get('username', f"User_{followed_id[:6]}"),
                    'userType': followed.get('userType', ''),
                    'profileImageUrl': followed.get('profileImageUrl', '')
                }
                
                # Ajouter la date de suivi
                for item in following_items:
                    if item['followed_id'] == followed_id:
                        profile['followDate'] = item.get('created_at')
                
                # Vérifier si l'utilisateur courant suit cette personne
                if current_user_id != user_id and current_user_id != followed_id:
                    follow_id = f"{current_user_id}#{followed_id}"
                    is_following_response = follows_table.get_item(Key={'follow_id': follow_id})
                    profile['isFollowing'] = 'Item' in is_following_response
                elif current_user_id == user_id:
                    profile['isFollowing'] = True
                
                following_profiles.append(profile)
        
        # Trier par date de suivi (le plus récent en premier)
        following_profiles = sorted(
            following_profiles, 
            key=lambda x: x.get('followDate', 0), 
            reverse=True
        )
        
        logger.info(f"Récupéré {len(following_profiles)} abonnements pour {user_id}")
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'userId': user_id,
                'following': following_profiles,
                'count': len(following_profiles)
            }, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des abonnements: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving following: {str(e)}'})
        }