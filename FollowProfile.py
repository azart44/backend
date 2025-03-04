import json
import boto3
import os
import logging
from decimal import Decimal
import traceback
from boto3.dynamodb.conditions import Key, Attr
import time

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

def get_cors_headers(event):
    """
    Génère les en-têtes CORS dynamiques basés sur l'origine de la requête.
    """
    origin = None
    if 'headers' in event and event['headers']:
        origin = event['headers'].get('origin') or event['headers'].get('Origin')
    
    allowed_origin = origin if origin else 'http://localhost:3000'
    
    return {
        'Access-Control-Allow-Origin': allowed_origin,
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,OPTIONS,POST,DELETE',
        'Access-Control-Allow-Credentials': 'true'
    }

def lambda_handler(event, context):
    """
    Fonction principale qui gère les requêtes de follow/unfollow et de récupération des statuts
    """
    logger.info(f"Événement reçu: {json.dumps(event)}")
    
    # Générer les en-têtes CORS
    cors_headers = get_cors_headers(event)
    
    # Gestion des requêtes OPTIONS (pre-flight CORS)
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('Preflight request successful')
        }
    
    try:
        # Vérifier l'authentification
        if 'requestContext' not in event or 'authorizer' not in event['requestContext'] or 'claims' not in event['requestContext']['authorizer']:
            logger.error("Informations d'authentification manquantes")
            return {
                'statusCode': 401,
                'headers': cors_headers,
                'body': json.dumps('Unauthorized: Missing authentication information')
            }
        
        # Extraire l'ID utilisateur authentifié
        auth_user_id = event['requestContext']['authorizer']['claims']['sub']
        logger.info(f"Utilisateur authentifié: {auth_user_id}")
        
        # Déterminer le type de requête par la méthode et les paramètres
        http_method = event['httpMethod']
        path = event.get('path', '')
        resource = event.get('resource', '')
        path_parameters = event.get('pathParameters', {}) or {}
        
        # Récupération des followers de l'utilisateur courant ou spécifié
        if http_method == 'GET' and '/followers' in path:
            target_user_id = path_parameters.get('userId', auth_user_id)
            logger.info(f"Récupération des followers pour l'utilisateur: {target_user_id}")
            return get_followers(target_user_id, auth_user_id, cors_headers)
        
        # Récupération des utilisateurs suivis par l'utilisateur courant ou spécifié
        elif http_method == 'GET' and '/following' in path:
            target_user_id = path_parameters.get('userId', auth_user_id)
            logger.info(f"Récupération des following pour l'utilisateur: {target_user_id}")
            return get_following(target_user_id, auth_user_id, cors_headers)
        
        # Vérification du statut de follow entre l'utilisateur authentifié et un autre
        elif http_method == 'GET' and '/status/' in path:
            target_id = path_parameters.get('targetId', '')
            logger.info(f"Vérification du statut de follow entre {auth_user_id} et {target_id}")
            return check_follow_status(auth_user_id, target_id, cors_headers)
        
        # Récupération des compteurs de followers/following
        elif http_method == 'GET' and resource == '/follow/{userId}':
            target_user_id = path_parameters.get('userId', '')
            logger.info(f"Récupération des compteurs pour l'utilisateur: {target_user_id}")
            return get_follow_counts(target_user_id, cors_headers)
        
        elif http_method == 'GET' and resource == '/follow':
            logger.info(f"Récupération des compteurs pour l'utilisateur authentifié: {auth_user_id}")
            return get_follow_counts(auth_user_id, cors_headers)
        
        # Ajouter un follow
        elif http_method == 'POST':
            # Récupérer le corps de la requête
            body = json.loads(event.get('body', '{}'))
            followed_id = body.get('followedId', '')
            
            if not followed_id:
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'message': 'followedId is required'})
                }
            
            logger.info(f"Ajout de follow: {auth_user_id} suit {followed_id}")
            return add_follow(auth_user_id, followed_id, cors_headers)
        
        # Supprimer un follow
        elif http_method == 'DELETE':
            # Pour DELETE, le corps peut être dans l'événement ou dans les paramètres
            body = {}
            if 'body' in event and event['body']:
                body = json.loads(event['body'])
            
            followed_id = body.get('followedId', '')
            if 'userId' in path_parameters:
                followed_id = path_parameters['userId']
            
            if not followed_id:
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'message': 'followedId is required'})
                }
            
            logger.info(f"Suppression de follow: {auth_user_id} ne suit plus {followed_id}")
            return remove_follow(auth_user_id, followed_id, cors_headers)
        
        else:
            logger.warn(f"Méthode non supportée: {http_method} {path}")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Unsupported method or path'})
            }
    
    except Exception as e:
        logger.error(f"Erreur non gérée: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }

def get_followers(user_id, auth_user_id, cors_headers):
    """
    Récupère la liste des utilisateurs qui suivent un utilisateur spécifique
    """
    try:
        # Requête pour trouver tous les follows où l'utilisateur est suivi
        response = follows_table.query(
            IndexName='followed_id-index',
            KeyConditionExpression=Key('followed_id').eq(user_id)
        )
        
        follows = response.get('Items', [])
        logger.info(f"Nombre de followers trouvés: {len(follows)}")
        
        # Si aucun follower, retourner une liste vide
        if not follows:
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'followers': [],
                    'count': 0
                }, cls=DecimalEncoder)
            }
        
        # Récupérer les informations de profil des followers
        followers_profiles = []
        
        for follow in follows:
            follower_id = follow['follower_id']
            
            # Vérifier si l'utilisateur authentifié suit ce follower
            is_following = False
            if auth_user_id:
                follow_id = f"{auth_user_id}#{follower_id}"
                follow_response = follows_table.get_item(Key={'follow_id': follow_id})
                is_following = 'Item' in follow_response
            
            # Récupérer le profil du follower
            try:
                user_response = users_table.get_item(Key={'userId': follower_id})
                if 'Item' in user_response:
                    user_profile = user_response['Item']
                    
                    # Construire un objet simplifié du profil
                    follower_profile = {
                        'userId': follower_id,
                        'username': user_profile.get('username', f"User_{follower_id[-6:]}"),
                        'userType': user_profile.get('userType', ''),
                        'profileImageUrl': user_profile.get('profileImageUrl', ''),
                        'followDate': follow.get('created_at', 0),
                        'isFollowing': is_following
                    }
                    
                    followers_profiles.append(follower_profile)
                else:
                    # Si le profil n'existe pas, ajouter uniquement l'ID
                    followers_profiles.append({
                        'userId': follower_id,
                        'username': f"User_{follower_id[-6:]}",
                        'followDate': follow.get('created_at', 0),
                        'isFollowing': is_following
                    })
            except Exception as profile_error:
                logger.error(f"Erreur lors de la récupération du profil {follower_id}: {str(profile_error)}")
                # Continuer avec le prochain follower
        
        # Trier par date de follow (du plus récent au plus ancien)
        followers_profiles.sort(key=lambda x: x.get('followDate', 0), reverse=True)
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
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
            'body': json.dumps({'message': f'Error retrieving followers list: {str(e)}'})
        }

def get_following(user_id, auth_user_id, cors_headers):
    """
    Récupère la liste des utilisateurs suivis par un utilisateur spécifique
    """
    try:
        # Requête pour trouver tous les follows où l'utilisateur suit d'autres personnes
        response = follows_table.query(
            IndexName='follower_id-index',
            KeyConditionExpression=Key('follower_id').eq(user_id)
        )
        
        follows = response.get('Items', [])
        logger.info(f"Nombre d'utilisateurs suivis trouvés: {len(follows)}")
        
        # Si aucun suivi, retourner une liste vide
        if not follows:
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'following': [],
                    'count': 0
                }, cls=DecimalEncoder)
            }
        
        # Récupérer les informations de profil des utilisateurs suivis
        following_profiles = []
        
        for follow in follows:
            followed_id = follow['followed_id']
            
            # Récupérer le profil de l'utilisateur suivi
            try:
                user_response = users_table.get_item(Key={'userId': followed_id})
                if 'Item' in user_response:
                    user_profile = user_response['Item']
                    
                    # Construire un objet simplifié du profil
                    followed_profile = {
                        'userId': followed_id,
                        'username': user_profile.get('username', f"User_{followed_id[-6:]}"),
                        'userType': user_profile.get('userType', ''),
                        'profileImageUrl': user_profile.get('profileImageUrl', ''),
                        'followDate': follow.get('created_at', 0),
                        'isFollowing': True  # L'utilisateur suit cette personne, donc toujours true
                    }
                    
                    following_profiles.append(followed_profile)
                else:
                    # Si le profil n'existe pas, ajouter uniquement l'ID
                    following_profiles.append({
                        'userId': followed_id,
                        'username': f"User_{followed_id[-6:]}",
                        'followDate': follow.get('created_at', 0),
                        'isFollowing': True
                    })
            except Exception as profile_error:
                logger.error(f"Erreur lors de la récupération du profil {followed_id}: {str(profile_error)}")
                # Continuer avec le prochain utilisateur suivi
        
        # Trier par date de follow (du plus récent au plus ancien)
        following_profiles.sort(key=lambda x: x.get('followDate', 0), reverse=True)
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'following': following_profiles,
                'count': len(following_profiles)
            }, cls=DecimalEncoder)
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des following: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving following list: {str(e)}'})
        }

def check_follow_status(follower_id, followed_id, cors_headers):
    """
    Vérifie si un utilisateur suit un autre utilisateur
    """
    try:
        # Vérifier si l'utilisateur authentifié suit l'utilisateur cible
        follow_id = f"{follower_id}#{followed_id}"
        follow_response = follows_table.get_item(Key={'follow_id': follow_id})
        is_following = 'Item' in follow_response
        
        # Vérifier si l'utilisateur cible suit l'utilisateur authentifié
        reverse_follow_id = f"{followed_id}#{follower_id}"
        reverse_follow_response = follows_table.get_item(Key={'follow_id': reverse_follow_id})
        is_followed_by = 'Item' in reverse_follow_response
        
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
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error checking follow status: {str(e)}'})
        }

def get_follow_counts(user_id, cors_headers):
    """
    Récupère le nombre de followers et de following pour un utilisateur
    """
    try:
        # Compter les followers
        followers_response = follows_table.query(
            IndexName='followed_id-index',
            KeyConditionExpression=Key('followed_id').eq(user_id),
            Select='COUNT'
        )
        followers_count = followers_response.get('Count', 0)
        
        # Compter les following
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
            }, cls=DecimalEncoder)
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des compteurs: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving follow counts: {str(e)}'})
        }

def add_follow(follower_id, followed_id, cors_headers):
    """
    Ajoute une relation de follow entre deux utilisateurs
    """
    try:
        # Vérifier que l'utilisateur ne se suit pas lui-même
        if follower_id == followed_id:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Cannot follow yourself'})
            }
        
        # Vérifier que l'utilisateur à suivre existe
        user_response = users_table.get_item(Key={'userId': followed_id})
        if 'Item' not in user_response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'User to follow not found'})
            }
        
        # Créer un ID unique pour la relation de follow
        follow_id = f"{follower_id}#{followed_id}"
        timestamp = int(time.time())
        
        # Vérifier si le follow existe déjà
        follow_response = follows_table.get_item(Key={'follow_id': follow_id})
        if 'Item' in follow_response:
            return {
                'statusCode': 409,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Already following this user'})
            }
        
        # Ajouter le follow
        follows_table.put_item(Item={
            'follow_id': follow_id,
            'follower_id': follower_id,
            'followed_id': followed_id,
            'created_at': timestamp
        })
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'message': 'Follow added successfully',
                'follow_id': follow_id,
                'follower_id': follower_id,
                'followed_id': followed_id,
                'created_at': timestamp
            })
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de l'ajout du follow: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error adding follow: {str(e)}'})
        }

def remove_follow(follower_id, followed_id, cors_headers):
    """
    Supprime une relation de follow entre deux utilisateurs
    """
    try:
        # Créer l'ID du follow
        follow_id = f"{follower_id}#{followed_id}"
        
        # Vérifier si le follow existe
        follow_response = follows_table.get_item(Key={'follow_id': follow_id})
        if 'Item' not in follow_response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Follow relationship not found'})
            }
        
        # Supprimer le follow
        follows_table.delete_item(Key={'follow_id': follow_id})
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'message': 'Follow removed successfully',
                'follower_id': follower_id,
                'followed_id': followed_id
            })
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la suppression du follow: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error removing follow: {str(e)}'})
        }