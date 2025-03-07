import json
import boto3
import logging
import os
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr
import base64
from datetime import datetime, timedelta

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Variables d'environnement
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')
S3_BUCKET = os.environ.get('S3_BUCKET', 'chordora-user-uploads')
CLOUDFRONT_URL = os.environ.get('CLOUDFRONT_URL', '')
PRESIGNED_URL_EXPIRATION = int(os.environ.get('PRESIGNED_URL_EXPIRATION', '3600'))

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')
tracks_table = dynamodb.Table(TRACKS_TABLE)
s3 = boto3.client('s3')

class DecimalEncoder(json.JSONEncoder):
    """Encodeur JSON personnalisé pour gérer les objets Decimal de DynamoDB"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def get_cors_headers(event):
    """Renvoie les en-têtes CORS en fonction de l'origine de la requête"""
    origin = None
    if 'headers' in event and event['headers']:
        origin = event['headers'].get('origin') or event['headers'].get('Origin')
    
    allowed_origin = origin if origin else '*'
    
    return {
        'Access-Control-Allow-Origin': allowed_origin,
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,OPTIONS,POST',
        'Access-Control-Allow-Credentials': 'true'
    }

def generate_presigned_url(object_key, expiration=PRESIGNED_URL_EXPIRATION):
    """Génère une URL présignée pour un objet S3"""
    try:
        if not object_key:
            return None
            
        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET, 'Key': object_key},
            ExpiresIn=expiration
        )
        return url
    except Exception as e:
        logger.error(f"Erreur lors de la génération de l'URL présignée pour {object_key}: {str(e)}")
        return None

def lambda_handler(event, context):
    """Gestionnaire principal de la Lambda"""
    logger.info(f"Événement reçu: {json.dumps(event)}")
    cors_headers = get_cors_headers(event)
    
    # Gestion des requêtes OPTIONS (pre-flight CORS)
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('Preflight request successful')
        }
    
    try:
        # Vérification des paramètres de requête 
        query_params = event.get('queryStringParameters', {}) or {}
        path_params = event.get('pathParameters', {}) or {}
        
        # Extraction de l'ID utilisateur authentifié
        auth_user_id = None
        if 'requestContext' in event and 'authorizer' in event['requestContext'] and 'claims' in event['requestContext']['authorizer']:
            auth_user_id = event['requestContext']['authorizer']['claims']['sub']
            logger.info(f"Utilisateur authentifié: {auth_user_id}")
        
        # Cas 1: Une piste spécifique par ID
        if 'trackId' in path_params:
            track_id = path_params['trackId']
            return get_track_by_id(track_id, auth_user_id, cors_headers)
        
        # Cas 2: Liste de pistes (avec différents filtres possibles)
        return get_tracks(query_params, auth_user_id, cors_headers)
        
    except Exception as e:
        logger.error(f"Erreur lors du traitement de la requête: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }

def get_track_by_id(track_id, auth_user_id, cors_headers):
    """Récupère une piste spécifique par son ID"""
    try:
        # Récupérer la piste depuis DynamoDB
        response = tracks_table.get_item(Key={'track_id': track_id})
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Track not found'})
            }
        
        track = response['Item']
        
        # Vérifier si la piste est privée et n'appartient pas à l'utilisateur authentifié
        if track.get('isPrivate', False) and track.get('user_id') != auth_user_id:
            return {
                'statusCode': 403,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Access denied to private track'})
            }
        
        # Générer des URLs présignées pour l'audio et l'image de couverture
        if 'file_path' in track:
            track['presigned_url'] = generate_presigned_url(track['file_path'])
        
        if 'cover_image_path' in track:
            track['cover_image'] = generate_presigned_url(track['cover_image_path'])
        
        # Vérifier si l'utilisateur a liké cette piste
        if auth_user_id:
            try:
                # Table des likes (supposée exister)
                likes_table = dynamodb.Table('chordora-track-likes')
                like_response = likes_table.get_item(
                    Key={
                        'user_id': auth_user_id,
                        'track_id': track_id
                    }
                )
                track['isLiked'] = 'Item' in like_response
            except Exception as e:
                logger.error(f"Erreur lors de la vérification du like: {str(e)}")
                track['isLiked'] = False
        
        # Incrémenter le compteur de vues
        try:
            tracks_table.update_item(
                Key={'track_id': track_id},
                UpdateExpression="SET plays = if_not_exists(plays, :start) + :inc",
                ExpressionAttributeValues={
                    ':start': 0,
                    ':inc': 1
                }
            )
        except Exception as e:
            logger.error(f"Erreur lors de l'incrémentation du compteur de vues: {str(e)}")
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(track, cls=DecimalEncoder)
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la récupération de la piste {track_id}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving track: {str(e)}'})
        }

def get_tracks(query_params, auth_user_id, cors_headers):
    """Récupère une liste de pistes en fonction des paramètres de requête"""
    try:
        # Paramètres de filtrage
        user_id = query_params.get('userId')
        genre = query_params.get('genre')
        mood = query_params.get('mood')
        bpm_min = query_params.get('bpmMin')
        bpm_max = query_params.get('bpmMax')
        search_query = query_params.get('query')
        liked_by = query_params.get('likedBy')
        limit = int(query_params.get('limit', '50'))
        track_ids = query_params.get('ids')
        
        # Construction des expressions de filtre
        filter_expression = None
        expression_values = {}
        
        # Filtrer par genre
        if genre:
            filter_expression = Attr('genre').eq(genre)
            expression_values[':genre'] = genre
        
        # Filtrer par mood
        if mood:
            mood_filter = Attr('mood').eq(mood)
            filter_expression = mood_filter if not filter_expression else filter_expression & mood_filter
            expression_values[':mood'] = mood
        
        # Filtrer par plage de BPM
        if bpm_min:
            bpm_min = int(bpm_min)
            bpm_min_filter = Attr('bpm').gte(bpm_min)
            filter_expression = bpm_min_filter if not filter_expression else filter_expression & bpm_min_filter
            expression_values[':bpm_min'] = bpm_min
        
        if bpm_max:
            bpm_max = int(bpm_max)
            bpm_max_filter = Attr('bpm').lte(bpm_max)
            filter_expression = bpm_max_filter if not filter_expression else filter_expression & bpm_max_filter
            expression_values[':bpm_max'] = bpm_max
        
        # Filtrer par recherche textuelle
        if search_query:
            # Recherche dans le titre, la description et les tags
            query_filter = (
                Attr('title').contains(search_query) | 
                Attr('description').contains(search_query) | 
                Attr('tags').contains(search_query)
            )
            filter_expression = query_filter if not filter_expression else filter_expression & query_filter
            expression_values[':query'] = search_query
        
        # Exécuter la requête appropriée en fonction des paramètres
        tracks = []
        
        # Cas 1: Récupérer des pistes spécifiques par leurs IDs
        if track_ids:
            track_id_list = track_ids.split(',')
            
            # Utiliser BatchGetItem pour récupérer plusieurs pistes à la fois
            # BatchGetItem est limité à 100 éléments, donc nous devons paginer si nécessaire
            chunk_size = 100
            for i in range(0, len(track_id_list), chunk_size):
                chunk = track_id_list[i:i+chunk_size]
                response = dynamodb.batch_get_item(
                    RequestItems={
                        TRACKS_TABLE: {
                            'Keys': [{'track_id': track_id} for track_id in chunk]
                        }
                    }
                )
                
                if TRACKS_TABLE in response.get('Responses', {}):
                    tracks.extend(response['Responses'][TRACKS_TABLE])
        
        # Cas 2: Récupérer les pistes likées par un utilisateur
        elif liked_by:
            target_user_id = liked_by
            if liked_by == 'current':
                target_user_id = auth_user_id
                
            if not target_user_id:
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'message': 'User ID is required for liked tracks'})
                }
                
            # Récupérer les likes de l'utilisateur
            likes_table = dynamodb.Table('chordora-track-likes')
            like_response = likes_table.query(
                KeyConditionExpression=Key('user_id').eq(target_user_id)
            )
            
            liked_track_ids = [item['track_id'] for item in like_response.get('Items', [])]
            
            # Récupérer les pistes correspondantes
            if liked_track_ids:
                # Utiliser BatchGetItem comme ci-dessus
                chunk_size = 100
                for i in range(0, len(liked_track_ids), chunk_size):
                    chunk = liked_track_ids[i:i+chunk_size]
                    response = dynamodb.batch_get_item(
                        RequestItems={
                            TRACKS_TABLE: {
                                'Keys': [{'track_id': track_id} for track_id in chunk]
                            }
                        }
                    )
                    
                    if TRACKS_TABLE in response.get('Responses', {}):
                        tracks.extend(response['Responses'][TRACKS_TABLE])
        
        # Cas 3: Récupérer les pistes d'un utilisateur spécifique
        elif user_id:
            response = tracks_table.query(
                IndexName='user_id-index',
                KeyConditionExpression=Key('user_id').eq(user_id),
                FilterExpression=filter_expression,
                ExpressionAttributeValues=expression_values if expression_values else None
            )
            tracks = response.get('Items', [])
        
        # Cas 4: Recherche générale (avec possibilité de filtrage)
        else:
            # Limiter le nombre de résultats
            response = tracks_table.scan(
                FilterExpression=filter_expression,
                ExpressionAttributeValues=expression_values if expression_values else None,
                Limit=limit
            )
            tracks = response.get('Items', [])
        
        # Filtrer les pistes privées si l'utilisateur n'est pas le propriétaire
        filtered_tracks = []
        for track in tracks:
            # Ne pas inclure les pistes privées sauf si elles appartiennent à l'utilisateur authentifié
            if track.get('isPrivate', False) and track.get('user_id') != auth_user_id:
                continue
                
            # Générer des URLs présignées pour l'audio et l'image de couverture
            if 'file_path' in track:
                track['presigned_url'] = generate_presigned_url(track['file_path'])
            
            if 'cover_image_path' in track:
                track['cover_image'] = generate_presigned_url(track['cover_image_path'])
                
            # Si l'utilisateur est authentifié, vérifier s'il a liké chaque piste
            if auth_user_id:
                try:
                    likes_table = dynamodb.Table('chordora-track-likes')
                    like_response = likes_table.get_item(
                        Key={
                            'user_id': auth_user_id,
                            'track_id': track['track_id']
                        }
                    )
                    track['isLiked'] = 'Item' in like_response
                except Exception as e:
                    logger.error(f"Erreur lors de la vérification du like: {str(e)}")
                    track['isLiked'] = False
                    
            filtered_tracks.append(track)
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'tracks': filtered_tracks,
                'count': len(filtered_tracks)
            }, cls=DecimalEncoder)
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des pistes: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving tracks: {str(e)}'})
        }