import json
import boto3
import logging
import os
from decimal import Decimal
from boto3.dynamodb.conditions import Key

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Variables d'environnement
PLAYLISTS_TABLE = os.environ.get('PLAYLISTS_TABLE', 'chordora-playlists')
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')
S3_BUCKET = os.environ.get('S3_BUCKET', 'chordora-users')

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')
playlists_table = dynamodb.Table(PLAYLISTS_TABLE)
tracks_table = dynamodb.Table(TRACKS_TABLE)
s3 = boto3.client('s3')

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def get_cors_headers(event):
    """Renvoie les en-têtes CORS en fonction de l'origine de la requête"""
    origin = None
    if 'headers' in event and event['headers']:
        origin = event['headers'].get('origin') or event['headers'].get('Origin')
    
    allowed_origin = origin if origin else 'http://localhost:3000'
    
    return {
        'Access-Control-Allow-Origin': allowed_origin,
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

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
        # Extraction de l'ID utilisateur authentifié (si présent)
        auth_user_id = None
        if 'requestContext' in event and 'authorizer' in event['requestContext'] and 'claims' in event['requestContext']['authorizer']:
            auth_user_id = event['requestContext']['authorizer']['claims']['sub']
            logger.info(f"Utilisateur authentifié: {auth_user_id}")
        
        # Récupérer les paramètres de requête et de chemin
        query_params = event.get('queryStringParameters', {}) or {}
        path_params = event.get('pathParameters', {}) or {}
        
        # CAS 1: Une playlist spécifique
        if 'playlistId' in path_params:
            playlist_id = path_params['playlistId']
            logger.info(f"Récupération de la playlist: {playlist_id}")
            return get_playlist_by_id(playlist_id, auth_user_id, cors_headers)
        
        # CAS 2: Les playlists d'un utilisateur spécifique (ou de l'utilisateur authentifié)
        user_id = query_params.get('userId') or auth_user_id
        
        if not user_id:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'User ID is required either as userId query parameter or from authentication'})
            }
        
        logger.info(f"Récupération des playlists de l'utilisateur: {user_id}")
        return get_user_playlists(user_id, auth_user_id, cors_headers, query_params)
    
    except Exception as e:
        logger.error(f"Erreur non gérée: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }

def get_playlist_by_id(playlist_id, auth_user_id, cors_headers):
    """Récupère une playlist spécifique par son ID"""
    try:
        # Récupérer les informations de la playlist
        response = playlists_table.get_item(Key={'playlist_id': playlist_id})
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Playlist not found'})
            }
        
        playlist = response['Item']
        
        # Vérifier si la playlist est privée et n'appartient pas à l'utilisateur authentifié
        if not playlist.get('is_public', True) and playlist['user_id'] != auth_user_id:
            return {
                'statusCode': 403,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Access denied to private playlist'})
            }
        
        # Récupérer les pistes de la playlist
        if 'track_ids' in playlist and playlist['track_ids']:
            playlist['tracks'] = get_tracks_by_ids(playlist['track_ids'], playlist.get('track_positions', {}))
        else:
            playlist['tracks'] = []
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(playlist, cls=DecimalEncoder)
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la récupération de la playlist {playlist_id}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving playlist: {str(e)}'})
        }

def get_user_playlists(user_id, auth_user_id, cors_headers, query_params):
    """Récupère toutes les playlists d'un utilisateur"""
    try:
        # Requête pour les playlists de l'utilisateur
        response = playlists_table.query(
            IndexName='user_id-index',
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
        
        playlists = response.get('Items', [])
        
        # Filtrer les playlists privées si l'utilisateur n'est pas le propriétaire
        if user_id != auth_user_id:
            playlists = [p for p in playlists if p.get('is_public', True)]
        
        # Déterminer si on doit inclure les pistes dans la réponse
        include_tracks = 'includeTracks' in query_params and query_params['includeTracks'].lower() == 'true'
        
        if include_tracks:
            for playlist in playlists:
                if 'track_ids' in playlist and playlist['track_ids']:
                    playlist['tracks'] = get_tracks_by_ids(playlist['track_ids'], playlist.get('track_positions', {}))
                else:
                    playlist['tracks'] = []
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'playlists': playlists,
                'count': len(playlists)
            }, cls=DecimalEncoder)
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des playlists de l'utilisateur {user_id}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving user playlists: {str(e)}'})
        }

def get_tracks_by_ids(track_ids, track_positions):
    """Récupère plusieurs pistes par leurs IDs et ajoute leurs positions"""
    try:
        if not track_ids:
            return []
        
        tracks_with_details = []
        
        # BatchGetItem est limité à 100 éléments, donc on divise en chunks si nécessaire
        chunk_size = 100
        for i in range(0, len(track_ids), chunk_size):
            chunk_ids = track_ids[i:i + chunk_size]
            keys = [{'track_id': id} for id in chunk_ids]
            
            batch_response = dynamodb.batch_get_item(
                RequestItems={
                    TRACKS_TABLE: {
                        'Keys': keys
                    }
                }
            )
            
            # Récupérer les pistes retournées
            if TRACKS_TABLE in batch_response.get('Responses', {}):
                tracks_batch = batch_response['Responses'][TRACKS_TABLE]
                
                for track in tracks_batch:
                    # Ajouter la position depuis track_positions
                    track_id = track['track_id']
                    track['position'] = track_positions.get(track_id, 0)
                    
                    # Générer des URLs présignées pour l'audio
                    if 'file_path' in track:
                        try:
                            presigned_url = s3.generate_presigned_url(
                                'get_object',
                                Params={'Bucket': S3_BUCKET, 'Key': track['file_path']},
                                ExpiresIn=3600
                            )
                            track['presigned_url'] = presigned_url
                        except Exception as e:
                            logger.error(f"Erreur lors de la génération de l'URL présignée pour {track['track_id']}: {str(e)}")
                    
                    # Générer des URLs présignées pour les images de couverture
                    if 'cover_image_path' in track:
                        try:
                            cover_url = s3.generate_presigned_url(
                                'get_object',
                                Params={'Bucket': S3_BUCKET, 'Key': track['cover_image_path']},
                                ExpiresIn=3600
                            )
                            track['cover_image'] = cover_url
                        except Exception as e:
                            logger.error(f"Erreur lors de la génération de l'URL de couverture pour {track['track_id']}: {str(e)}")
                    
                    tracks_with_details.append(track)
        
        # Trier les pistes en fonction de l'ordre dans track_ids
        sorted_tracks = []
        for track_id in track_ids:
            track = next((t for t in tracks_with_details if t['track_id'] == track_id), None)
            if track:
                sorted_tracks.append(track)
        
        return sorted_tracks
    
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des pistes: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return []