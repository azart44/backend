import json
import boto3
import logging
import os
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Variables d'environnement
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')
S3_BUCKET = os.environ.get('S3_BUCKET', 'chordora-users')

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')
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
        # Extraction de l'ID utilisateur authentifié
        auth_user_id = None
        query_params = event.get('queryStringParameters', {}) or {}
        
        # Récupérer l'ID utilisateur à partir des claims ou des paramètres de requête
        if 'requestContext' in event and 'authorizer' in event['requestContext'] and 'claims' in event['requestContext']['authorizer']:
            auth_user_id = event['requestContext']['authorizer']['claims']['sub']
        
        # Récupérer l'ID utilisateur des paramètres de requête (priorité sur l'ID authentifié)
        user_id = query_params.get('userId', auth_user_id)
        
        if not user_id:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'User ID is required'})
            }
        
        # Filtres supplémentaires (optionnels)
        genre = query_params.get('genre')
        search_term = query_params.get('query')
        liked_by = query_params.get('likedBy')
        track_ids = query_params.get('ids')
        recent = query_params.get('recent') == 'true'
        
        # Construire l'expression de filtre
        filter_expression = None
        expression_attribute_names = {}
        expression_attribute_values = {}
        
        # Filtres de base sur l'utilisateur et la visibilité
        base_filter = Attr('user_id').eq(user_id)
        
        # Vérifier si l'utilisateur consulte ses propres pistes ou celles d'un autre
        if user_id != auth_user_id:
            # Pour un autre utilisateur, n'afficher que les pistes publiques
            base_filter &= Attr('isPrivate').ne(True)
        
        # Application des filtres supplémentaires
        if genre:
            base_filter &= Attr('genre').eq(genre)
        
        if search_term:
            # Recherche insensible à la casse dans le titre, l'artiste, etc.
            base_filter &= (
                Attr('title').contains(search_term) | 
                Attr('artist').contains(search_term) |
                Attr('genre').contains(search_term)
            )
        
        # Gestion de la recherche par IDs spécifiques
        if track_ids:
            track_id_list = track_ids.split(',')
            return get_tracks_by_ids(track_id_list, auth_user_id, cors_headers)
        
        # Gestion du filtre "likes"
        if liked_by == 'current' and auth_user_id:
            # Récupérer les IDs des pistes likées
            liked_tracks_response = tracks_table.query(
                IndexName='liked_by_index',
                KeyConditionExpression=Key('liked_by').eq(auth_user_id)
            )
            
            # Si aucune piste likée, retourner une liste vide
            if not liked_tracks_response.get('Items'):
                return {
                    'statusCode': 200,
                    'headers': cors_headers,
                    'body': json.dumps({
                        'tracks': [],
                        'count': 0
                    })
                }
            
            # Extraire les IDs des pistes likées
            liked_track_ids = [track['track_id'] for track in liked_tracks_response.get('Items', [])]
            
            # Filtrer par les pistes likées
            base_filter &= Attr('track_id').is_in(liked_track_ids)
        elif liked_by and liked_by != 'current':
            # Filtrer les pistes likées par un utilisateur spécifique
            liked_tracks_response = tracks_table.query(
                IndexName='liked_by_index',
                KeyConditionExpression=Key('liked_by').eq(liked_by)
            )
            
            # Si aucune piste likée, retourner une liste vide
            if not liked_tracks_response.get('Items'):
                return {
                    'statusCode': 200,
                    'headers': cors_headers,
                    'body': json.dumps({
                        'tracks': [],
                        'count': 0
                    })
                }
            
            # Extraire les IDs des pistes likées
            liked_track_ids = [track['track_id'] for track in liked_tracks_response.get('Items', [])]
            
            # Filtrer par les pistes likées
            base_filter &= Attr('track_id').is_in(liked_track_ids)
        
        # Préparation de la requête de scan avec filtres
        scan_kwargs = {
            'FilterExpression': base_filter,
            'Select': 'ALL_ATTRIBUTES'
        }
        
        # Limiter à X pistes récentes si demandé
        if recent:
            scan_kwargs.update({
                'Limit': 6,
                'ScanIndexForward': False
            })
        
        # Exécuter la requête
        response = tracks_table.scan(**scan_kwargs)
        
        # Récupérer les pistes et générer des URLs présignées
        tracks = response.get('Items', [])
        
        # Générer des URLs présignées pour chaque piste
        for track in tracks:
            # Générer URL présignée pour le fichier audio
            if 'file_path' in track:
                try:
                    presigned_url = s3.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': S3_BUCKET, 'Key': track['file_path']},
                        ExpiresIn=3600
                    )
                    track['presigned_url'] = presigned_url
                except Exception as e:
                    logger.error(f"Erreur lors de la génération de l'URL audio: {str(e)}")
            
            # Générer URL présignée pour l'image de couverture
            if 'cover_image_path' in track:
                try:
                    cover_url = s3.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': S3_BUCKET, 'Key': track['cover_image_path']},
                        ExpiresIn=3600
                    )
                    track['cover_image'] = cover_url
                except Exception as e:
                    logger.error(f"Erreur lors de la génération de l'URL de couverture: {str(e)}")
            
            # Calculer le nombre de likes pour chaque piste
            try:
                likes_response = tracks_table.query(
                    IndexName='track_id-index',
                    KeyConditionExpression=Key('track_id').eq(track['track_id'])
                )
                track['likes'] = len(likes_response.get('Items', []))
            except Exception as e:
                logger.error(f"Erreur lors du calcul des likes: {str(e)}")
                track['likes'] = 0
        
        # Retourner les pistes
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'tracks': tracks,
                'count': len(tracks)
            }, cls=DecimalEncoder)
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des pistes: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'message': f'Erreur lors de la récupération des pistes: {str(e)}'
            })
        }

def get_tracks_by_ids(track_ids, auth_user_id, cors_headers):
    """Récupère plusieurs pistes par leurs IDs avec vérification de visibilité"""
    try:
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
                    # Vérifier si la piste est privée
                    is_private = track.get('isPrivate', False)
                    
                    # Ne pas afficher les pistes privées si l'utilisateur n'est pas le propriétaire
                    if is_private and track.get('user_id') != auth_user_id:
                        continue
                    
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
                            logger.error(f"Erreur URL audio: {str(e)}")
                    
                    # Générer des URLs présignées pour la couverture
                    if 'cover_image_path' in track:
                        try:
                            cover_url = s3.generate_presigned_url(
                                'get_object',
                                Params={'Bucket': S3_BUCKET, 'Key': track['cover_image_path']},
                                ExpiresIn=3600
                            )
                            track['cover_image'] = cover_url
                        except Exception as e:
                            logger.error(f"Erreur URL couverture: {str(e)}")
                    
                    # Calculer les likes
                    try:
                        likes_response = tracks_table.query(
                            IndexName='track_id-index',
                            KeyConditionExpression=Key('track_id').eq(track['track_id'])
                        )
                        track['likes'] = len(likes_response.get('Items', []))
                    except Exception as e:
                        logger.error(f"Erreur likes: {str(e)}")
                        track['likes'] = 0
                    
                    tracks_with_details.append(track)
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'tracks': tracks_with_details,
                'count': len(tracks_with_details)
            }, cls=DecimalEncoder)
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des pistes par ID: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'message': f'Erreur lors de la récupération des pistes: {str(e)}'
            })
        }