import json
import boto3
import logging
from decimal import Decimal
import os
import traceback
from boto3.dynamodb.conditions import Key, Attr

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Variables d'environnement
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')
LIKES_TABLE = os.environ.get('LIKES_TABLE', 'chordora-track-likes')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'chordora-users')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')
tracks_table = dynamodb.Table(TRACKS_TABLE)
likes_table = dynamodb.Table(LIKES_TABLE)
s3 = boto3.client('s3')

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def get_cors_headers(event):
    """Renvoie les en-têtes CORS adaptés à l'origine de la requête"""
    origin = None
    if 'headers' in event and event['headers']:
        origin = event['headers'].get('origin') or event['headers'].get('Origin')
    
    allowed_origin = origin if origin else 'http://localhost:3000'
    
    return {
        'Access-Control-Allow-Origin': allowed_origin,
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

def generate_presigned_urls(tracks, auth_user_id=None):
    """
    Génère des URLs présignées pour les pistes audio et les images de couverture
    Vérifie aussi si l'utilisateur authentifié a liké chaque piste
    """
    tracks_with_urls = []
    
    for track in tracks:
        try:
            track_with_url = dict(track)  # Créer une copie pour éviter de modifier l'original
            
            # Générer URL présignée pour le fichier audio
            if 'file_path' in track:
                try:
                    # Vérifier si l'objet existe dans S3
                    try:
                        s3.head_object(Bucket=BUCKET_NAME, Key=track['file_path'])
                        
                        # Générer l'URL présignée avec une durée de validité plus longue et des paramètres améliorés
                        presigned_url = s3.generate_presigned_url(
                            'get_object',
                            Params={
                                'Bucket': BUCKET_NAME, 
                                'Key': track['file_path'],
                                'ResponseContentType': 'audio/mpeg',  # Forcer le type MIME correct
                                'ResponseContentDisposition': 'inline'  # Encourage la lecture en ligne
                            },
                            ExpiresIn=86400  # URL valide 24 heures pour éviter les problèmes de rafraîchissement
                        )
                        
                        # Vérifier que l'URL n'est pas vide
                        if not presigned_url:
                            logger.error(f"URL présignée générée vide pour la piste {track.get('track_id')}")
                            raise Exception("URL présignée vide générée")
                            
                        logger.info(f"URL présignée générée pour la piste {track.get('track_id')}: {presigned_url[:50]}...")
                        track_with_url['presigned_url'] = presigned_url
                        
                        # Ajouter le format et la taille comme métadonnées
                        try:
                            response = s3.head_object(Bucket=BUCKET_NAME, Key=track['file_path'])
                            if 'ContentLength' in response:
                                track_with_url['file_size'] = response['ContentLength']
                            if 'ContentType' in response:
                                track_with_url['file_type'] = response['ContentType']
                        except Exception as meta_error:
                            logger.warning(f"Impossible de récupérer les métadonnées du fichier: {str(meta_error)}")
                            
                    except s3.exceptions.ClientError as e:
                        # Si le fichier n'existe pas, on le journalise clairement
                        if e.response['Error']['Code'] == '404':
                            logger.error(f"Le fichier audio n'existe pas dans S3: {track['file_path']}")
                            track_with_url['error'] = "Le fichier audio n'existe pas"
                            track_with_url['file_missing'] = True
                        else:
                            logger.error(f"Erreur S3 lors de la vérification du fichier {track['file_path']}: {str(e)}")
                            track_with_url['error'] = f"Erreur S3: {e.response['Error']['Code']}"
                except Exception as e:
                    logger.error(f"Erreur lors de la génération de l'URL audio pour {track.get('track_id')}: {str(e)}")
                    logger.error(traceback.format_exc())
                    track_with_url['error'] = 'Could not generate audio URL'
            
            # Générer URL présignée pour l'image de couverture si elle existe
            if 'cover_image_path' in track and track['cover_image_path']:
                try:
                    # Vérifier si le fichier existe avant de générer l'URL
                    try:
                        s3.head_object(Bucket=BUCKET_NAME, Key=track['cover_image_path'])
                        
                        cover_url = s3.generate_presigned_url(
                            'get_object',
                            Params={
                                'Bucket': BUCKET_NAME, 
                                'Key': track['cover_image_path'],
                                'ResponseContentType': 'image/jpeg',  # Forcer le type MIME
                                'ResponseContentDisposition': 'inline'  # Pour affichage direct
                            },
                            ExpiresIn=86400  # URL valide 24 heures
                        )
                        track_with_url['cover_image'] = cover_url
                        logger.info(f"URL de couverture générée pour la piste {track.get('track_id')}")
                    except s3.exceptions.ClientError as e:
                        if e.response['Error']['Code'] == '404':
                            logger.error(f"L'image de couverture n'existe pas dans S3: {track['cover_image_path']}")
                            # Utiliser une image par défaut
                            track_with_url['cover_image'] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/public/default-cover.jpg"
                        else:
                            logger.error(f"Erreur S3 lors de la vérification de l'image {track['cover_image_path']}: {str(e)}")
                except Exception as e:
                    logger.error(f"Erreur lors de la génération de l'URL de couverture pour {track.get('track_id')}: {str(e)}")
                    logger.error(traceback.format_exc())
            else:
                # Si pas d'image de couverture, utiliser une image par défaut
                track_with_url['cover_image'] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/public/default-cover.jpg"
            
            # Vérifier si l'utilisateur authentifié a liké cette piste
            if auth_user_id:
                try:
                    like_id = f"{auth_user_id}#{track['track_id']}"
                    like_response = likes_table.get_item(Key={'like_id': like_id})
                    track_with_url['isLiked'] = 'Item' in like_response
                except Exception as e:
                    logger.error(f"Erreur lors de la vérification du like: {str(e)}")
                    track_with_url['isLiked'] = False
            
            tracks_with_urls.append(track_with_url)
        except Exception as track_error:
            logger.error(f"Erreur lors du traitement de la piste: {str(track_error)}")
            logger.error(traceback.format_exc())
            # Ajouter quand même la piste avec une erreur
            if 'track_id' in track:
                tracks_with_urls.append({
                    'track_id': track['track_id'],
                    'title': track.get('title', 'Piste inconnue'),
                    'error': f"Erreur de traitement: {str(track_error)}",
                    'user_id': track.get('user_id', '')
                })
    
    return tracks_with_urls

# Dans la fonction get_track_by_id, ajoutez cette vérification après avoir généré les URLs présignées:

# Si une erreur "file_missing" est détectée, renvoyer une erreur 404
if track_with_url.get('file_missing'):
    return {
        'statusCode': 404,
        'headers': cors_headers,
        'body': json.dumps({'message': 'Track file not found'})
    }

# Journaliser l'URL générée pour debug
logger.info(f"URL fournie pour le frontend: {track_with_url.get('presigned_url', '')[:50]}...")

def lambda_handler(event, context):
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
        if 'requestContext' in event and 'authorizer' in event['requestContext'] and 'claims' in event['requestContext']['authorizer']:
            auth_user_id = event['requestContext']['authorizer']['claims']['sub']
            logger.info(f"Utilisateur authentifié: {auth_user_id}")
        
        # Récupérer les paramètres de requête
        query_params = event.get('queryStringParameters', {}) or {}
        path_params = event.get('pathParameters', {}) or {}
        
        # CAS 1: Piste spécifique par ID (détail d'une piste)
        if 'trackId' in path_params:
            track_id = path_params['trackId']
            logger.info(f"Récupération de la piste spécifique par ID: {track_id}")
            return get_track_by_id(track_id, auth_user_id, cors_headers)
        
        # CAS 2: Pistes likées par l'utilisateur (page favoris)
        if 'likedBy' in query_params:
            liked_by = query_params['likedBy']
            
            # Si 'current' est passé, utiliser l'ID de l'utilisateur authentifié
            if liked_by == 'current':
                if not auth_user_id:
                    return {
                        'statusCode': 401,
                        'headers': cors_headers,
                        'body': json.dumps({'message': 'Authentication required to view your liked tracks'})
                    }
                liked_by = auth_user_id
                
            logger.info(f"Récupération des pistes likées par: {liked_by}")
            return get_liked_tracks(liked_by, auth_user_id, cors_headers)
        
        # CAS 3: Pistes d'un utilisateur spécifique (page profil)
        if 'userId' in query_params:
            target_user_id = query_params['userId']
            logger.info(f"Récupération des pistes de l'utilisateur: {target_user_id}")
            return get_user_tracks(target_user_id, auth_user_id, query_params, cors_headers)
        
        # CAS 4: Multiple pistes par leurs IDs
        if 'ids' in query_params and query_params['ids']:
            track_ids = query_params['ids'].split(',')
            logger.info(f"Récupération de plusieurs pistes par IDs: {track_ids}")
            return get_tracks_by_ids(track_ids, auth_user_id, cors_headers)
        
        # CAS 5: Si aucun paramètre spécifique n'est fourni, utiliser l'ID authentifié comme userId (ma page profil)
        if auth_user_id:
            logger.info(f"Récupération des pistes de l'utilisateur authentifié: {auth_user_id}")
            return get_user_tracks(auth_user_id, auth_user_id, query_params, cors_headers)
        
        # Si aucun des cas ci-dessus n'est applicable, renvoyer une erreur
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'message': 'Invalid request parameters. Specify trackId, userId, or likedBy.'})
        }
        
    except Exception as e:
        logger.error(f"Erreur non gérée: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Internal server error: {str(e)}'}, cls=DecimalEncoder)
        }

def get_track_by_id(track_id, auth_user_id, cors_headers):
    """Récupère une piste spécifique par son ID"""
    try:
        # Récupérer la piste
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
        
        # Générer les URLs présignées et vérifier les likes
        tracks_with_urls = generate_presigned_urls([track], auth_user_id)
        track_with_url = tracks_with_urls[0] if tracks_with_urls else track
        
        # Si une erreur "file_missing" est détectée, renvoyer une erreur 404
        if track_with_url.get('file_missing'):
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Track file not found'})
            }
        
        # Journaliser l'URL générée pour debug
        logger.info(f"URL fournie pour le frontend: {track_with_url.get('presigned_url', '')[:50]}...")
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(track_with_url, cls=DecimalEncoder)
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la récupération de la piste {track_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving track: {str(e)}'})
        }

def get_liked_tracks(user_id, auth_user_id, cors_headers):
    """Récupère toutes les pistes likées par un utilisateur"""
    try:
        # Récupérer les likes de l'utilisateur
        likes_response = likes_table.query(
            IndexName='user_id-index',  # Assurez-vous que cet index existe sur la table des likes
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
        likes = likes_response.get('Items', [])
        
        if not likes:
            # Pas de pistes likées trouvées
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({'tracks': [], 'count': 0}, cls=DecimalEncoder)
            }
        
        # Récupérer les IDs des pistes likées
        track_ids = [like['track_id'] for like in likes]
        
        # Récupérer les pistes en batch
        tracks = []
        
        # BatchGetItem est limité à 100 éléments, donc on divise en chunks si nécessaire
        chunk_size = 100
        for i in range(0, len(track_ids), chunk_size):
            chunk = track_ids[i:i + chunk_size]
            keys = [{'track_id': id} for id in chunk]
            
            response = dynamodb.batch_get_item(
                RequestItems={
                    TRACKS_TABLE: {
                        'Keys': keys
                    }
                }
            )
            
            if TRACKS_TABLE in response.get('Responses', {}):
                batch_tracks = response['Responses'][TRACKS_TABLE]
                tracks.extend(batch_tracks)
        
        # Filtrer les pistes privées si l'utilisateur n'est pas le propriétaire
        if user_id != auth_user_id:
            tracks = [track for track in tracks if not track.get('isPrivate', False)]
        
        # Marquer toutes les pistes comme likées (puisqu'elles viennent de la liste des likes)
        for track in tracks:
            track['isLiked'] = True
        
        # Générer les URLs présignées
        tracks_with_urls = generate_presigned_urls(tracks, auth_user_id)
        
        # Filtrer les pistes avec des fichiers manquants
        valid_tracks = [track for track in tracks_with_urls if not track.get('file_missing')]
        
        # Ajouter des informations d'artiste si manquantes
        for track in valid_tracks:
            if 'artist' not in track or not track['artist']:
                # Possibilité de récupérer le nom d'utilisateur à partir de l'ID utilisateur
                track['artist'] = track.get('artist', 'Artiste')
        
        # Trier par date de like (si disponible), sinon par date de création
        valid_tracks.sort(key=lambda x: x.get('created_at', 0), reverse=True)
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({'tracks': valid_tracks, 'count': len(valid_tracks)}, cls=DecimalEncoder)
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des pistes likées: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving liked tracks: {str(e)}'})
        }

def get_tracks_by_ids(track_ids, auth_user_id, cors_headers):
    """Récupère plusieurs pistes par leurs IDs"""
    try:
        if not track_ids:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'No track IDs provided'})
            }
        
        # Limiter à 100 pistes maximum pour éviter les problèmes de performance
        track_ids = track_ids[:100]
        
        # Récupérer les pistes par batch
        tracks = []
        keys = [{'track_id': id} for id in track_ids]
        
        # BatchGetItem est limité à 100 éléments
        response = dynamodb.batch_get_item(
            RequestItems={
                TRACKS_TABLE: {
                    'Keys': keys
                }
            }
        )
        
        if TRACKS_TABLE in response.get('Responses', {}):
            tracks = response['Responses'][TRACKS_TABLE]
        
        # Filtrer les pistes privées si l'utilisateur n'est pas le propriétaire
        if auth_user_id:
            tracks = [track for track in tracks if 
                     not track.get('isPrivate', False) or track.get('user_id') == auth_user_id]
        else:
            tracks = [track for track in tracks if not track.get('isPrivate', False)]
        
        # Générer les URLs présignées
        tracks_with_urls = generate_presigned_urls(tracks, auth_user_id)
        
        # Filtrer les pistes avec des fichiers manquants
        valid_tracks = [track for track in tracks_with_urls if not track.get('file_missing')]
        
        # Préserver l'ordre des pistes tel que demandé dans track_ids
        ordered_tracks = []
        for tid in track_ids:
            for track in valid_tracks:
                if track['track_id'] == tid:
                    ordered_tracks.append(track)
                    break
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({'tracks': ordered_tracks, 'count': len(ordered_tracks)}, cls=DecimalEncoder)
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des pistes par IDs: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving tracks by IDs: {str(e)}'})
        }

def get_user_tracks(user_id, auth_user_id, query_params, cors_headers):
    """Récupère les pistes d'un utilisateur spécifique"""
    try:
        # Paramètres de filtrage supplémentaires (genre, etc.)
        genre = query_params.get('genre')
        
        # Requête pour les pistes de l'utilisateur
        query_params = {
            'IndexName': 'user_id-index',  # Assurez-vous que cet index existe sur la table des tracks
            'KeyConditionExpression': Key('user_id').eq(user_id)
        }
        
        # Ajouter un filtre par genre si spécifié
        if genre:
            query_params['FilterExpression'] = Attr('genre').eq(genre)
        
        # Si l'utilisateur n'est pas le propriétaire, exclure les pistes privées
        if user_id != auth_user_id:
            if 'FilterExpression' in query_params:
                query_params['FilterExpression'] = query_params['FilterExpression'] & Attr('isPrivate').ne(True)
            else:
                query_params['FilterExpression'] = Attr('isPrivate').ne(True)
        
        # Exécuter la requête
        response = tracks_table.query(**query_params)
        tracks = response.get('Items', [])
        
        # Générer les URLs présignées et vérifier si l'utilisateur a liké les pistes
        tracks_with_urls = generate_presigned_urls(tracks, auth_user_id)
        
        # Filtrer les pistes avec des fichiers manquants
        valid_tracks = [track for track in tracks_with_urls if not track.get('file_missing')]
        
        # Ajout de noms d'artistes
        for track in valid_tracks:
            if 'artist' not in track or not track['artist']:
                # Vous pourriez récupérer le nom d'utilisateur depuis la table des utilisateurs
                track['artist'] = "Artiste"
        
        # Tri par date de création (plus récent en premier)
        valid_tracks.sort(key=lambda x: x.get('created_at', 0), reverse=True)
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({'tracks': valid_tracks, 'count': len(valid_tracks)}, cls=DecimalEncoder)
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des pistes de l'utilisateur {user_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving user tracks: {str(e)}'})
        }