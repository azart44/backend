import json
import boto3
import logging
import traceback
import os
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
from datetime import datetime, timedelta

# IMPORTANT: Cette lambda combine la logique de recommandation de BeatSwipeGetRecommendations
# avec la génération d'URLs présignées de GetTracks pour garantir la compatibilité avec le lecteur audio

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

# Variables d'environnement
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')
USERS_TABLE = os.environ.get('USERS_TABLE', 'chordora-users')
SWIPES_TABLE = os.environ.get('SWIPES_TABLE', 'chordora-beat-swipes')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'chordora-tracks')
DEFAULT_IMAGE_KEY = os.environ.get('DEFAULT_IMAGE_KEY', 'public/default-cover.jpg')
MAX_RECOMMENDATIONS = int(os.environ.get('MAX_RECOMMENDATIONS', '20'))
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# Tables DynamoDB
tracks_table = dynamodb.Table(TRACKS_TABLE)
users_table = dynamodb.Table(USERS_TABLE)
swipes_table = dynamodb.Table(SWIPES_TABLE)

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
    
    allowed_origin = origin if origin else 'https://app.chordora.com'
    
    return {
        'Access-Control-Allow-Origin': allowed_origin,
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

def get_user_profile(user_id):
    """Récupère le profil utilisateur depuis DynamoDB"""
    try:
        response = users_table.get_item(Key={'userId': user_id})
        if 'Item' in response:
            return response['Item']
        return None
    except Exception as e:
        logger.error(f"Erreur lors de la récupération du profil utilisateur {user_id}: {str(e)}")
        return None

def get_audio_duration(bucket, key):
    """
    Tente d'extraire la durée d'un fichier audio en utilisant les métadonnées S3.
    Si ce n'est pas possible, renvoie une durée par défaut.
    """
    default_duration = 180  # 3 minutes par défaut
    
    try:
        # Récupérer les métadonnées du fichier
        response = s3.head_object(Bucket=bucket, Key=key)
        
        # Vérifier si les métadonnées personnalisées contiennent la durée
        if 'Metadata' in response and 'duration' in response['Metadata']:
            try:
                return float(response['Metadata']['duration'])
            except (ValueError, TypeError):
                logger.warning(f"Durée invalide dans les métadonnées pour {key}")
                pass
        
        # Vérifier la taille du fichier pour estimer approximativement la durée
        # MP3 à 128kbps = ~1Mo par minute
        if 'ContentLength' in response:
            file_size_mb = response['ContentLength'] / (1024 * 1024)
            # Estimation très approximative
            estimated_duration = file_size_mb * 60
            if estimated_duration > 0:
                logger.info(f"Durée estimée par la taille pour {key}: {estimated_duration}s")
                return min(estimated_duration, 1800)  # Limiter à 30 minutes max
        
        # Si aucune durée n'est trouvée ou estimée, utiliser une valeur par défaut
        logger.info(f"Utilisation de la durée par défaut pour {key}: {default_duration}s")
        return default_duration
    except Exception as e:
        logger.warning(f"Impossible de déterminer la durée du fichier audio {key}: {str(e)}")
        return default_duration

def generate_presigned_urls(tracks, auth_user_id=None):
    """
    Génère des URLs présignées pour les pistes audio et les images de couverture
    Ajoute également les informations d'artiste et vérifie si l'utilisateur authentifié a liké chaque piste
    """
    tracks_with_urls = []
    
    for track in tracks:
        try:
            track_with_url = dict(track)  # Créer une copie pour éviter de modifier l'original
            
            # Récupérer les informations de l'artiste
            if 'user_id' in track:
                user_profile = get_user_profile(track['user_id'])
                if user_profile and 'username' in user_profile:
                    track_with_url['artist'] = user_profile['username']
                else:
                    track_with_url['artist'] = "Artiste"
            else:
                track_with_url['artist'] = "Artiste"
            
            # Générer URL présignée pour le fichier audio
            if 'file_path' in track:
                try:
                    # Vérifier si l'objet existe dans S3
                    try:
                        s3.head_object(Bucket=BUCKET_NAME, Key=track['file_path'])
                        
                        # Extraire la durée du fichier audio
                        if 'duration' not in track or not track['duration']:
                            track_with_url['duration'] = get_audio_duration(BUCKET_NAME, track['file_path'])
                        
                        # Générer l'URL présignée avec une durée de validité plus longue et des paramètres améliorés
                        presigned_url = s3.generate_presigned_url(
                            'get_object',
                            Params={
                                'Bucket': BUCKET_NAME, 
                                'Key': track['file_path'],
                                'ResponseContentType': 'audio/mpeg',  # Forcer le type MIME correct
                                'ResponseContentDisposition': 'inline'  # Encourage la lecture en ligne
                            },
                            ExpiresIn=86400  # URL valide 24 heures au lieu de 3600s
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

def filter_recommendations(tracks, user_preferences, swiped_track_ids):
    """
    Filtrer et prioriser les recommandations
    """
    # Filtrer les pistes déjà swipées
    unswipped_tracks = [
        track for track in tracks 
        if track['track_id'] not in swiped_track_ids
    ]
    
    # Prioriser selon les préférences
    genre_matched = []
    mood_matched = []
    other_tracks = []
    
    for track in unswipped_tracks:
        genre_match = track.get('genre') in user_preferences.get('genres', [])
        mood_match = track.get('mood') == user_preferences.get('mood')
        
        if genre_match and mood_match:
            genre_matched.append(track)
        elif genre_match:
            mood_matched.append(track)
        else:
            other_tracks.append(track)
    
    # Combiner et limiter les recommandations
    recommendations = (
        genre_matched[:MAX_RECOMMENDATIONS // 2] +
        mood_matched[:MAX_RECOMMENDATIONS // 4] +
        other_tracks[:MAX_RECOMMENDATIONS // 4]
    )[:MAX_RECOMMENDATIONS]
    
    return recommendations

def lambda_handler(event, context):
    """
    Gestionnaire principal pour les recommandations BeatSwipe
    """
    logger.info(f"Événement reçu: {json.dumps(event)}")
    cors_headers = get_cors_headers(event)
    
    # Gestion des requêtes OPTIONS (preflight CORS)
    if event['httpMethod'] == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('Preflight request successful')
        }
    
    try:
        # Extraire l'ID utilisateur du token JWT
        user_id = event['requestContext']['authorizer']['claims']['sub']
        logger.info(f"Récupération des recommandations pour userId: {user_id}")
        
        # Récupérer le profil utilisateur
        user_response = users_table.get_item(Key={'userId': user_id})
        if 'Item' not in user_response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Profil utilisateur non trouvé'})
            }
        
        user_profile = user_response['Item']
        
        # Vérifier si l'utilisateur est un artiste
        if user_profile.get('userType', '').lower() != 'rappeur':
            return {
                'statusCode': 403,
                'headers': cors_headers,
                'body': json.dumps({'message': 'BeatSwipe est uniquement disponible pour les artistes'})
            }
        
        # Récupérer les pistes déjà swipées
        swipe_response = swipes_table.query(
            IndexName='user_id-index',
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
        swiped_track_ids = [item['track_id'] for item in swipe_response.get('Items', [])]
        
        # Définir les préférences utilisateur
        user_preferences = {
            'genres': user_profile.get('musicGenres', []),
            'mood': user_profile.get('musicalMood')
        }
        
        # Récupérer les pistes disponibles
        tracks_response = tracks_table.scan(
            FilterExpression=Attr('user_id').ne(user_id) & 
                             Attr('genre').exists() & 
                             Attr('isPrivate').ne(True),
            Limit=MAX_RECOMMENDATIONS * 3  # Récupérer plus de pistes pour le filtrage
        )
        
        all_tracks = tracks_response.get('Items', [])
        
        # Filtrer et prioriser les recommandations
        recommended_tracks = filter_recommendations(
            all_tracks, 
            user_preferences, 
            swiped_track_ids
        )
        
        # Ajouter des URLs présignées avec la méthode de GetTracks.py
        tracks_with_urls = generate_presigned_urls(recommended_tracks, user_id)
        
        # Filtrer les pistes avec des fichiers manquants
        valid_tracks = [track for track in tracks_with_urls if not track.get('file_missing')]
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'tracks': valid_tracks,
                'count': len(valid_tracks)
            }, cls=DecimalEncoder)
        }
    
    except Exception as e:
        logger.error(f"Erreur non gérée: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'message': 'Erreur interne du serveur',
                'error': str(e)
            })
        }
