import json
import boto3
import logging
import traceback
import os
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
from datetime import datetime, timedelta
import random

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
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'chordora-users')
DEFAULT_IMAGE_KEY = os.environ.get('DEFAULT_IMAGE_KEY', 'public/default-cover.jpg')
MAX_RECOMMENDATIONS = int(os.environ.get('MAX_RECOMMENDATIONS', '20'))
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'development')  # 'development' ou 'production'

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

def generate_mock_data(user_id, count=10):
    """
    Génère des données mock pour le développement
    """
    genres = ["Trap", "Drill", "Hip Hop", "Boom Bap", "R&B", "Pop", "Electronic"]
    moods = ["Mélancolique", "Énergique", "Festif", "Agressif", "Chill", "Sombre", "Inspirant"]
    
    mock_tracks = []
    
    # Générer plusieurs beatmakers aléatoires
    beatmaker_ids = [f"mock-beatmaker-{i}" for i in range(1, 6)]
    beatmaker_names = [f"MockProducer{i}" for i in range(1, 6)]
    
    for i in range(count):
        beatmaker_index = random.randint(0, 4)
        beatmaker_id = beatmaker_ids[beatmaker_index]
        
        # Utiliser une URL d'image statique différente pour chaque piste
        cover_image_url = f"https://source.unsplash.com/random/300x300?music&sig={i}"
        
        mock_track = {
            "track_id": f"mock-track-{i}",
            "user_id": beatmaker_id,
            "title": f"Beat Sample {i}",
            "genre": random.choice(genres),
            "mood": random.choice(moods),
            "bpm": random.randint(70, 160),
            "duration": random.randint(120, 300),
            "artist": beatmaker_names[beatmaker_index],
            "likes": random.randint(0, 100),
            "created_at": int(datetime.now().timestamp()) - random.randint(0, 30*24*60*60),
            # URL d'image de couverture (nouvelle approche)
            "cover_image": cover_image_url,
            # URL audio factice
            "presigned_url": "https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3"
        }
        mock_tracks.append(mock_track)
    
    return mock_tracks

def file_exists_in_s3(bucket, key):
    """Vérifie si un fichier existe dans S3"""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

def generate_presigned_urls(tracks, auth_user_id=None):
    """
    Génère des URLs présignées pour les pistes audio et les images de couverture
    """
    tracks_with_urls = []
    
    for track in tracks:
        try:
            # Créer une copie pour éviter de modifier l'original
            track_with_url = dict(track)
            
            # Récupérer les informations de l'artiste
            if 'user_id' in track:
                user_profile = get_user_profile(track['user_id'])
                if user_profile and 'username' in user_profile:
                    track_with_url['artist'] = user_profile['username']
                else:
                    track_with_url['artist'] = track.get('artist', "Artiste")
            else:
                track_with_url['artist'] = track.get('artist', "Artiste")
            
            # Génération d'URL présignée pour le fichier audio
            if 'file_path' in track:
                try:
                    if file_exists_in_s3(BUCKET_NAME, track['file_path']):
                        # Extraire la durée du fichier audio si non disponible
                        if 'duration' not in track or not track['duration']:
                            track_with_url['duration'] = get_audio_duration(BUCKET_NAME, track['file_path'])
                        
                        # Générer l'URL présignée pour l'audio
                        presigned_url = s3.generate_presigned_url(
                            'get_object',
                            Params={
                                'Bucket': BUCKET_NAME, 
                                'Key': track['file_path'],
                                'ResponseContentType': 'audio/mpeg',
                                'ResponseContentDisposition': 'inline'
                            },
                            ExpiresIn=86400  # 24 heures
                        )
                        
                        track_with_url['presigned_url'] = presigned_url
                        logger.info(f"URL audio présignée générée pour la piste {track.get('track_id')}")
                    else:
                        logger.warning(f"Le fichier audio n'existe pas dans S3: {track['file_path']}")
                        
                        # Utiliser une URL d'exemple en mode développement
                        if ENVIRONMENT == 'development':
                            track_with_url['presigned_url'] = "https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3"
                            track_with_url['duration'] = track.get('duration', random.randint(120, 300))
                        else:
                            track_with_url['file_missing'] = True
                except Exception as e:
                    logger.error(f"Erreur lors de la génération de l'URL audio pour {track.get('track_id')}: {str(e)}")
                    
                    # Utiliser une URL d'exemple en mode développement
                    if ENVIRONMENT == 'development':
                        track_with_url['presigned_url'] = "https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3"
                        track_with_url['duration'] = track.get('duration', random.randint(120, 300))
                    else:
                        track_with_url['error'] = 'Could not generate audio URL'
            elif ENVIRONMENT == 'development' and 'presigned_url' not in track:
                # Si pas de chemin de fichier mais en mode développement, utiliser une URL d'exemple
                track_with_url['presigned_url'] = "https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3"
                track_with_url['duration'] = track.get('duration', random.randint(120, 300))
            
            # SECTION CORRIGÉE: Traitement des images de couverture avec meilleure priorité
            # 1. Vérifier si une URL cover_image existante est déjà fournie
            if 'cover_image' in track and track['cover_image'] and (
                track['cover_image'].startswith('http://') or 
                track['cover_image'].startswith('https://')
            ):
                # Si l'URL est déjà une URL absolue, la garder
                logger.info(f"Utilisation de l'URL de couverture existante: {track['cover_image'][:50]}...")
                # L'URL est déjà copiée dans track_with_url car c'est une copie de track
            
            # 2. Sinon, essayer de générer à partir de cover_image_path
            elif 'cover_image_path' in track and track['cover_image_path']:
                try:
                    if file_exists_in_s3(BUCKET_NAME, track['cover_image_path']):
                        # Générer une URL présignée pour l'image
                        cover_url = s3.generate_presigned_url(
                            'get_object',
                            Params={
                                'Bucket': BUCKET_NAME, 
                                'Key': track['cover_image_path'],
                                'ResponseContentType': 'image/jpeg',
                                'ResponseContentDisposition': 'inline'
                            },
                            ExpiresIn=86400  # 24 heures
                        )
                        track_with_url['cover_image'] = cover_url
                        logger.info(f"URL d'image de couverture générée pour {track.get('track_id')}")
                    else:
                        logger.warning(f"L'image de couverture n'existe pas dans S3: {track['cover_image_path']}")
                        # Utiliser l'image par défaut
                        track_with_url['cover_image'] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{DEFAULT_IMAGE_KEY}"
                except Exception as e:
                    logger.error(f"Erreur lors de la génération de l'URL de couverture: {str(e)}")
                    track_with_url['cover_image'] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{DEFAULT_IMAGE_KEY}"
            
            # 3. Si aucune source d'image n'est trouvée, utiliser l'image par défaut
            elif not track_with_url.get('cover_image'):
                track_with_url['cover_image'] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{DEFAULT_IMAGE_KEY}"
                logger.info(f"Utilisation de l'image de couverture par défaut pour {track.get('track_id')}")
            
            # Dupliquer l'URL dans coverImageUrl pour la compatibilité avec le frontend
            track_with_url['coverImageUrl'] = track_with_url['cover_image']
            
            # Log des URLs pour débogage
            logger.debug(f"URLs finales pour {track.get('track_id')}:\n" +
                        f"Audio: {track_with_url.get('presigned_url', 'Non disponible')[:50]}...\n" +
                        f"Image: {track_with_url.get('cover_image', 'Non disponible')[:50]}...")
            
            tracks_with_urls.append(track_with_url)
            
        except Exception as track_error:
            logger.error(f"Erreur lors du traitement de la piste: {str(track_error)}")
            logger.error(traceback.format_exc())
            # Ajouter quand même la piste avec une erreur en mode développement
            if ENVIRONMENT == 'development':
                tracks_with_urls.append({
                    'track_id': track.get('track_id', f"error-track-{len(tracks_with_urls)}"),
                    'title': track.get('title', 'Piste inconnue'),
                    'artist': track.get('artist', 'Artiste'),
                    'genre': track.get('genre', 'Unknown'),
                    'bpm': track.get('bpm', 120),
                    'duration': track.get('duration', 180),
                    'presigned_url': "https://www.soundhelix.com/examples/mp3/SoundHelix-Song-2.mp3",
                    'cover_image': "https://source.unsplash.com/random/300x300?error"
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
    
    # Mélanger légèrement pour éviter de présenter toujours les mêmes en premier
    random.shuffle(recommendations)
    
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
        
        # Vérifier s'il faut utiliser des données mockées
        use_mock_data = (ENVIRONMENT == 'development' and 
                        (event.get('queryStringParameters', {}) or {}).get('mock', 'false').lower() == 'true')
        
        if use_mock_data:
            logger.info("Utilisation de données mockées pour le développement")
            all_tracks = generate_mock_data(user_id, count=15)
        else:
            # Récupérer les pistes disponibles
            tracks_response = tracks_table.scan(
                FilterExpression=Attr('user_id').ne(user_id) & 
                                Attr('genre').exists() & 
                                Attr('isPrivate').ne(True),
                Limit=MAX_RECOMMENDATIONS * 3  # Récupérer plus de pistes pour le filtrage
            )
            
            all_tracks = tracks_response.get('Items', [])
            
            # Si pas assez de pistes réelles et qu'on est en développement, ajouter des pistes mockées
            if len(all_tracks) < 5 and ENVIRONMENT == 'development':
                logger.info(f"Pas assez de pistes réelles ({len(all_tracks)}), ajout de pistes mockées")
                mock_tracks = generate_mock_data(user_id, count=10)
                all_tracks.extend(mock_tracks)
        
        # Filtrer et prioriser les recommandations
        recommended_tracks = filter_recommendations(
            all_tracks, 
            user_preferences, 
            swiped_track_ids
        )
        
        # Ajouter des URLs présignées
        tracks_with_urls = generate_presigned_urls(recommended_tracks, user_id)
        
        # Filtrer les pistes avec des fichiers manquants seulement en production
        if ENVIRONMENT == 'production':
            valid_tracks = [track for track in tracks_with_urls if not track.get('file_missing')]
        else:
            valid_tracks = tracks_with_urls
        
        # Dernière vérification pour s'assurer que toutes les pistes ont une cover_image
        for track in valid_tracks:
            if not track.get('cover_image'):
                track['cover_image'] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{DEFAULT_IMAGE_KEY}"
                track['coverImageUrl'] = track['cover_image']
        
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
