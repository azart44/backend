import json
import boto3
import logging
import traceback
import os
import random
import time
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
from datetime import datetime, timedelta
from collections import Counter

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')

# Variables d'environnement
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')
USERS_TABLE = os.environ.get('USERS_TABLE', 'chordora-users')
SWIPES_TABLE = os.environ.get('SWIPES_TABLE', 'chordora-beat-swipes')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'chordora-users')
DEFAULT_IMAGE_KEY = os.environ.get('DEFAULT_IMAGE_KEY', 'public/default-cover.jpg')
MAX_RECOMMENDATIONS = int(os.environ.get('MAX_RECOMMENDATIONS', '20'))
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'development')

# Tables DynamoDB
tracks_table = dynamodb.Table(TRACKS_TABLE)
users_table = dynamodb.Table(USERS_TABLE)
swipes_table = dynamodb.Table(SWIPES_TABLE)
s3 = boto3.client('s3')

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
    
    allowed_origin = origin if origin else 'https://app.chordora.com'
    
    return {
        'Access-Control-Allow-Origin': allowed_origin,
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

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
                except Exception as e:
                    logger.error(f"Erreur lors de la génération de l'URL audio pour {track.get('track_id')}: {str(e)}")
                    
                    # Utiliser une URL d'exemple en mode développement
                    if ENVIRONMENT == 'development':
                        track_with_url['presigned_url'] = "https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3"
            elif ENVIRONMENT == 'development' and 'presigned_url' not in track:
                # Si pas de chemin de fichier mais en mode développement, utiliser une URL d'exemple
                track_with_url['presigned_url'] = "https://www.soundhelix.com/examples/mp3/SoundHelix-Song-1.mp3"
            
            # Traitement des images de couverture
            if 'cover_image' in track and track['cover_image'] and (
                track['cover_image'].startswith('http://') or 
                track['cover_image'].startswith('https://')
            ):
                # Garder l'URL existante
                pass
            
            # Si aucune image valide, utiliser l'image par défaut
            elif not track_with_url.get('cover_image'):
                track_with_url['cover_image'] = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{DEFAULT_IMAGE_KEY}"
            
            # Dupliquer l'URL dans coverImageUrl pour la compatibilité avec le frontend
            track_with_url['coverImageUrl'] = track_with_url.get('cover_image')
            
            tracks_with_urls.append(track_with_url)
            
        except Exception as track_error:
            logger.error(f"Erreur lors du traitement de la piste: {str(track_error)}")
            logger.error(traceback.format_exc())
    
    return tracks_with_urls

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

def file_exists_in_s3(bucket, key):
    """Vérifie si un fichier existe dans S3"""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

class SimpleRecommender:
    """
    Version simplifiée du recommandeur pour BeatSwipe
    Optimisée pour éviter les erreurs et timeouts
    """
    
    def __init__(self, tracks_table, users_table, swipes_table):
        self.tracks_table = tracks_table
        self.users_table = users_table
        self.swipes_table = swipes_table
    
    def get_user_likes(self, user_id):
        """Récupère les pistes likées par l'utilisateur"""
        try:
            # Récupérer les swipes à droite (likes)
            response = self.swipes_table.query(
                IndexName='user_id-index',
                KeyConditionExpression=Key('user_id').eq(user_id),
                FilterExpression=Attr('action').eq('right')
            )
            
            return [swipe.get('track_id') for swipe in response.get('Items', []) if 'track_id' in swipe]
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des likes: {str(e)}")
            return []
    
    def get_user_swipes(self, user_id):
        """Récupère tous les swipes d'un utilisateur"""
        try:
            response = self.swipes_table.query(
                IndexName='user_id-index',
                KeyConditionExpression=Key('user_id').eq(user_id)
            )
            
            return [swipe.get('track_id') for swipe in response.get('Items', []) if 'track_id' in swipe]
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des swipes: {str(e)}")
            return []
    
    def analyze_preferences(self, user_id, user_profile):
        """
        Analyse les préférences de l'utilisateur
        """
        # Récupérer les pistes likées
        liked_track_ids = self.get_user_likes(user_id)
        
        # Si pas de likes, utiliser uniquement les préférences du profil
        if not liked_track_ids:
            return {
                'genres': user_profile.get('musicGenres', []),
                'mood': user_profile.get('musicalMood'),
                'preferred_beatmakers': [],
                'avg_bpm': None
            }
        
        # Récupérer les détails des pistes likées (une par une pour éviter les erreurs)
        liked_tracks = []
        for track_id in liked_track_ids:
            try:
                response = self.tracks_table.get_item(Key={'track_id': track_id})
                if 'Item' in response:
                    liked_tracks.append(response['Item'])
            except Exception as e:
                logger.warning(f"Erreur lors de la récupération de la piste {track_id}: {str(e)}")
        
        # Extraire les attributs
        genres = [track.get('genre') for track in liked_tracks if track.get('genre')]
        moods = [track.get('mood') for track in liked_tracks if track.get('mood')]
        beatmakers = [track.get('user_id') for track in liked_tracks if track.get('user_id')]
        
        # Compter les occurrences
        genre_counter = Counter(genres)
        mood_counter = Counter(moods)
        beatmaker_counter = Counter(beatmakers)
        
        # Extraire les BPM
        bpms = []
        for track in liked_tracks:
            if track.get('bpm'):
                try:
                    bpm = float(track['bpm'])
                    bpms.append(bpm)
                except (ValueError, TypeError):
                    pass
        
        # Calculer le BPM moyen
        avg_bpm = sum(bpms) / len(bpms) if bpms else None
        
        # Ajouter les préférences explicites du profil
        explicit_genres = user_profile.get('musicGenres', [])
        for genre in explicit_genres:
            genre_counter[genre] = genre_counter.get(genre, 0) + 3  # Donner plus de poids
        
        # Construire les préférences
        preferences = {
            'genres': [genre for genre, _ in genre_counter.most_common(5)],
            'mood': user_profile.get('musicalMood'),
            'preferred_beatmakers': [bm for bm, _ in beatmaker_counter.most_common(5)],
            'avg_bpm': avg_bpm
        }
        
        return preferences
    
    def score_track(self, track, preferences, swiped_track_ids):
        """
        Attribue un score à une piste en fonction des préférences
        """
        # Ignorer les pistes déjà swipées
        if track['track_id'] in swiped_track_ids:
            return -1
        
        score = 0
        
        # 1. Match de genre
        track_genre = track.get('genre')
        if track_genre in preferences['genres']:
            score += 5
        
        # 2. Match de mood
        track_mood = track.get('mood')
        user_mood = preferences['mood']
        if user_mood and track_mood == user_mood:
            score += 3
        
        # 3. Match de beatmaker
        track_beatmaker = track.get('user_id')
        if track_beatmaker in preferences['preferred_beatmakers']:
            score += 2
        
        # 4. Match de BPM
        avg_bpm = preferences['avg_bpm']
        if avg_bpm and track.get('bpm'):
            try:
                track_bpm = float(track['bpm'])
                diff = abs(track_bpm - avg_bpm)
                if diff <= 10:
                    score += 2
                elif diff <= 20:
                    score += 1
            except (ValueError, TypeError):
                pass
        
        # 5. Popularité
        likes = int(track.get('likes', 0))
        if likes > 10:
            score += min(1, likes / 50)
        
        # 6. Ajouter un peu d'aléatoire pour la diversité
        score += random.uniform(0, 0.5)
        
        return score
    
    def get_recommendations(self, user_id, max_recommendations=20):
        """
        Génère des recommandations pour un utilisateur
        """
        # 1. Récupérer les swipes existants
        swiped_track_ids = self.get_user_swipes(user_id)
        logger.info(f"Récupéré {len(swiped_track_ids)} swipes pour {user_id}")
        
        # 2. Récupérer le profil utilisateur
        try:
            response = self.users_table.get_item(Key={'userId': user_id})
            user_profile = response.get('Item', {})
        except Exception as e:
            logger.error(f"Erreur lors de la récupération du profil: {str(e)}")
            user_profile = {}
        
        # 3. Analyser les préférences
        preferences = self.analyze_preferences(user_id, user_profile)
        logger.info(f"Préférences analysées pour {user_id}: {preferences}")
        
        # 4. Récupérer les pistes disponibles (limiter pour éviter les timeouts)
        try:
            params = {
                'FilterExpression': Attr('user_id').ne(user_id) & 
                                  Attr('genre').exists() & 
                                  Attr('isPrivate').ne(True),
                'Limit': 100  # Limiter pour des raisons de performance
            }
            
            response = self.tracks_table.scan(**params)
            all_tracks = response.get('Items', [])
            
            logger.info(f"Récupéré {len(all_tracks)} pistes pour scoring")
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des pistes: {str(e)}")
            all_tracks = []
        
        # 5. Scorer et filtrer les pistes
        scored_tracks = []
        for track in all_tracks:
            score = self.score_track(track, preferences, swiped_track_ids)
            if score >= 0:
                scored_tracks.append((track, score))
        
        # 6. Trier et retourner les meilleures
        scored_tracks.sort(key=lambda x: x[1], reverse=True)
        
        # Log pour debug
        if scored_tracks:
            logger.info(f"Top tracks pour {user_id}:")
            for i, (track, score) in enumerate(scored_tracks[:3]):
                logger.info(f"{i+1}. {track.get('title')} - Score: {score}")
        
        return [track for track, _ in scored_tracks[:max_recommendations]]

def lambda_handler(event, context):
    """Gestionnaire principal pour les recommandations BeatSwipe"""
    logger.info(f"Événement reçu: {event}")
    cors_headers = get_cors_headers(event)
    
    # Gestion des requêtes OPTIONS (preflight CORS)
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('Preflight request successful')
        }
    
    try:
        # Extraire l'ID utilisateur du token JWT
        if 'requestContext' not in event or 'authorizer' not in event['requestContext'] or 'claims' not in event['requestContext']['authorizer']:
            return {
                'statusCode': 401,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Utilisateur non authentifié'})
            }
        
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
        
        # Utiliser le recommandeur simplifié pour éviter les timeouts
        recommender = SimpleRecommender(tracks_table, users_table, swipes_table)
        recommended_tracks = recommender.get_recommendations(user_id, MAX_RECOMMENDATIONS)
        
        # Ajouter des URLs présignées
        tracks_with_urls = generate_presigned_urls(recommended_tracks, user_id)
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'tracks': tracks_with_urls,
                'count': len(tracks_with_urls)
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
