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
from collections import Counter, defaultdict

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')

# Variables d'environnement
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')
USERS_TABLE = os.environ.get('USERS_TABLE', 'chordora-users')
SWIPES_TABLE = os.environ.get('SWIPES_TABLE', 'chordora-beat-swipes')
LIKES_TABLE = os.environ.get('LIKES_TABLE', 'chordora-track-likes')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'chordora-users')
DEFAULT_IMAGE_KEY = os.environ.get('DEFAULT_IMAGE_KEY', 'public/default-cover.jpg')
MAX_RECOMMENDATIONS = int(os.environ.get('MAX_RECOMMENDATIONS', '20'))
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'development')

# Tables DynamoDB
tracks_table = dynamodb.Table(TRACKS_TABLE)
users_table = dynamodb.Table(USERS_TABLE)
swipes_table = dynamodb.Table(SWIPES_TABLE)
likes_table = dynamodb.Table(LIKES_TABLE)
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
    tracks_with_urls = []
    
    for track in tracks:
        try:
            track_with_url = dict(track)
            
            # Amélioration de la gestion des URLs de couverture
            cover_image_url = None
            
            # Priorités pour les URLs de cover
            if track.get('cover_image') and (track['cover_image'].startswith('http://') or track['cover_image'].startswith('https://')):
                cover_image_url = track['cover_image']
            elif track.get('cover_image_path'):
                # Construire l'URL S3 complète
                cover_image_url = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{track['cover_image_path']}"
            
            # Si aucune URL valide n'est trouvée, utiliser l'image par défaut
            if not cover_image_url:
                cover_image_url = f"https://{BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{DEFAULT_IMAGE_KEY}"
            
            # Ajouter les URLs de couverture au track
            track_with_url['cover_image'] = cover_image_url
            track_with_url['coverImageUrl'] = cover_image_url
            
            # Générer l'URL présignée pour le fichier audio si nécessaire
            if 'file_path' in track and not track.get('presigned_url'):
                try:
                    presigned_url = s3.generate_presigned_url(
                        'get_object',
                        Params={
                            'Bucket': BUCKET_NAME,
                            'Key': track['file_path'],
                            'ResponseContentType': 'audio/mpeg',
                            'ResponseContentDisposition': 'inline'
                        },
                        ExpiresIn=86400  # URL valide 24 heures
                    )
                    track_with_url['presigned_url'] = presigned_url
                except Exception as e:
                    logger.error(f"Erreur lors de la génération de l'URL présignée pour {track.get('track_id', 'unknown')}: {str(e)}")
            
            tracks_with_urls.append(track_with_url)
            
        except Exception as track_error:
            logger.error(f"Erreur lors du traitement de la piste: {str(track_error)}")
    
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

class ImprovedRecommender:
    """
    Version améliorée du recommandeur pour BeatSwipe
    Avec système de scoring précis et personnalisé
    """
    
    def __init__(self, tracks_table, users_table, swipes_table, likes_table):
        self.tracks_table = tracks_table
        self.users_table = users_table
        self.swipes_table = swipes_table
        self.likes_table = likes_table
    
    def get_user_swipes(self, user_id, action=None, days_limit=None):
        """
        Récupère les swipes d'un utilisateur avec filtres optionnels
        
        Args:
            user_id (str): ID de l'utilisateur
            action (str, optional): Filtrer par type d'action (right, left, down)
            days_limit (int, optional): Limiter aux X derniers jours
            
        Returns:
            list: Liste des swipes correspondant aux critères
        """
        try:
            # Construire l'expression de requête de base
            query_params = {
                'IndexName': 'user_id-index',
                'KeyConditionExpression': Key('user_id').eq(user_id)
            }
            
            # Construire l'expression de filtre
            filter_expressions = []
            expression_values = {}
            expression_names = {}
            
            if action:
                filter_expressions.append('#act = :action')
                expression_values[':action'] = action
                expression_names['#act'] = 'action'
            
            if days_limit:
                # Calculer le timestamp pour la limite de jours
                cutoff_time = int((datetime.now() - timedelta(days=days_limit)).timestamp())
                filter_expressions.append('timestamp >= :cutoff')
                expression_values[':cutoff'] = cutoff_time
            
            # Ajouter les expressions à la requête si nécessaire
            if filter_expressions:
                query_params['FilterExpression'] = ' AND '.join(filter_expressions)
                query_params['ExpressionAttributeValues'] = expression_values
                
                if expression_names:
                    query_params['ExpressionAttributeNames'] = expression_names
            
            # Exécuter la requête
            response = self.swipes_table.query(**query_params)
            swipes = response.get('Items', [])
            
            # Log pour débogage
            logger.info(f"Récupéré {len(swipes)} swipes pour {user_id}" + 
                       (f" avec action '{action}'" if action else "") +
                       (f" des {days_limit} derniers jours" if days_limit else ""))
            
            return swipes
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des swipes: {str(e)}")
            logger.error(traceback.format_exc())
            return []
    
    def get_user_likes(self, user_id):
        """
        Récupère les pistes likées par l'utilisateur (depuis la table des likes)
        
        Returns:
            list: Liste des ID de pistes likées
        """
        try:
            # Récupérer les likes de l'utilisateur
            response = self.likes_table.query(
                IndexName='user_id-index',  # Assurez-vous que cet index existe
                KeyConditionExpression=Key('user_id').eq(user_id)
            )
            likes = response.get('Items', [])
            
            # Extraire les IDs de pistes
            return [like.get('track_id') for like in likes if 'track_id' in like]
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des likes: {str(e)}")
            return []
    
    def get_user_genre_preferences(self, user_id, user_profile):
        """
        Analyse les préférences de genre de l'utilisateur en combinant:
        1. Les genres likés via BeatSwipe
        2. Les genres likés via la fonctionnalité de like standard
        3. Les genres déclarés dans le profil utilisateur
        
        Args:
            user_id (str): ID de l'utilisateur
            user_profile (dict): Profil de l'utilisateur
            
        Returns:
            dict: Dictionnaire des genres avec leurs scores de préférence
        """
        genre_scores = defaultdict(float)
        
        # 1. Analyser les swipes à droite récents (plus de poids)
        right_swipes = self.get_user_swipes(user_id, 'right', days_limit=30)
        track_ids = [swipe.get('track_id') for swipe in right_swipes if 'track_id' in swipe]
        
        # Récupérer les genres des pistes swipées à droite
        swipe_tracks_genres = self.get_tracks_genres(track_ids)
        
        # Compter et scorer les genres des swipes récents (poids important)
        for genre, count in Counter(swipe_tracks_genres).items():
            if genre:  # Ignorer les genres vides/null
                # Score plus élevé pour les swipes récents
                genre_scores[genre] += count * 2
        
        # 2. Analyser les likes standard (poids moyen)
        liked_track_ids = self.get_user_likes(user_id)
        like_tracks_genres = self.get_tracks_genres(liked_track_ids)
        
        # Compter et scorer les genres des likes
        for genre, count in Counter(like_tracks_genres).items():
            if genre:
                genre_scores[genre] += count * 1.5
        
        # 3. Intégrer les préférences explicites du profil (poids très élevé)
        explicit_genres = user_profile.get('musicGenres', [])
        for genre in explicit_genres:
            if genre:
                genre_scores[genre] += 3  # Poids plus important
        
        # Si aucune préférence n'est trouvée, utiliser des genres populaires par défaut
        if not genre_scores:
            default_genres = ["Trap", "Hip Hop", "Drill", "RnB", "Boom Bap"]
            for genre in default_genres:
                genre_scores[genre] = 1
        
        # Normaliser les scores pour qu'ils soient entre 0 et 10
        if genre_scores:
            max_score = max(genre_scores.values())
            if max_score > 0:
                for genre in genre_scores:
                    genre_scores[genre] = (genre_scores[genre] / max_score) * 10
        
        return dict(genre_scores)
    
    def get_tracks_genres(self, track_ids):
        """
        Récupère les genres des pistes spécifiées
        
        Args:
            track_ids (list): Liste d'IDs de pistes
            
        Returns:
            list: Liste des genres (peut contenir des doublons)
        """
        genres = []
        
        # Limiter le nombre de requêtes pour éviter les timeouts
        unique_track_ids = list(set(track_ids))[:50]  # Limiter à 50 pistes uniques
        
        # Récupérer les pistes une par une
        for track_id in unique_track_ids:
            try:
                response = self.tracks_table.get_item(Key={'track_id': track_id})
                if 'Item' in response and 'genre' in response['Item']:
                    genres.append(response['Item']['genre'])
            except Exception as e:
                logger.warning(f"Erreur lors de la récupération du genre pour la piste {track_id}: {str(e)}")
        
        return genres
    
    def get_user_mood_preferences(self, user_id, user_profile):
        """
        Analyse les préférences de mood de l'utilisateur
        avec gestion robuste des erreurs
        
        Returns:
            dict: Dictionnaire des moods avec leurs scores
        """
        mood_scores = defaultdict(float)
        
        # 1. Récupérer les pistes likées récemment
        try:
            right_swipes = self.get_user_swipes(user_id, 'right', days_limit=30)
            track_ids = [swipe.get('track_id') for swipe in right_swipes if 'track_id' in swipe]
        except Exception as e:
            logger.warning(f"Erreur lors de la récupération des swipes pour l'analyse mood: {str(e)}")
            track_ids = []
            
            # Prioriser le mood du profil utilisateur en cas d'erreur
            user_mood = user_profile.get('musicalMood')
            if user_mood:
                mood_scores[user_mood] = 10
                return dict(mood_scores)
        
        # 2. Récupérer les moods des pistes
        moods = []
        unique_track_ids = list(set(track_ids))[:50]
        
        for track_id in unique_track_ids:
            try:
                response = self.tracks_table.get_item(Key={'track_id': track_id})
                if 'Item' in response and 'mood' in response['Item']:
                    moods.append(response['Item']['mood'])
            except Exception as e:
                logger.warning(f"Erreur lors de la récupération du mood pour {track_id}: {str(e)}")
        
        # 3. Compter et scorer les moods
        for mood, count in Counter(moods).items():
            if mood:
                mood_scores[mood] += count * 2
        
        # 4. Intégrer le mood du profil utilisateur (priorité maximale)
        user_mood = user_profile.get('musicalMood')
        if user_mood:
            mood_scores[user_mood] += 5
        
        # Normaliser les scores
        if mood_scores:
            max_score = max(mood_scores.values())
            if max_score > 0:
                for mood in mood_scores:
                    mood_scores[mood] = (mood_scores[mood] / max_score) * 10
        
        return dict(mood_scores)
    
    def get_user_bpm_preferences(self, user_id):
        """
        Analyse les préférences de BPM de l'utilisateur avec gestion des erreurs
        
        Returns:
            dict: Informations sur les BPM préférés (moyenne, plage)
        """
        # Valeurs par défaut en cas d'erreur
        default_bpm_prefs = {
            'avg_bpm': 125,  # BPM moyen courant pour les tracks rap
            'min_bpm': 90,
            'max_bpm': 160,
            'has_preference': False
        }
        
        try:
            # Récupérer les pistes likées récemment
            right_swipes = self.get_user_swipes(user_id, 'right', days_limit=30)
            track_ids = [swipe.get('track_id') for swipe in right_swipes if 'track_id' in swipe]
            
            # Si aucun swipe n'est trouvé, utiliser les valeurs par défaut
            if not track_ids:
                return default_bpm_prefs
        except Exception as e:
            logger.warning(f"Erreur lors de la récupération des swipes pour l'analyse BPM: {str(e)}")
            return default_bpm_prefs
        
        # Récupérer les BPM des pistes
        bpms = []
        unique_track_ids = list(set(track_ids))[:50]
        
        for track_id in unique_track_ids:
            try:
                response = self.tracks_table.get_item(Key={'track_id': track_id})
                if 'Item' in response and 'bpm' in response['Item']:
                    try:
                        bpm = float(response['Item']['bpm'])
                        if 40 <= bpm <= 200:  # Filtrer les valeurs aberrantes
                            bpms.append(bpm)
                    except (ValueError, TypeError):
                        continue
            except Exception as e:
                logger.warning(f"Erreur lors de la récupération du BPM pour {track_id}: {str(e)}")
        
        # Calculer les statistiques
        if not bpms:
            return {
                'avg_bpm': None,
                'min_bpm': None,
                'max_bpm': None,
                'has_preference': False
            }
        
        avg_bpm = sum(bpms) / len(bpms)
        min_bpm = min(bpms)
        max_bpm = max(bpms)
        
        return {
            'avg_bpm': avg_bpm,
            'min_bpm': min_bpm,
            'max_bpm': max_bpm,
            'has_preference': True
        }
    
    def get_preferred_beatmakers(self, user_id):
        """
        Détermine les beatmakers préférés en fonction des likes et swipes
        
        Returns:
            dict: Dictionnaire des IDs de beatmakers avec leurs scores
        """
        beatmaker_scores = defaultdict(float)
        
        # 1. Récupérer les pistes swipées à droite
        right_swipes = self.get_user_swipes(user_id, 'right')
        swipe_track_ids = [swipe.get('track_id') for swipe in right_swipes if 'track_id' in swipe]
        
        # 2. Récupérer les likes
        liked_track_ids = self.get_user_likes(user_id)
        
        # Combiner les deux listes
        all_track_ids = list(set(swipe_track_ids + liked_track_ids))[:100]  # Limiter pour performance
        
        # 3. Récupérer les beatmakers des pistes
        for track_id in all_track_ids:
            try:
                response = self.tracks_table.get_item(Key={'track_id': track_id})
                if 'Item' in response and 'user_id' in response['Item']:
                    beatmaker_id = response['Item']['user_id']
                    
                    # Donner un score plus élevé si la piste a été à la fois likée et swipée
                    score = 1
                    if track_id in swipe_track_ids and track_id in liked_track_ids:
                        score = 3
                    elif track_id in swipe_track_ids:
                        score = 2
                    
                    beatmaker_scores[beatmaker_id] += score
            except Exception as e:
                logger.warning(f"Erreur lors de la récupération du beatmaker pour {track_id}: {str(e)}")
        
        # Normaliser les scores
        if beatmaker_scores:
            max_score = max(beatmaker_scores.values())
            if max_score > 0:
                for beatmaker_id in beatmaker_scores:
                    beatmaker_scores[beatmaker_id] = (beatmaker_scores[beatmaker_id] / max_score) * 10
        
        return dict(beatmaker_scores)
    
    def analyze_user_preferences(self, user_id, user_profile):
        """
        Analyse complète des préférences de l'utilisateur
        
        Args:
            user_id (str): ID de l'utilisateur
            user_profile (dict): Profil de l'utilisateur
            
        Returns:
            dict: Préférences complètes de l'utilisateur
        """
        # Récupérer tous les swipes de l'utilisateur pour exclusion
        all_swipes = self.get_user_swipes(user_id)
        swiped_track_ids = [swipe.get('track_id') for swipe in all_swipes if 'track_id' in swipe]
        
        # Analyser les préférences de genre
        genre_preferences = self.get_user_genre_preferences(user_id, user_profile)
        
        # Analyser les préférences de mood
        mood_preferences = self.get_user_mood_preferences(user_id, user_profile)
        
        # Analyser les préférences de BPM
        bpm_preferences = self.get_user_bpm_preferences(user_id)
        
        # Déterminer les beatmakers préférés
        beatmaker_preferences = self.get_preferred_beatmakers(user_id)
        
        # Log des préférences pour debug
        logger.info(f"Préférences de genre pour {user_id}: {genre_preferences}")
        logger.info(f"Préférences de mood pour {user_id}: {mood_preferences}")
        logger.info(f"Préférences de BPM pour {user_id}: {bpm_preferences}")
        logger.info(f"Préférences de beatmakers pour {user_id}: {beatmaker_preferences}")
        
        return {
            'genre_preferences': genre_preferences,
            'mood_preferences': mood_preferences,
            'bpm_preferences': bpm_preferences,
            'beatmaker_preferences': beatmaker_preferences,
            'swiped_track_ids': swiped_track_ids,
            'user_id': user_id
        }
    
    def score_track(self, track, preferences):
        """
        Attribue un score à une piste en fonction des préférences de l'utilisateur
        
        Args:
            track (dict): La piste à évaluer
            preferences (dict): Les préférences de l'utilisateur
            
        Returns:
            float: Score de la piste
        """
        # Ignorer les pistes déjà swipées
        if track['track_id'] in preferences['swiped_track_ids']:
            return -1
        
        # Ignorer les pistes de l'utilisateur lui-même
        if track.get('user_id') == preferences['user_id']:
            return -1
        
        # Ignorer les pistes privées
        if track.get('isPrivate', False):
            return -1
        
        # Initialiser le score
        score = 0
        
        # 1. Score basé sur le genre (poids important: 35%)
        track_genre = track.get('genre')
        if track_genre and track_genre in preferences['genre_preferences']:
            genre_score = preferences['genre_preferences'][track_genre]
            score += genre_score * 0.35
        
        # 2. Score basé sur le mood (poids moyen: 25%)
        track_mood = track.get('mood')
        if track_mood and track_mood in preferences['mood_preferences']:
            mood_score = preferences['mood_preferences'][track_mood]
            score += mood_score * 0.25
        
        # 3. Score basé sur le BPM (poids moyen: 15%)
        bpm_pref = preferences['bpm_preferences']
        if track.get('bpm') and bpm_pref['has_preference']:
            try:
                track_bpm = float(track['bpm'])
                avg_bpm = bpm_pref['avg_bpm']
                
                # Calculer la différence avec le BPM moyen préféré
                diff = abs(track_bpm - avg_bpm)
                
                # Score inversement proportionnel à l'écart
                if diff <= 5:
                    bpm_score = 10  # Excellent match
                elif diff <= 10:
                    bpm_score = 8
                elif diff <= 20:
                    bpm_score = 5
                elif diff <= 30:
                    bpm_score = 3
                else:
                    bpm_score = 1  # Match faible
                
                score += bpm_score * 0.15
            except (ValueError, TypeError):
                pass
        
        # 4. Score basé sur le beatmaker (poids moyen: 15%)
        track_beatmaker = track.get('user_id')
        if track_beatmaker and track_beatmaker in preferences['beatmaker_preferences']:
            beatmaker_score = preferences['beatmaker_preferences'][track_beatmaker]
            score += beatmaker_score * 0.15
        
        # 5. Score basé sur la popularité (poids faible: 5%)
        likes = int(track.get('likes', 0))
        plays = int(track.get('plays', 0))
        
        popularity_score = min(10, (likes * 0.7 + plays * 0.3) / 20)
        score += popularity_score * 0.05
        
        # 6. Facteur de nouveauté (poids faible: 5%)
        # Favoriser légèrement les pistes récentes
        if 'created_at' in track:
            try:
                created_timestamp = int(track['created_at'])
                now = int(time.time())
                age_days = (now - created_timestamp) / (24 * 3600)
                
                if age_days <= 7:  # Piste de moins d'une semaine
                    novelty_score = 10
                elif age_days <= 30:  # Piste de moins d'un mois
                    novelty_score = 7
                elif age_days <= 90:  # Piste de moins de trois mois
                    novelty_score = 5
                else:
                    novelty_score = 3
                
                score += novelty_score * 0.05
            except (ValueError, TypeError):
                # Si on ne peut pas déterminer l'âge, score neutre
                score += 5 * 0.05
        else:
            # Si pas de date de création, score neutre
            score += 5 * 0.05
        
        return score
    
    def get_recommendations(self, user_id, max_recommendations=20):
        """
        Génère des recommandations personnalisées pour un utilisateur
        avec gestion robuste des erreurs pour éviter les timeouts Lambda
        
        Args:
            user_id (str): ID de l'utilisateur
            max_recommendations (int): Nombre maximum de recommandations
            
        Returns:
            list: Liste des pistes recommandées
        """
        # Mettre en place une gestion du temps d'exécution pour éviter les timeouts
        start_time = time.time()
        # Allouer 80% du temps d'exécution max de Lambda (défaut 3 secondes)
        max_execution_time = 3
        
        # 1. Récupérer le profil utilisateur
        try:
            response = self.users_table.get_item(Key={'userId': user_id})
            user_profile = response.get('Item', {})
        except Exception as e:
            logger.error(f"Erreur lors de la récupération du profil: {str(e)}")
            user_profile = {}
        
        # 2. Analyser les préférences
        preferences = self.analyze_user_preferences(user_id, user_profile)
        
        # 3. Récupérer les pistes pour scoring avec une approche simplifiée et robuste
        all_tracks = []
        
        try:
            # Vérifier le temps restant disponible
            current_time = time.time()
            time_elapsed = current_time - start_time
            logger.info(f"Temps écoulé avant récupération des pistes: {time_elapsed:.2f}s")
            
            # Simplifier la récupération des pistes pour éviter les timeouts
            # Utiliser un simple scan avec une limite réduite
            response = self.tracks_table.scan(
                FilterExpression=Attr('isPrivate').ne(True),
                Limit=80  # Limite réduite pour de meilleures performances
            )
            all_tracks = response.get('Items', [])
            
            # Logger le nombre de pistes récupérées pour monitoring
            logger.info(f"Récupération simplifiée: {len(all_tracks)} pistes")
            
            # Filtre préliminaire basé sur les préférences pour réduire la charge de traitement
            if all_tracks and preferences['genre_preferences']:
                # Obtenir la liste des genres préférés
                preferred_genres = [
                    genre for genre, score in preferences['genre_preferences'].items() 
                    if score > 5  # Seuil de score pour considérer un genre comme préféré
                ]
                
                # Si des genres préférés sont identifiés, filtrer les pistes en mémoire
                if preferred_genres:
                    # Conserver les pistes des genres préférés, plus un échantillon aléatoire d'autres genres
                    preferred_tracks = [t for t in all_tracks if t.get('genre') in preferred_genres]
                    other_tracks = [t for t in all_tracks if t.get('genre') not in preferred_genres]
                    
                    # Prendre un échantillon aléatoire des autres pistes
                    sample_size = min(20, len(other_tracks))
                    other_sample = random.sample(other_tracks, sample_size) if other_tracks else []
                    
                    # Combiner les pistes préférées et l'échantillon
                    all_tracks = preferred_tracks + other_sample
                    logger.info(f"Filtrage par genre: {len(preferred_tracks)} pistes de genres préférés + {len(other_sample)} autres")
            
        except Exception as e:
            logger.error(f"Erreur lors de la récupération des pistes: {str(e)}")
            logger.error(traceback.format_exc())
            
            # En cas d'erreur, utiliser une méthode de récupération encore plus simple
            try:
                # Récupérer un petit nombre de pistes sans filtre
                response = self.tracks_table.scan(Limit=50)
                all_tracks = response.get('Items', [])
                logger.info(f"Récupération de secours: {len(all_tracks)} pistes")
            except Exception as fe:
                logger.error(f"Échec de la récupération de secours: {str(fe)}")
                all_tracks = []
        
        # 4. Scorer et filtrer les pistes avec vérification du temps d'exécution
        scored_tracks = []
        current_track_count = 0
        
        for track in all_tracks:
            # Vérifier si on approche du timeout
            current_time = time.time()
            if current_time - start_time > max_execution_time:
                logger.warning(f"Temps d'exécution approchant le timeout - scoring interrompu après {current_track_count} pistes")
                break
                
            current_track_count += 1
            score = self.score_track(track, preferences)
            if score >= 0:
                scored_tracks.append((track, score))
        
        # 5. Ajouter un peu de diversité (stratégie d'exploration/exploitation)
        scored_tracks.sort(key=lambda x: x[1], reverse=True)
        
        # Sélectionner 80% des meilleures pistes basées sur le score
        top_count = int(min(len(scored_tracks), max_recommendations) * 0.8)
        top_tracks = [track for track, _ in scored_tracks[:top_count]]
        
        # Sélectionner 20% de pistes aléatoires parmi les autres (qui ont un score positif)
        remaining_tracks = [track for track, _ in scored_tracks[top_count:] if track not in top_tracks]
        random_tracks = random.sample(
            remaining_tracks, 
            min(max_recommendations - top_count, len(remaining_tracks))
        )
        
        # Combiner les deux ensembles
        recommended_tracks = top_tracks + random_tracks
        
        # Log pour debug
        logger.info(f"Génération de {len(recommended_tracks)} recommandations pour {user_id}")
        if recommended_tracks:
            for i, track in enumerate(recommended_tracks[:3]):
                logger.info(f"Top {i+1}: {track.get('title')} ({track.get('genre')})")
        
        return recommended_tracks

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
        
        # Utiliser le recommandeur amélioré
        recommender = ImprovedRecommender(tracks_table, users_table, swipes_table, likes_table)
        recommended_tracks = recommender.get_recommendations(user_id, MAX_RECOMMENDATIONS)
        
        # Si aucune recommandation n'est trouvée, essayer avec le recommandeur simple comme fallback
        if not recommended_tracks:
            logger.warning(f"Aucune recommandation trouvée avec l'algorithme amélioré. Tentative avec l'algorithme simple.")
            from original_recommender import SimpleRecommender
            simple_recommender = SimpleRecommender(tracks_table, users_table, swipes_table)
            recommended_tracks = simple_recommender.get_recommendations(user_id, MAX_RECOMMENDATIONS)
        
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
