import json
import boto3
import logging
import traceback
import os
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
from datetime import datetime, timedelta

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# Variables d'environnement
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')
USERS_TABLE = os.environ.get('USERS_TABLE', 'chordora-users')
SWIPES_TABLE = os.environ.get('SWIPES_TABLE', 'chordora-beat-swipes')
S3_BUCKET = os.environ.get('S3_BUCKET', 'chordora-tracks')
MAX_RECOMMENDATIONS = int(os.environ.get('MAX_RECOMMENDATIONS', '20'))

# Tables DynamoDB
tracks_table = dynamodb.Table(TRACKS_TABLE)
users_table = dynamodb.Table(USERS_TABLE)
swipes_table = dynamodb.Table(SWIPES_TABLE)

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def generate_presigned_url(bucket, key, expiration=3600):
    """
    Génère une URL présignée pour un fichier S3
    """
    try:
        url = s3.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket,
                'Key': key
            },
            ExpiresIn=expiration
        )
        return url
    except Exception as e:
        logger.error(f"Erreur de génération d'URL présignée: {str(e)}")
        return None

def get_cors_headers(event):
    """
    Génère les en-têtes CORS dynamiques
    """
    # En développement, autoriser toutes les origines
    # En production, remplacez par votre domaine exact
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

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
        
        # Ajouter des URLs présignées
        for track in recommended_tracks:
            if track.get('file_path'):
                track['presigned_url'] = generate_presigned_url(
                    S3_BUCKET, 
                    track['file_path']
                )
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'tracks': recommended_tracks,
                'count': len(recommended_tracks)
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