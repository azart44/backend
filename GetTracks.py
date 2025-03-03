import json
import boto3
import logging
from decimal import Decimal
import os
from boto3.dynamodb.conditions import Attr
import traceback

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Variables d'environnement
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-users')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'chordora-tracks')

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')
tracks_table = dynamodb.Table(TRACKS_TABLE)
s3 = boto3.client('s3')

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def get_cors_headers():
    """
    Renvoie les en-têtes CORS standard qui fonctionnent pour la plupart des cas
    """
    return {
        'Access-Control-Allow-Origin': 'http://localhost:3000',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

def lambda_handler(event, context):
    logger.info(f"Événement reçu: {json.dumps(event)}")
    cors_headers = get_cors_headers()
    
    # Gestion des requêtes OPTIONS (pre-flight CORS)
    if event.get('httpMethod') == 'OPTIONS':
        logger.info("Requête OPTIONS reçue")
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('Preflight request successful')
        }
    
    try:
        # Tentative d'extraction de l'ID utilisateur (si authentifié)
        is_authenticated = False
        user_id = None
        
        try:
            if 'requestContext' in event and 'authorizer' in event['requestContext'] and 'claims' in event['requestContext']['authorizer']:
                user_id = event['requestContext']['authorizer']['claims']['sub']
                is_authenticated = True
                logger.info(f"Utilisateur authentifié: {user_id}")
            else:
                logger.info("Authentification non trouvée dans la requête")
        except Exception as auth_error:
            logger.error(f"Erreur lors de l'extraction des informations d'authentification: {str(auth_error)}")
            logger.error(traceback.format_exc())
            # Continuer en tant qu'utilisateur non authentifié
        
        # Traitement différent selon la méthode HTTP
        if event.get('httpMethod') == 'GET':
            if event.get('pathParameters') and 'trackId' in event.get('pathParameters', {}):
                logger.info(f"Récupération d'une piste spécifique: {event['pathParameters']['trackId']}")
                return get_track_by_id(event, cors_headers, user_id, is_authenticated)
            else:
                logger.info("Récupération de toutes les pistes")
                return get_all_tracks(event, cors_headers, user_id, is_authenticated)
        else:
            logger.warning(f"Méthode HTTP non prise en charge: {event.get('httpMethod')}")
            return {
                'statusCode': 405,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Method Not Allowed'})
            }
    except Exception as e:
        # Capture et log toutes les exceptions non gérées
        logger.error(f"Erreur non gérée: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }

def get_all_tracks(event, cors_headers, user_id, is_authenticated):
    """Récupère toutes les pistes, filtrées par utilisateur si spécifié"""
    try:
        # Obtenir les paramètres de requête, avec gestion des cas où ils sont absents
        query_params = {}
        if 'queryStringParameters' in event and event['queryStringParameters']:
            query_params = event['queryStringParameters']
        
        # Filtrage par utilisateur ou genre
        target_user_id = None
        genre = None
        
        if query_params:
            target_user_id = query_params.get('userId')
            genre = query_params.get('genre')
        
        logger.info(f"Paramètres: userId={target_user_id}, genre={genre}")
        
        filter_expression = None
        
        # Si un userId est spécifié, filtrer par cet ID
        if target_user_id:
            filter_expression = Attr('user_id').eq(target_user_id)
            logger.info(f"Filtrage par utilisateur: {target_user_id}")
        
        # Si un genre est spécifié, ajouter ce filtre
        if genre:
            genre_filter = Attr('genre').eq(genre)
            filter_expression = genre_filter if filter_expression is None else filter_expression & genre_filter
            logger.info(f"Filtrage par genre: {genre}")
        
        # Si l'utilisateur n'est pas authentifié, ne montrer que les pistes publiques
        if not is_authenticated:
            public_filter = Attr('isPrivate').ne(True)
            filter_expression = public_filter if filter_expression is None else filter_expression & public_filter
            logger.info("Utilisateur non authentifié, filtrage des pistes privées")
        
        # Exécuter la requête avec les filtres
        scan_params = {}
        if filter_expression:
            scan_params['FilterExpression'] = filter_expression
        
        logger.info(f"Paramètres de scan: {scan_params}")
        response = tracks_table.scan(**scan_params)
        tracks = response.get('Items', [])
        
        # Si l'utilisateur est authentifié, s'assurer qu'il ne peut voir les pistes privées que si elles lui appartiennent
        if is_authenticated and not target_user_id:
            tracks = [track for track in tracks if not track.get('isPrivate', False) or track.get('user_id') == user_id]
            logger.info(f"Filtrage post-query des pistes privées pour l'utilisateur authentifié")
        
        logger.info(f"Nombre de pistes trouvées: {len(tracks)}")
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(tracks, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des pistes: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving tracks: {str(e)}'})
        }

def get_track_by_id(event, cors_headers, user_id, is_authenticated):
    """Récupère une piste spécifique par son ID et génère une URL présignée"""
    try:
        track_id = event['pathParameters']['trackId']
        logger.info(f"Récupération de la piste avec l'ID: {track_id}")
        
        response = tracks_table.get_item(Key={'track_id': track_id})
        
        if 'Item' not in response:
            logger.info(f"Piste non trouvée: {track_id}")
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Track not found'})
            }
        
        track = response['Item']
        logger.info(f"Piste trouvée: {track_id}, appartenant à: {track.get('user_id')}")
        
        # Vérifier les autorisations pour les pistes privées
        if track.get('isPrivate', False) and (not is_authenticated or track['user_id'] != user_id):
            logger.warning(f"Accès non autorisé à la piste privée: {track_id}")
            return {
                'statusCode': 403,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Not authorized to access this track'})
            }
        
        # Générer une URL présignée pour écouter la piste
        if 'file_path' in track:
            logger.info(f"Génération de l'URL présignée pour: {track['file_path']}")
            try:
                presigned_url = s3.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': BUCKET_NAME, 'Key': track['file_path']},
                    ExpiresIn=3600
                )
                track_info = {**track, 'presigned_url': presigned_url}
            except Exception as s3_error:
                logger.error(f"Erreur lors de la génération de l'URL présignée: {str(s3_error)}")
                logger.error(traceback.format_exc())
                # Continuer sans URL présignée
                track_info = {**track, 'error': 'Could not generate presigned URL'}
        else:
            logger.warning(f"Chemin de fichier manquant pour la piste: {track_id}")
            track_info = {**track, 'error': 'Missing file path'}
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(track_info, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Erreur lors de la récupération de la piste: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving track: {str(e)}'})
        }