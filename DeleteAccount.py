import json
import boto3
import logging
import os
from boto3.dynamodb.conditions import Key
import traceback

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Variables d'environnement
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')
USERS_TABLE = os.environ.get('USERS_TABLE', 'chordora-users')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'chordora-users')

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')
tracks_table = dynamodb.Table(TRACKS_TABLE)
users_table = dynamodb.Table(USERS_TABLE)
s3 = boto3.client('s3')

def get_cors_headers():
    """
    Renvoie les en-têtes CORS standard
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
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('Preflight request successful')
        }
    
    try:
        # Obtenir l'ID utilisateur du corps de la requête ou du token
        request_body = {}
        if 'body' in event and event['body']:
            try:
                request_body = json.loads(event['body'])
            except json.JSONDecodeError:
                logger.error("Erreur lors du parsing du corps de la requête JSON")
        
        user_id = request_body.get('userId')
        
        # Si l'ID n'est pas dans le corps, essayer de l'obtenir du token d'authentification
        if not user_id and 'requestContext' in event and 'authorizer' in event['requestContext']:
            user_id = event['requestContext']['authorizer']['claims']['sub']
        
        if not user_id:
            logger.error("ID utilisateur non fourni")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'User ID is required'})
            }
        
        logger.info(f"Nettoyage des données pour l'utilisateur: {user_id}")
        
        # 1. Supprimer toutes les pistes de l'utilisateur
        delete_tracks_result = delete_user_tracks(user_id)
        
        # 2. Supprimer le profil utilisateur de DynamoDB si le compte Cognito est aussi supprimé
        # Nous gardons cette ligne en commentaire car la suppression du profil se fera uniquement
        # si la suppression Cognito réussit, ce qui sera fait côté client
        # delete_profile_result = delete_user_profile(user_id)
        
        # 3. Supprimer tous les fichiers liés à l'utilisateur dans S3
        delete_files_result = delete_user_files(user_id)
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'message': f'User data cleaned successfully for user {user_id}',
                'tracksDeleted': delete_tracks_result,
                'filesDeleted': delete_files_result
            })
        }
    
    except Exception as e:
        logger.error(f"Erreur non gérée: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error cleaning user data: {str(e)}'})
        }

def delete_user_tracks(user_id):
    """
    Supprime toutes les pistes appartenant à l'utilisateur
    """
    try:
        # Utiliser l'index secondaire global pour trouver toutes les pistes de l'utilisateur
        response = tracks_table.query(
            IndexName='user_id-index',
            KeyConditionExpression=Key('user_id').eq(user_id)
        )
        
        tracks = response.get('Items', [])
        logger.info(f"Nombre de pistes à supprimer: {len(tracks)}")
        
        # Supprimer chaque piste
        deleted_count = 0
        file_paths = []
        
        for track in tracks:
            try:
                # Collecter les chemins de fichiers pour une suppression ultérieure
                if 'file_path' in track:
                    file_paths.append(track['file_path'])
                
                # Supprimer l'entrée de la base de données
                tracks_table.delete_item(Key={'track_id': track['track_id']})
                deleted_count += 1
            except Exception as track_error:
                logger.error(f"Erreur lors de la suppression de la piste {track.get('track_id')}: {str(track_error)}")
        
        # Supprimer les fichiers audio dans S3
        for file_path in file_paths:
            try:
                s3.delete_object(
                    Bucket=BUCKET_NAME,
                    Key=file_path
                )
                logger.info(f"Fichier S3 supprimé: {file_path}")
            except Exception as s3_error:
                logger.error(f"Erreur lors de la suppression du fichier S3 {file_path}: {str(s3_error)}")
        
        return {
            'tracksFound': len(tracks),
            'tracksDeleted': deleted_count,
            'filesDeleted': len(file_paths)
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la suppression des pistes: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'error': str(e)
        }

def delete_user_profile(user_id):
    """
    Supprime le profil utilisateur de DynamoDB
    """
    try:
        # Vérifier si le profil existe
        response = users_table.get_item(Key={'userId': user_id})
        
        if 'Item' not in response:
            logger.info(f"Profil utilisateur introuvable pour {user_id}")
            return {'message': 'User profile not found'}
        
        # Supprimer le profil
        users_table.delete_item(Key={'userId': user_id})
        logger.info(f"Profil utilisateur supprimé pour {user_id}")
        
        return {'message': 'User profile deleted successfully'}
    
    except Exception as e:
        logger.error(f"Erreur lors de la suppression du profil: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'error': str(e)
        }

def delete_user_files(user_id):
    """
    Supprime tous les fichiers liés à l'utilisateur dans le bucket S3
    """
    try:
        # Préfixe pour les fichiers de profil
        profile_prefix = f"public/users/{user_id}/"
        
        # Lister les objets à supprimer
        objects_to_delete = []
        
        # Lister les objets de profil
        profile_response = s3.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=profile_prefix
        )
        
        if 'Contents' in profile_response:
            for obj in profile_response['Contents']:
                objects_to_delete.append({'Key': obj['Key']})
        
        # Si des objets à supprimer ont été trouvés, les supprimer par lots
        total_deleted = 0
        
        if objects_to_delete:
            # AWS limite à 1000 objets par demande de suppression
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i:i+1000]
                s3.delete_objects(
                    Bucket=BUCKET_NAME,
                    Delete={
                        'Objects': batch,
                        'Quiet': True
                    }
                )
                total_deleted += len(batch)
        
        logger.info(f"Nombre total de fichiers supprimés: {total_deleted}")
        
        return {
            'filesDeleted': total_deleted
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la suppression des fichiers: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'error': str(e)
        }