import json
import boto3
import logging
import uuid
import os
import datetime
from decimal import Decimal
from boto3.dynamodb.conditions import Key

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Variables d'environnement
PLAYLISTS_TABLE = os.environ.get('PLAYLISTS_TABLE', 'chordora-playlists')
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')

# Initialisation des clients AWS
dynamodb = boto3.resource('dynamodb')
playlists_table = dynamodb.Table(PLAYLISTS_TABLE)
tracks_table = dynamodb.Table(TRACKS_TABLE)

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
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
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
        # Vérification de l'authentification
        if 'requestContext' not in event or 'authorizer' not in event['requestContext'] or 'claims' not in event['requestContext']['authorizer']:
            logger.error("Informations d'authentification manquantes")
            return {
                'statusCode': 401,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Unauthorized: Missing authentication information'})
            }
        
        user_id = event['requestContext']['authorizer']['claims']['sub']
        logger.info(f"User ID extrait: {user_id}")
        
        http_method = event['httpMethod']
        
        # CREATE et UPDATE
        if http_method in ['POST', 'PUT']:
            return handle_create_update_playlist(event, user_id, cors_headers)
        # DELETE
        elif http_method == 'DELETE':
            return handle_delete_playlist(event, user_id, cors_headers)
        else:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': f'Méthode HTTP non supportée: {http_method}'})
            }
    
    except Exception as e:
        logger.error(f"Erreur non gérée: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }

def handle_create_update_playlist(event, user_id, cors_headers):
    """Gère la création et la mise à jour d'une playlist"""
    try:
        # Vérification du corps de la requête
        if 'body' not in event or not event['body']:
            logger.error("Corps de la requête manquant")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Missing request body'})
            }
        
        # Analyse du corps de la requête
        try:
            body = json.loads(event['body'])
        except json.JSONDecodeError as e:
            logger.error(f"Erreur d'analyse JSON: {str(e)}")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Invalid JSON in request body'})
            }
        
        # Déterminer s'il s'agit d'une création ou d'une mise à jour
        is_update = 'playlist_id' in body and body['playlist_id']
        
        # Validation du titre
        if 'title' not in body or not body['title']:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Playlist title is required'})
            }
        
        # Timestamp actuel
        timestamp = int(datetime.datetime.now().timestamp())
        
        if is_update:
            # MISE À JOUR
            playlist_id = body['playlist_id']
            
            # Vérifier si la playlist existe et appartient à l'utilisateur
            response = playlists_table.get_item(Key={'playlist_id': playlist_id})
            
            if 'Item' not in response:
                return {
                    'statusCode': 404,
                    'headers': cors_headers,
                    'body': json.dumps({'message': 'Playlist not found'})
                }
            
            playlist = response['Item']
            
            if playlist['user_id'] != user_id:
                return {
                    'statusCode': 403,
                    'headers': cors_headers,
                    'body': json.dumps({'message': 'Not authorized to update this playlist'})
                }
            
            # Mise à jour des données
            track_ids = []
            track_positions = {}
            
            if 'tracks' in body and isinstance(body['tracks'], list):
                # Vérifier et ajouter chaque piste
                for i, track_data in enumerate(body['tracks']):
                    if isinstance(track_data, dict) and 'track_id' in track_data:
                        track_id = track_data['track_id']
                        
                        # Vérifier si la piste existe et appartient à l'utilisateur
                        try:
                            track_response = tracks_table.get_item(Key={'track_id': track_id})
                            if 'Item' in track_response:
                                track = track_response['Item']
                                
                                # Vérification critique: la piste doit appartenir à l'utilisateur
                                if track.get('user_id') != user_id:
                                    return {
                                        'statusCode': 403,
                                        'headers': cors_headers,
                                        'body': json.dumps({'message': 'Vous ne pouvez ajouter que vos propres pistes à vos playlists'})
                                    }
                                
                                # Si la vérification est réussie, ajouter la piste
                                track_ids.append(track_id)
                                track_positions[track_id] = i
                        except Exception as e:
                            logger.error(f"Erreur lors de la vérification de la piste {track_id}: {str(e)}")
                            # Continuer malgré l'erreur
            
            # Mettre à jour la playlist
            playlist_update = {
                'title': body['title'],
                'description': body.get('description', ''),
                'is_public': body.get('is_public', True),
                'updated_at': timestamp,
                'track_count': len(track_ids)
            }
            
            # Ajouter l'URL de l'image de couverture si fournie
            if 'cover_image_url' in body:
                playlist_update['cover_image_url'] = body['cover_image_url']
            
            # Ajouter les pistes si fournies
            if track_ids:
                playlist_update['track_ids'] = track_ids
                playlist_update['track_positions'] = track_positions
            
            # Construire l'expression de mise à jour
            update_expression = "SET "
            expression_values = {}
            
            for key, value in playlist_update.items():
                update_expression += f"{key} = :{key.replace('_', '')}, "
                expression_values[f":{key.replace('_', '')}"] = value
            
            # Supprimer la virgule finale
            update_expression = update_expression[:-2]
            
            # Mettre à jour la playlist
            playlists_table.update_item(
                Key={'playlist_id': playlist_id},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values
            )
            
            # Récupérer la playlist mise à jour
            updated_response = playlists_table.get_item(Key={'playlist_id': playlist_id})
            updated_playlist = updated_response.get('Item', {})
            
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'message': 'Playlist updated successfully',
                    'playlist': updated_playlist
                }, cls=DecimalEncoder)
            }
        else:
            # CRÉATION
            # Génération de l'ID de la playlist
            playlist_id = str(uuid.uuid4())
            
            # Préparer les données des pistes
            track_ids = []
            track_positions = {}
            
            if 'tracks' in body and isinstance(body['tracks'], list):
                # Vérifier et ajouter chaque piste
                for i, track_data in enumerate(body['tracks']):
                    if isinstance(track_data, dict) and 'track_id' in track_data:
                        track_id = track_data['track_id']
                        
                        # Vérifier si la piste existe et appartient à l'utilisateur
                        try:
                            track_response = tracks_table.get_item(Key={'track_id': track_id})
                            if 'Item' in track_response:
                                track = track_response['Item']
                                
                                # Vérification critique: la piste doit appartenir à l'utilisateur
                                if track.get('user_id') != user_id:
                                    return {
                                        'statusCode': 403,
                                        'headers': cors_headers,
                                        'body': json.dumps({'message': 'Vous ne pouvez ajouter que vos propres pistes à vos playlists'})
                                    }
                                
                                # Si la vérification est réussie, ajouter la piste
                                track_ids.append(track_id)
                                track_positions[track_id] = i
                        except Exception as e:
                            logger.error(f"Erreur lors de la vérification de la piste {track_id}: {str(e)}")
                            # Continuer malgré l'erreur
            
            # Création de l'objet playlist
            playlist = {
                'playlist_id': playlist_id,
                'user_id': user_id,
                'title': body['title'],
                'description': body.get('description', ''),
                'is_public': body.get('is_public', True),
                'cover_image_url': body.get('cover_image_url', ''),
                'created_at': timestamp,
                'updated_at': timestamp,
                'track_count': len(track_ids),
                'track_ids': track_ids,
                'track_positions': track_positions
            }
            
            # Enregistrement dans DynamoDB
            playlists_table.put_item(Item=playlist)
            
            return {
                'statusCode': 201,
                'headers': cors_headers,
                'body': json.dumps({
                    'message': 'Playlist created successfully',
                    'playlist': playlist
                }, cls=DecimalEncoder)
            }
    
    except Exception as e:
        logger.error(f"Erreur lors de la création/mise à jour de la playlist: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error processing playlist: {str(e)}'})
        }

def handle_delete_playlist(event, user_id, cors_headers):
    """Gère la suppression d'une playlist"""
    try:
        # Vérification des paramètres de chemin
        if 'pathParameters' not in event or not event['pathParameters'] or 'playlistId' not in event['pathParameters']:
            logger.error("ID de playlist manquant")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Missing playlist ID'})
            }
        
        playlist_id = event['pathParameters']['playlistId']
        logger.info(f"Suppression de la playlist: {playlist_id}")
        
        # Vérifier si la playlist existe et appartient à l'utilisateur
        response = playlists_table.get_item(Key={'playlist_id': playlist_id})
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Playlist not found'})
            }
        
        playlist = response['Item']
        
        if playlist['user_id'] != user_id:
            return {
                'statusCode': 403,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Not authorized to delete this playlist'})
            }
        
        # Supprimer la playlist
        playlists_table.delete_item(Key={'playlist_id': playlist_id})
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'message': 'Playlist deleted successfully'
            })
        }
    
    except Exception as e:
        logger.error(f"Erreur lors de la suppression de la playlist: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error deleting playlist: {str(e)}'})
        }