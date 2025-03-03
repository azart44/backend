import json
import boto3
import os
import uuid
import datetime
from boto3.dynamodb.conditions import Key, Attr
import logging
from decimal import Decimal
import traceback

# Configuration du logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Classe pour l'encodage des décimaux en JSON
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

# Fonction pour obtenir les en-têtes CORS
def get_cors_headers():
    return {
        'Access-Control-Allow-Origin': 'http://localhost:3000',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS',
        'Access-Control-Allow-Credentials': 'true'
    }

# Initialisation des clients AWS
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Variables d'environnement
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'chordora-users')
TRACKS_TABLE = os.environ.get('TRACKS_TABLE', 'chordora-tracks')

def lambda_handler(event, context):
    logger.info(f"Received event: {json.dumps(event)}")
    
    http_method = event['httpMethod']
    cors_headers = get_cors_headers()
    
    if http_method == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps('OK')
        }
    
    try:
        # Vérification de l'authentification
        if 'requestContext' not in event or 'authorizer' not in event['requestContext'] or 'claims' not in event['requestContext']['authorizer']:
            logger.error("Informations d'authentification manquantes")
            return {
                'statusCode': 401,
                'headers': cors_headers,
                'body': json.dumps('Unauthorized: Missing authentication information')
            }
        
        user_id = event['requestContext']['authorizer']['claims']['sub']
        logger.info(f"User ID extracted: {user_id}")
    except Exception as auth_error:
        logger.error(f"Error extracting authentication: {str(auth_error)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 401,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Authentication error: {str(auth_error)}'})
        }
    
    try:
        if http_method == 'GET':
            if 'pathParameters' in event and event['pathParameters'] and 'trackId' in event['pathParameters']:
                return handle_get_track(event, user_id, cors_headers)
            else:
                return handle_get_all_tracks(event, user_id, cors_headers)
        elif http_method == 'POST':
            return handle_post(event, user_id, cors_headers)
        elif http_method == 'PUT':
            return handle_put(event, user_id, cors_headers)
        elif http_method == 'DELETE':
            return handle_delete(event, user_id, cors_headers)
        else:
            logger.warning(f"Unsupported HTTP method: {http_method}")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Unsupported HTTP method'})
            }
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }

def handle_get_all_tracks(event, user_id, cors_headers):
    logger.info(f"Handling GET request for all tracks, user_id: {user_id}")
    try:
        table = dynamodb.Table(TRACKS_TABLE)
        response = table.scan(
            FilterExpression=Attr('user_id').eq(user_id)
        )
        
        tracks = response.get('Items', [])
        logger.info(f"Found {len(tracks)} tracks for user {user_id}")
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(tracks, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Error in handle_get_all_tracks: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving tracks: {str(e)}'})
        }

def handle_get_track(event, user_id, cors_headers):
    logger.info("Handling GET request for a specific track")
    try:
        track_id = event['pathParameters']['trackId']
        
        table = dynamodb.Table(TRACKS_TABLE)
        response = table.get_item(Key={'track_id': track_id})
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Track not found'})
            }
        
        track = response['Item']
        
        if track['user_id'] != user_id:
            return {
                'statusCode': 403,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Not authorized to access this track'})
            }
        
        presigned_url = s3.generate_presigned_url('get_object',
                                                  Params={'Bucket': BUCKET_NAME,
                                                          'Key': track['file_path']},
                                                  ExpiresIn=3600)
        
        track_info = {**track, 'presigned_url': presigned_url}
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(track_info, cls=DecimalEncoder)
        }
    except Exception as e:
        logger.error(f"Error in handle_get_track: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error retrieving track: {str(e)}'})
        }

def handle_post(event, user_id, cors_headers):
    logger.info("Handling POST request")
    try:
        # Vérification du corps de la requête
        if 'body' not in event or not event['body']:
            logger.error("Missing request body")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Missing request body'})
            }
        
        try:
            body = json.loads(event['body'])
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON: {str(e)}")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Invalid JSON in request body'})
            }
        
        # Vérification des champs requis
        required_fields = ['fileName', 'fileType', 'title', 'genre', 'bpm']
        missing_fields = [field for field in required_fields if field not in body]
        
        if missing_fields:
            logger.error(f"Missing required fields: {missing_fields}")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': f'Missing required fields: {missing_fields}'})
            }
        
        file_name = body['fileName']
        file_type = body['fileType']
        title = body['title']
        genre = body['genre']
        
        # Conversion et validation du BPM
        try:
            bpm = int(body['bpm'])
            if bpm <= 0:
                logger.error(f"Invalid BPM value: {bpm}")
                return {
                    'statusCode': 400,
                    'headers': cors_headers,
                    'body': json.dumps({'message': 'BPM must be a positive number'})
                }
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing BPM: {str(e)}")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'BPM must be a valid number'})
            }
        
        # Génération d'un ID unique pour la piste
        track_id = str(uuid.uuid4())
        
        # Construction du chemin S3
        # Format de chemin simplifié
        s3_key = f"tracks/{track_id}/{file_name}"
        logger.info(f"S3 key for new track: {s3_key}")
        
        # Génération de l'URL présignée pour l'upload
        try:
            presigned_url = s3.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': BUCKET_NAME,
                    'Key': s3_key,
                    'ContentType': file_type
                },
                ExpiresIn=3600
            )
            logger.info(f"Generated presigned URL (truncated): {presigned_url[:50]}...")
        except Exception as s3_error:
            logger.error(f"Error generating presigned URL: {str(s3_error)}")
            logger.error(traceback.format_exc())
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'message': f'Error generating upload URL: {str(s3_error)}'})
            }
        
        # Enregistrement des métadonnées dans DynamoDB
        try:
            table = dynamodb.Table(TRACKS_TABLE)
            timestamp = int(datetime.datetime.now().timestamp())
            
            # Création de l'objet track
            track_item = {
                'track_id': track_id,
                'user_id': user_id,
                'title': title,
                'genre': genre,
                'bpm': bpm,
                'file_path': s3_key,
                'created_at': timestamp,
                'updated_at': timestamp,
                'isPrivate': body.get('isPrivate', False)
            }
            
            # Ajout des champs optionnels
            if 'description' in body:
                track_item['description'] = body['description']
            
            if 'tags' in body and isinstance(body['tags'], list):
                track_item['tags'] = body['tags']
            
            # Enregistrement dans DynamoDB
            table.put_item(Item=track_item)
            logger.info(f"Track metadata saved to DynamoDB, track_id: {track_id}")
            
            # Réponse avec l'URL d'upload et l'ID de la piste
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'trackId': track_id,
                    'uploadUrl': presigned_url
                })
            }
        except Exception as db_error:
            logger.error(f"Error saving track metadata: {str(db_error)}")
            logger.error(traceback.format_exc())
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({'message': f'Error saving track metadata: {str(db_error)}'})
            }
            
    except Exception as e:
        logger.error(f"Error in handle_post: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error creating track: {str(e)}'})
        }

def handle_put(event, user_id, cors_headers):
    logger.info("Handling PUT request")
    try:
        # Vérification de l'ID de piste
        if 'pathParameters' not in event or not event['pathParameters'] or 'trackId' not in event['pathParameters']:
            logger.error("Missing trackId in path parameters")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Missing trackId in path parameters'})
            }
        
        track_id = event['pathParameters']['trackId']
        
        # Vérification du corps de la requête
        if 'body' not in event or not event['body']:
            logger.error("Missing request body")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Missing request body'})
            }
        
        try:
            body = json.loads(event['body'])
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON: {str(e)}")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Invalid JSON in request body'})
            }
        
        # Vérifier si la piste existe et appartient à l'utilisateur
        table = dynamodb.Table(TRACKS_TABLE)
        response = table.get_item(Key={'track_id': track_id})
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Track not found'})
            }
        
        track = response['Item']
        
        if track['user_id'] != user_id:
            return {
                'statusCode': 403,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Not authorized to modify this track'})
            }
        
        # Préparer les mises à jour
        update_expression = "SET updated_at = :updated_at"
        expression_attribute_values = {
            ':updated_at': int(datetime.datetime.now().timestamp())
        }
        
        # Ajouter chaque champ à mettre à jour
        for key, value in body.items():
            if key not in ['track_id', 'user_id', 'file_path', 'created_at']:  # Champs qu'on ne veut pas modifier
                update_expression += f", {key} = :{key}"
                expression_attribute_values[f':{key}'] = value
        
        # Effectuer la mise à jour
        table.update_item(
            Key={'track_id': track_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values
        )
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({'message': 'Track updated successfully'})
        }
    except Exception as e:
        logger.error(f"Error in handle_put: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error updating track: {str(e)}'})
        }

def handle_delete(event, user_id, cors_headers):
    logger.info("Handling DELETE request")
    try:
        # Vérification de l'ID de piste
        if 'pathParameters' not in event or not event['pathParameters'] or 'trackId' not in event['pathParameters']:
            logger.error("Missing trackId in path parameters")
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Missing trackId in path parameters'})
            }
        
        track_id = event['pathParameters']['trackId']
        
        # Vérifier si la piste existe et appartient à l'utilisateur
        table = dynamodb.Table(TRACKS_TABLE)
        response = table.get_item(Key={'track_id': track_id})
        
        if 'Item' not in response:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Track not found'})
            }
        
        track = response['Item']
        
        if track['user_id'] != user_id:
            return {
                'statusCode': 403,
                'headers': cors_headers,
                'body': json.dumps({'message': 'Not authorized to delete this track'})
            }
        
        # Supprimer le fichier de S3
        try:
            s3.delete_object(
                Bucket=BUCKET_NAME,
                Key=track['file_path']
            )
            logger.info(f"S3 object deleted: {track['file_path']}")
        except Exception as s3_error:
            logger.error(f"Error deleting S3 object: {str(s3_error)}")
            # Continuer malgré l'erreur S3 pour au moins supprimer l'entrée de la base de données
        
        # Supprimer l'entrée de DynamoDB
        table.delete_item(
            Key={'track_id': track_id}
        )
        logger.info(f"Track deleted from DynamoDB: {track_id}")
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({'message': 'Track deleted successfully'})
        }
    except Exception as e:
        logger.error(f"Error in handle_delete: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'message': f'Error deleting track: {str(e)}'})
        }