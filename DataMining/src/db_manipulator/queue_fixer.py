import time
from tqdm import tqdm
from google.cloud import firestore
import os
# from rtdb_api import FirebaseApi

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credentials/spotify-co-featuring2gra-34f8c-firebase-adminsdk-k0pe2-bfcd86823f.json'

db = firestore.Client(
    project="spotify-co-featuring2gra-34f8c"
)
# transaction = db.transaction()
queue_doc_ref = db.collection('staging-dashboard').document('queue')
artist_collection_ref = db.collection('staging-artist_data')

collections = artist_collection_ref.get()
payloads = {}
queue = []
for doc in collections:
    payload = doc.to_dict()
    artist_id = doc.id
    # Reset status
    already_traversed = payload['is_traversed']
    if not already_traversed:
        queue.append(artist_id)

# def update_transaction(transaction, queue)
queue_doc_ref.update({'ids':queue})