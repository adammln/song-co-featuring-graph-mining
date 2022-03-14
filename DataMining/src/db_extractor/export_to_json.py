import json
import time
from tqdm import tqdm
from google.cloud import firestore
import os
from pathlib import Path

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credentials/YOUR_GOOGLE_APPLICATION_CREDENTIALS_FILE.json'

db = firestore.Client(
    project="spotify-co-featuring2gra-34f8c"
)

MODE = 'staging'
collection_path = [MODE+'-exceeds_dump', MODE+'-artist_data']

def to_json(collections_path):
    for path in collections_path:
        artist_collection_ref = db.collection(path)
        collections = artist_collection_ref.get()
        root = {'collections':{}}
        folder = './data/raw/'
        filepath = folder+path+'_v3_1000.json'
        my_file = Path(filepath)
        # reset or create file
        open(filepath, 'w').close()
        print(f'preparing {filepath}...')
        for doc in tqdm(collections):
            data = doc.to_dict()
            artist_id = doc.id
            root['collections'][artist_id] = data
            # to_write.append(payload)
            # Reset status
        print(f'{len(root["collections"])} of documents to be written in a file...')
        with open(filepath, 'a', encoding='utf-8') as f:
            json.dump(root, f, ensure_ascii=False, indent=4)
        print(f'DONE writing {filepath}!')
to_json(collection_path)

