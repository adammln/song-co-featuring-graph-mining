import time
from tqdm import tqdm
from google.cloud import firestore
import os
from db_api import FirebaseApi

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credentials/YOUR_GOOGLE_APPLICATION_CREDENTIALS_FILE.json'

db = firestore.Client(
    project="spotify-co-featuring2gra-34f8c"
)
transaction = db.transaction()
prod_queue_doc_ref = db.collection('dev-dashboard').document('queue')
prod_artist_data_coll_ref = db.collection('dev-artist_data')
dashboard_coll_ref = db.collection('dev-dashboard')
exceed_coll_ref = db.collection('dev-exceeds_dump')

rtdb = FirebaseApi()

def migrate_artist_data():
    # get all artist_data push ids:
    print("getting id pairs...")
    pairs = rtdb.get_pairs()
    print("Starting migration...")
    for artist_id, push_id in tqdm(pairs.items()):
        artist_data = rtdb.get_artist_data_by_push_id(push_id)[0]
        artist_data['collaborators'].remove('!')
        artist_data['genre'].remove('!')
        payload = {
            'collaborators': artist_data['collaborators'],
            'followers': artist_data['followers'],
            'genres': artist_data['genre'],
            'is_traversed': False,
            'name': artist_data['name'],
            'playlist': artist_data['playlist'],
            'popularity': artist_data['popularity']
        }
        doc_ref = prod_artist_data_coll_ref.document(artist_id)
        doc_ref.set(payload)
        time.sleep(0.2)
    print("artist_data migration done!")

def migrate_queue():
    print("getting queue...")
    queue = rtdb.get_queue()
    payload = {'ids':queue}
    print("Migrating queue...")
    prod_queue_doc_ref.set(payload)
    print("Done!")

def generate_dashboard():
    dashboard_coll_ref.document('in_process').set(
        {'ids':[]}
    )
    dashboard_coll_ref.document('summary').set(
        {
            'total_in_dump': 0,
            'total_in_process': 0,
            'total_in_queue': 301,
            'total_nodes': 301,
            'total_nodes_traversed': 0,
            'total_nodes_with_enough_degree':0
        }
    )

def create_exceeding_node_dump():
    exceed_coll_ref.document('xx').set({'test':1})


if __name__ == "__main__":
    migrate_artist_data()
    migrate_queue()
    generate_dashboard()
    create_exceeding_node_dump()