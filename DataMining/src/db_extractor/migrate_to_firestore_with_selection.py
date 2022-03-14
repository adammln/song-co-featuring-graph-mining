import time
from tqdm import tqdm
from google.cloud import firestore
import os
from rtdb_api import FirebaseApi

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credentials/YOUR_GOOGLE_APPLICATION_CREDENTIALS_FILE.json'

db = firestore.Client(
    project="spotify-co-featuring2gra-34f8c"
)
transaction = db.transaction()
prod_queue_doc_ref = db.collection('staging-dashboard').document('queue')
prod_artist_data_coll_ref = db.collection('staging-artist_data')
dashboard_coll_ref = db.collection('staging-dashboard')
exceed_coll_ref = db.collection('staging-exceeds_dump')
log_coll_ref = db.collection('staging-log')

rtdb = FirebaseApi()
excluded_ids = []
def migrate_artist_data():
    # get all artist_data push ids:
    print("getting id pairs...")
    pairs = rtdb.get_pairs()
    print("Starting migration...")
    exclusion = ['Top Hits of 2014', 'Top Hits of 2015']
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
        if artist_data['playlist'] not in exclusion:
            doc_ref = prod_artist_data_coll_ref.document(artist_id)
            doc_ref.set(payload)
        else:
            excluded_ids.append(artist_id)
            continue
        time.sleep(0.1)
    print("artist_data migration done!")

def migrate_queue():
    print("getting queue...")
    queue = rtdb.get_queue()
    queue_with_exlusion = list(set(queue) - set(excluded_ids))
    payload = {'ids':queue_with_exlusion}
    print("Migrating queue...")
    prod_queue_doc_ref.set(payload)
    print("Done!")

def generate_dashboard():
    dashboard_coll_ref.document('total_in_dump').set({'value':0})
    dashboard_coll_ref.document('total_nodes_traversed').set({'value':0})
    dashboard_coll_ref.document('total_in_queue').set({'value':132})
    dashboard_coll_ref.document('total_nodes').set({'value':132})
    dashboard_coll_ref.document('total_nodes_with_enough_degree').set({'value':0})

def create_exceeding_node_dump_and_log():
    exceed_coll_ref.document('xx').set({'test':1})
    log_coll_ref.document('xx').set({'test':1})


if __name__ == "__main__":
    migrate_artist_data()
    migrate_queue()
    generate_dashboard()
    create_exceeding_node_dump_and_log()