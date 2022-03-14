import time
from tqdm import tqdm
from google.cloud import firestore
import os
from rtdb_api import FirebaseApi

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credentials/YOUR_GOOGLE_APPLICATION_CREDENTIALS_FILE.json'

db = firestore.Client(
    project="your-google-cloud-project-id"
)
transaction = db.transaction()
# prod_queue_doc_ref = db.collection('prod1-dashboard').document('queue')
prod_artist_data_coll_ref = db.collection('prod1-artist_data')
# dashboard_coll_ref = db.collection('prod1-dashboard')
# exceed_coll_ref = db.collection('prod1-exceeds_dump')
# log_coll_ref = db.collection('prod1-log')
collections = prod_artist_data_coll_ref.get()
payloads = {}
for doc in collections:
    payload = doc.to_dict()
    artist_id = doc.id
    # Reset status
    payload['is_traversed'] = False
    payload['collaborators_v2'] = []
    payload['artist_id'] = artist_id
    payloads[artist_id] = payload
print(f'pushing {len(payloads)} documents to new firestore collection')

new_coll_ref = db.collection('prod2-artist_data')

payloads_part1 = dict(list(payloads.items())[len(payloads)//2:])
payloads_part2 = dict(list(payloads.items())[:len(payloads)//2])


batch = db.batch()
# Commit the batch
for key, payload in payloads_part1.items():
    doc_ref = new_coll_ref.document(key)
    batch.set(doc_ref, payload)
batch.commit()

batch = db.batch()
# Commit the batch
for key, payload in payloads_part2.items():
    doc_ref = new_coll_ref.document(key)
    batch.set(doc_ref, payload)
batch.commit()

new_queue_ref = db.collection('prod2-dashboard').document('queue')

new_queue_ref.set({'ids':list(payloads.keys())})


old_exceeds_dump =  db.collection('prod1-exceeds_dump')
new_exceeds_dump = db.collection('prod2-exceeds_dump')

collections_dump = old_exceeds_dump.get()
payloads_dump = {}
for doc in collections_dump:
    payload = doc.to_dict()
    artist_id = doc.id
    # Reset status
    payload['is_traversed'] = False
    # payload['collaborators'] = []
    payload['artist_id'] = artist_id
    payloads_dump[artist_id] = payload
print(f'pushing {len(payloads_dump)} documents to new firestore collection')
payloads_dump1 = dict(list(payloads_dump.items())[:400])
payloads_dump2 = dict(list(payloads_dump.items())[400:800])
payloads_dump3 = dict(list(payloads_dump.items())[800:1200])
payloads_dump4 = dict(list(payloads_dump.items())[1200:1600])
payloads_dump5 = dict(list(payloads_dump.items())[1600:2000])
payloads_dump6 = dict(list(payloads_dump.items())[2000:])

print("part1")
batch = db.batch()
# Commit the batch
for key, payload in payloads_dump1.items():
    doc_ref = new_exceeds_dump.document(key)
    batch.set(doc_ref, payload)
batch.commit()

print("part2")
batch = db.batch()
# Commit the batch
for key, payload in payloads_dump2.items():
    doc_ref = new_exceeds_dump.document(key)
    batch.set(doc_ref, payload)
batch.commit()

print("part3")
batch = db.batch()
for key, payload in payloads_dump3.items():
    doc_ref = new_exceeds_dump.document(key)
    batch.set(doc_ref, payload)
batch.commit()

print("part4")
batch = db.batch()
for key, payload in payloads_dump4.items():
    doc_ref = new_exceeds_dump.document(key)
    batch.set(doc_ref, payload)
batch.commit()

print("part5")
batch = db.batch()
for key, payload in payloads_dump5.items():
    doc_ref = new_exceeds_dump.document(key)
    batch.set(doc_ref, payload)
batch.commit()

print("part6")
batch = db.batch()
for key, payload in payloads_dump6.items():
    doc_ref = new_exceeds_dump.document(key)
    batch.set(doc_ref, payload)
batch.commit()

print("DONE!")