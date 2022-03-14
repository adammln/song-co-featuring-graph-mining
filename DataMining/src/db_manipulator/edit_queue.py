"""
    OBJECTIVES:
    - add more nodes that are connected to at least 2 other nodes inside artist_data
    - to mine more neighbors of nodes from IndieNesia playlist (since there are not enough data/imbalanced)
    - get rid or avoid mining nodes with degree == 1

    TO-DO:
    - get rids nodes with deg==1:
        - for existings(nodes degree=1):
            - delete (preferred in top hits playlist)
            - find existing neighbors in dump (preferred in IndieNesia playlist)
        - for new:
            - check if nodes (artist_id) is connected with the in_traverse 
              && at least the other one* inside artist_data
              *(listed in neighbors of all existing data (artist_data))
              [YES]--> add to queue

    STEPS
    [LOCAL]
    - read from local data, both in dump and artist_data.
    - get collaborators of indienesia (with no exclusion), only in dump.
    - filter out gotten collaborators ==> list_of_in_dump.
    - create intersection of set collaborators of each in list_of_in_dump...
    - ... with everyone in artist_data.
    - [one transaction with DB] Add to traverse queue if len intersection >= 2

    [DB CONNECTION]
    - after queue creation complete
    - move all collaborators from dump (id in queue) to artist_data
"""
import json

import time
from tqdm import tqdm
from google.cloud import firestore
import os

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credentials/spotify-co-featuring2gra-34f8c-firebase-adminsdk-k0pe2-bfcd86823f.json'

db = firestore.Client(
    project="spotify-co-featuring2gra-34f8c"
)

DUMP_PATH = 'backup/data/export/staging-exceeds_dump_v2.json'
ARTIST_DATA_PATH = 'backup/data/export/staging-artist_data_v2.json'

def load_ids(filepath):
    excluded_ids = []
    with open(filepath) as json_file:
        data = json.load(json_file)
        for x in data['collections']:
            excluded_ids.append(x)
    return excluded_ids

def load_data(filepath):
    data = {}
    with open(filepath) as json_file:
        data = json.load(json_file)
    return data

def indienesia_only(data):
    collections = data['collections']
    out = {'collections':{}}
    for artist_id, payload in collections.items():
        if payload['playlist'] == "IndieNesia":
            out['collections'][artist_id] = payload
    return out

def get_excluded_collaborators_ids(data, artist_ids, dump_ids):
    collections = data['collections']
    merged = set()
    for artist_id, payload in collections.items():
        collaborators = payload['collaborators']
        merged = merged.union(set(collaborators).intersection(dump_ids))
    out = merged - set(artist_ids)
    return out

def filter_data_by_ids(data, artist_ids):
    collections = data['collections']
    out = {
        'collections': {a_id: collections[a_id] for a_id in artist_ids}
    }
    return out

def filter_data_if_intersection_mteq_2(data, artist_ids):
    collections = data['collections']
    out = {'collections':{}}
    for artist_id, payload in collections.items():
        collaborators = payload['collaborators']
        intersected = set(collaborators).intersection(set(artist_ids))
        if len(intersected) >= 2:
            out['collections'][artist_id] = payload
            artist_ids.append(artist_id)
    return out

def update_db(queues_ids, excluded_indie_collaborator_data):
    print('updating_db....')
    # transaction = db.transaction()
    queue_doc_ref = db.collection('staging-dashboard').document('queue')
    # dump_coll_ref = db.collection('staging-exceeds_dump')
    # artist_collection_ref = db.collection('staging-artist_data')
    dashboard_queue_ref = db.collection('staging-dashboard').document('total_in_queue')
    # dashboard_nodes_ref = db.collection('staging-dashboard').document('total_nodes')

    batch = db.batch()
    # for queues_id in queues_ids:
        # payload = excluded_indie_collaborator_data['collections'][queues_id]
        # doc_ref = artist_collection_ref.document(queues_id)
        # dump_ref = dump_coll_ref.document(queues_id)
        # batch.set(doc_ref, payload)
        # print(f'delete from dump: {excluded_indie_collaborator_data["collections"][queues_id]["name"]}')
        # batch.delete(dump_ref)
    
    # total_nodes = dashboard_nodes_ref.get().get('value')
    batch.update(queue_doc_ref, {'ids':queues_ids})
    batch.update(dashboard_queue_ref, {'value':len(queues_ids)})
    # batch.update(dashboard_nodes_ref, {'value': total_nodes + len(queues_ids)})
    batch.commit()




def run():
    # get data
    dump_ids = load_ids(DUMP_PATH)
    artist_data_ids = load_ids(ARTIST_DATA_PATH)
    artist_data = load_data(ARTIST_DATA_PATH)
    dump_data = load_data(DUMP_PATH)
    # filter to indinesia only
    indienesia_data = indienesia_only(artist_data)
    # get ids of excluded indinesia's collaborators (in dump)
    excluded_indie_collaborator_ids = get_excluded_collaborators_ids(indienesia_data, artist_data_ids, dump_ids)
    # query exclude_indie_collaborator_ids from dump
    excluded_indie_collaborator_data = filter_data_by_ids(dump_data, excluded_indie_collaborator_ids)
    # filter out again which is associated to at least 2 artist_ids
    filtered_excluded_indie_collaborator_data = filter_data_if_intersection_mteq_2(excluded_indie_collaborator_data, artist_data_ids)
    first_queue_ids = list(filtered_excluded_indie_collaborator_data['collections'].keys())
    # print(first_queue_ids)
    update_db(first_queue_ids, filtered_excluded_indie_collaborator_data)

    # ------------
    # add to queue
    # push data & delete docuemnt to artist_id (1 batched transaction)
    # start mining

    # [LOCAL] optional, while mining
    # create reference = collaborators of filtered_excluded_indie_collaborator_data which in artist_data
    # update reference's collaborators --> add artist id of related filtered_excluded_indie_collaborator_data
    
    # recompute degree for each artist_ids in indienesia
    # if len == 1:
    # check original collaborators outside

run()
