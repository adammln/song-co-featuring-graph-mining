import time, datetime
from google.cloud import firestore
import os
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credentials/YOUR_GOOGLE_APPLICATION_CREDENTIALS_FILE.json'


PROJECT_ID = "spotify-co-featuring2gra-34f8c"
MODE = 'staging'
PATH_DASHBOARD = MODE + '-dashboard'
PATH_ARTIST_DATA = MODE + '-artist_data' 
PATH_DUMP = MODE + '-exceeds_dump'
PATH_LOG = MODE + '-log'

NODE_LIMIT = 1000

def traverse(artist_id):
    db = firestore.Client(project=PROJECT_ID)
    transaction = db.transaction()
    artist_doc_ref = db.collection(PATH_ARTIST_DATA).document(artist_id)
    total_traverse_ref = db.collection(PATH_DASHBOARD).document('total_nodes_traversed')
    
    @firestore.transactional
    def update_traverse_stats(
        transaction,
        artist_doc_ref,
        total_traverse_ref
        ):
        snapshot_artist_doc = artist_doc_ref.get(transaction=transaction)
        snapshot_total_traversed = total_traverse_ref.get(transaction=transaction)
        total_traversed = snapshot_total_traversed.get('value')
        transaction.update(artist_doc_ref, {'is_traversed':True})
        transaction.update(total_traverse_ref, {'value':total_traversed+1})
        return True
    status = update_traverse_stats(
        transaction,
        artist_doc_ref,
        total_traverse_ref
    )
    return status

def is_data_exists(artist_id):
    db = firestore.Client(project=PROJECT_ID)
    dump_collection_ref = db.collection(PATH_DUMP)
    in_dump_collection = dump_collection_ref.document(artist_id).get().exists
    if (in_dump_collection):
        return True
    artist_collection_ref = db.collection(PATH_ARTIST_DATA)
    in_artist_collection = artist_collection_ref.document(artist_id).get().exists   
    if (in_artist_collection):
        return True
    return False

def update_collaborators_of_an_artist(artist_id, collaborators):
    db = firestore.Client(project=PROJECT_ID)
    transaction = db.transaction()
    artist_collection_ref = db.collection(PATH_ARTIST_DATA)
    dump_collection_ref = db.collection(PATH_DUMP)
    
    @firestore.transactional
    def update_collaborators(
        transaction,
        artist_id, 
        collaborators,
        artist_collection_ref, 
        ):
        artist_doc_ref = artist_collection_ref.document(artist_id)
        snapshot_artist_doc = artist_doc_ref.get(transaction=transaction)
        existing_collaborators = snapshot_artist_doc.get('collaborators')
        new_collaborators = list(set(collaborators) - set(existing_collaborators))
        transaction.update(
            artist_doc_ref,
            {
                'collaborators':existing_collaborators + new_collaborators,
                'is_traversed': True
            }
        )
        return True
    
    update_success = update_collaborators(
        transaction=transaction,
        artist_id=artist_id, 
        collaborators=collaborators,
        artist_collection_ref=artist_collection_ref,
        )
    return update_success
    
    
def pop_from_queue(count):
    db = firestore.Client(project=PROJECT_ID)
    transaction = db.transaction()
    queue_doc_ref = db.collection(PATH_DASHBOARD).document('queue')
    total_in_queue_ref = db.collection(PATH_DASHBOARD).document('total_in_queue')
    
    @firestore.transactional
    def pop_out_traverse_queue(  
        transaction,
        count,
        queue_ref, 
        total_in_queue_ref):

        snapshot_queue = queue_ref.get(transaction=transaction)
        existing_queue = snapshot_queue.get('ids')
        popped_out = []
        if len(existing_queue) >= count:
            for _ in range (count):
                popped_out.append(existing_queue.pop(0))
        else:
            for _ in range (len(existing_queue)):
                popped_out.append(existing_queue.pop(0))

        if len(popped_out) > 0:
            snapshot_total_in_queue = total_in_queue_ref.get(transaction=transaction)
            total_in_queue = snapshot_total_in_queue.get('value')
            transaction.update(queue_ref, {'ids':existing_queue})
            transaction.update(total_in_queue_ref,{'value':total_in_queue - len(popped_out)})
            return popped_out, True
        else:
            print("not traversable")
            return popped_out, False
    
    popped_out, is_traversable = pop_out_traverse_queue(
        transaction=transaction,
        count=count,
        queue_ref=queue_doc_ref, 
        total_in_queue_ref=total_in_queue_ref
    )
    return popped_out, is_traversable

def add_artist_data(artist_id, data):
    db = firestore.Client(project=PROJECT_ID)
    transaction = db.transaction()

    artist_collection_ref = db.collection(PATH_ARTIST_DATA)
    queue_doc_ref = db.collection(PATH_DASHBOARD).document('queue')
    total_in_queue_ref = db.collection(PATH_DASHBOARD).document('total_in_queue')
    total_in_dump_ref = db.collection(PATH_DASHBOARD).document('total_in_dump')
    total_nodes_ref = db.collection(PATH_DASHBOARD).document('total_nodes')
    dump_collection_ref = db.collection(PATH_DUMP)


    @firestore.transactional
    def add_new_artist_data(
        transaction,
        artist_id, 
        data, 
        artist_collection_ref, 
        total_in_queue_ref,
        total_in_dump_ref,
        total_nodes_ref,
        queue_ref, 
        dump_collection_ref):
        artist_doc_ref = artist_collection_ref.document(artist_id)
        snapshot_artist_doc = artist_doc_ref.get(transaction=transaction)
        inside_artists_collection = snapshot_artist_doc.exists
        
        dump_doc_ref = dump_collection_ref.document(artist_id)
        snapshot_dump_doc = dump_doc_ref.get(transaction=transaction)
        inside_dump = snapshot_dump_doc.exists
        
        snapshot_total_in_queue = total_in_queue_ref.get(transaction=transaction)
        snapshot_total_nodes = total_nodes_ref.get(transaction=transaction)
        total_in_queue = snapshot_total_in_queue.get('value')
        total_nodes = snapshot_total_nodes.get('value')
        above_limit = (total_nodes) >= NODE_LIMIT
        # 1 add to artist_data
        if (not inside_artists_collection and not inside_dump and not above_limit):
            snapshot_queue = queue_ref.get(transaction=transaction)
            existing_queue = snapshot_queue.get('ids')
            # update queue --> since it's new
            existing_queue.append(artist_id)
            transaction.update(queue_ref, {'ids':existing_queue})
            transaction.set(artist_doc_ref, data)
            # update dashboard
            transaction.update(total_in_queue_ref,{'value':total_in_queue+1})
            transaction.update(total_nodes_ref,{'value':total_nodes+1})
            return True, "Addded to artist_data"
        
        # 2 add to dump
        elif (not inside_artists_collection and not inside_dump and above_limit):
            snapshot_total_in_dump = total_in_dump_ref.get(transaction=transaction)
            total_in_dump = snapshot_total_in_dump.get('value')
            transaction.update(total_in_dump_ref, {'value': total_in_dump+1})
            transaction.set(dump_doc_ref, data)
            # update dashboard
            return True, "Total nodes already "+str(NODE_LIMIT)+". Addded to dump"

        # 3 present in dump, below limit. move to artist_data
        elif (not inside_artists_collection and inside_dump and not above_limit):
            # do nothing
            # move data from dump into artist_data
            # - read on dump
            payload = snapshot_dump_doc.get('')
            # - update collaborators
            existing_collaborators = payload['collaborators']
            new_collaborators = list(set(data['collaborators']) - set(existing_collaborators))
            payload['collaborators'] = existing_collaborators + new_collaborators
            
            # update dashboard data
            # - queue data
            snapshot_queue = queue_ref.get(transaction=transaction)
            existing_queue = snapshot_queue.get('ids')
            existing_queue.append(artist_id)
            # - total in dump (decrement)
            snapshot_total_in_dump = total_in_dump_ref.get(transaction=transaction)
            total_in_dump = snapshot_total_in_dump.get('value')
            transaction.update(total_in_dump_ref, {'value': total_in_dump-1})
            transaction.update(queue_ref, {'ids':existing_queue})
            # update dashboard
            # - total in queue (increment)
            transaction.update(total_in_queue_ref,{'value':total_in_queue+1})
            # - total nodes (increment)
            transaction.update(total_nodes_ref,{'value':total_nodes+1})
            # - write to artist_data
            transaction.set(artist_doc_ref, payload)
            # - delete on dump
            transaction.delete(dump_doc_ref)
            return True, "In Dump. Moved to artist_data. Collaborators updated"
        
        # 4 update collaborators in dump
        elif (not inside_artists_collection and inside_dump and above_limit):
            existing_collaborators = snapshot_dump_doc.get('collaborators')
            new_collaborators = list(set(data['collaborators']) - set(existing_collaborators))
            if len(new_collaborators) > 0:
                transaction.update(dump_doc_ref, {'collaborators': existing_collaborators + new_collaborators})
                return True, 'Already in dump, collaborators updated' 
            else:
                return True, 'Already in dump. No update needed'

        # 5-6 update collaborators in artist_data (NOT A TRAVERSAL)
        elif (inside_artists_collection and not inside_dump and (not above_limit or above_limit)):
            existing_collaborators = snapshot_artist_doc.get('collaborators')
            new_collaborators = list(set(data['collaborators']) - set(existing_collaborators))
            if len(new_collaborators) > 0:
                transaction.update(artist_doc_ref, {
                    'collaborators': existing_collaborators + new_collaborators,
                })
                return True, 'Already in artist_data. Collaborators updated' 
            else:
                return True, 'Already in artist data. No update needed'
                    
        # 7 IMPOSSIBLE: present in dump and artist_data, below limit
        elif (inside_artists_collection and inside_dump and not above_limit):
            return False, "ERROR: present in both artist_data & dump. nodes < "+str(NODE_LIMIT)
        # 8 IMPOSSIBLE: present in dump and artist data, above limit
        elif (inside_artists_collection and inside_dump and above_limit):
            return False, "ERROR: present in both artist_data & dump. nodes >= "+str(NODE_LIMIT)
        else:
            print(77*"=!=")
            print(f'inside_artists_collection: {inside_artists_collection}')
            print(f'inside_dump: {inside_dump}')
            print(f'total_nodes: {total_nodes}')
            return False, "ERROR: UNHANDLED CASE"
    status, message = add_new_artist_data(
        transaction = transaction,
        artist_id = artist_id,
        data = data,
        artist_collection_ref = artist_collection_ref, 
        total_in_queue_ref = total_in_queue_ref,
        total_in_dump_ref = total_in_dump_ref,
        total_nodes_ref = total_nodes_ref,
        queue_ref = queue_doc_ref, 
        dump_collection_ref = dump_collection_ref
    )
    time.sleep(0.05)
    return status, message

def log_info(error_data):
    db = firestore.Client(project=PROJECT_ID)
    transaction = db.transaction()

    errors_collection_ref = db.collection(PATH_LOG)

    @firestore.transactional
    def add_log(transaction, error_data, errors_collection_ref):
        timestamp = datetime.datetime.now()
        timestamp = str(timestamp)
        error_doc_ref = errors_collection_ref.document(timestamp)
        snapshot_errors_doc = error_doc_ref.get(transaction=transaction)
        transaction.set(error_doc_ref, error_data)
        print("Activity logged!")
        print(error_data)
    
    add_log(
        transaction=transaction, 
        error_data=error_data, 
        errors_collection_ref=errors_collection_ref
    )