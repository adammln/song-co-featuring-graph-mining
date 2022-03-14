from firebase import firebase

FIREBASE_URL = "https://your-rtdb-project-id.asia-southeast1.firebasedatabase.app/"
FB = firebase.FirebaseApplication(FIREBASE_URL, None)
MODE = "Prod" # Dev/Prod
ROOT = '/'+MODE
QUEUE_NAME = "-MrcyIUk8tI-AdZgUUt7"
DASHBOARD_NAME = "-MrcyIS_R9NoYaBN_bVY"
PAIRS_NAME = "-MrcyIQOaVvQ1tMOQgqr"
IN_PROCESS_NAME = "-Mrd11nZdmknL3M3e-H2"

class FirebaseApi:
    def __init__(self):
        print("rtdb initiated!")

    def connect(self, fb_url=FIREBASE_URL):
        return firebase.FirebaseApplication(fb_url, None)

    ## Helper
    def get(self, path, name, fb=FB):
        response = fb.get(ROOT+path, name)
        return response

    def put(self, path, name, updated_data, fb=FB):
        response = fb.put(url=ROOT+path, name=name, data=updated_data)
        return response

    def post(self, path, data, fb=FB):
        response = fb.post(
            ROOT+path, 
            data, 
            {'print': 'pretty'}, 
            {'X_FANCY_HEADER': 'VERY FANCY'}
        )
        return response

    #1
    def add_new_artist(self, data):
        response = self.post('/artist_data', data)
        return response

    #2---3
    def is_artist_already_listed(self, artist_id):
        id_pairs = self.get('/pairs', PAIRS_NAME)
        boolean = artist_id in id_pairs['id_pairs']
        return boolean
    
    def get_pairs(self):
        response = self.get('/pairs', PAIRS_NAME)
        return response
    
    def get_dashboard(self):
        respones = self.get('/dashboard', DASHBOARD_NAME)

    #3---2
    def add_new_id_pair(self, artist_id, push_id):
        to_update_pairs = self.get('/pairs', PAIRS_NAME)
        to_update_pairs[artist_id] = push_id
        response = put('/pairs', PAIRS_NAME, to_update_pairs)
        return response

    #x
    def add_new_artist_to_dashboard(self, artist_id, dashboard_data):
        to_update_dashboard = self.get('/dashboard', DASHBOARD_NAME)
        if (artist_id not in to_update_dashboard):
            to_update_dashboard[artist_id] = dashboard_data
            response = put('/dashboard', DASHBOARD_NAME, to_update_dashboard)
            return response
        else:
            return {}

    #4
    def set_traversed_status_true(self, artist_id):
        to_update_dashboard = self.get('/dashboard', DASHBOARD_NAME)
        already_traversed = to_update_dashboard[artist_id]['traversed']
        if (not already_traversed):
            to_update_dashboard[artist_id]['traversed'] = True
            response = put('/dashboard', DASHBOARD_NAME, to_update_dashboard)
            return response
        else:
            return {}
        
    #6
    def get_push_id_by_artist_id(self, artist_id):
        listed_push_ids = self.get('/pairs', PAIRS_NAME)
        push_id = listed_push_ids[artist_id]
        return push_id

    def get_artist_data_by_push_id(self, push_id):
        response = self.get('/artist_data', push_id) #artist_data
        return response, push_id
    
    def update_artist_data_by_push_id(self, name, updated_data):
        response = self.put('/artist_data', name, updated_data)
        return response
    
    def add_new_queue_entries(self, new_artist_ids):
        to_update_queue = self.get('/traverse_queue', QUEUE_NAME)
        existing_queue = to_update_queue['queued_ids']
        updated_queue = list(set(existing_queue + new_artist_ids))
        to_update_queue['queued_ids'] = updated_queue
        response = self.put('/traverse_queue', QUEUE_NAME, to_update_queue)
        return response
    
    def get_queue(self):
        response = self.get('/traverse_queue', QUEUE_NAME)
        return response

    #8
    def pop_up_to_5_artists_from_queue(self):
        to_pop_queue = self.get('/traverse_queue', QUEUE_NAME)
        existing_queue = to_update_queue['queued_ids']
        popped_out = []
        size = len(existing_queue)
        if size >= 5:
            for _ in range(5): x.append(existing_queue.pop(0))
        else:
            for _ in range(size): x.append(existing_queue.pop(0))
        to_update_queue['queued_ids'] = existing_queue
        response = self.put('/traverse_queue', QUEUE_NAME, to_update_queue)
        return reponse, popped_out

    #x
    def add_new_in_process_entries(self, popped_out_artist_ids):
        to_update_ids_in_process = self.get('/traverse_in_process', IN_PROCESS_NAME)
        existing_ids_in_process = to_update_ids_in_process['ids_in_process']
        updated_ids_in_process = list(set(existing_ids_in_process + popped_out_artist_ids))
        to_update_ids_in_process['ids_in_process'] = updated_ids_in_process
        response = self.put('/traverse_in_process', IN_PROCESS_NAME, to_update_ids_in_process)
        return response

    def delete_ids_from_in_process(self, to_be_deleted_ids):
        to_update_ids_in_process = self.get('/traverse_in_process', IN_PROCESS_NAME)
        existing_ids_in_process = to_update_ids_in_process['ids_in_process']
        for artist_id in to_be_deleted_ids:
            try:
                existing_ids_in_process.remove(0)
            except ValueError:
                print(f'''Error: Trying to remove non existing id: ({artist_id}) 
                from /traverse_in_process''')
        to_update_ids_in_process['ids_in_process'] = existing_ids_in_process
        response = self.put('/traverse_in_process', IN_PROCESS_NAME, to_update_ids_in_process)
        return response
        
    #9
    def add_collaborators_to_an_artist(self, artist_id, new_collaborator_ids):
        artist_data, push_id = self.get_artist_data_and_push_id_by_artist_id(artist_id)
        existing_collaborators = artist_data['collaborators']
        updated_collaborators = list(set(existing_collaborators + new_collaborator_ids))
        artist_data['collaborators'] = updated_collaborators
        response = self.put('/artist_data', push_id, artist_data)
        return response