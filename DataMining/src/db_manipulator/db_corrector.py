from db_api import FirebaseApi
from tqdm import tqdm
import time
# to remove self id in collaborators

def run():
    #initiate db connection
    fb = FirebaseApi()
    print("Connection Initiated!")
    # get all pairs
    # pairs = fb.get_pairs()
    # for artist_id, push_id in tqdm(pairs.items()):
    #     artist_data = fb.get_artist_data_by_push_id(push_id)[0]
    #     if 'collaborators' not in artist_data:
    #         artist_data['collaborators'] = ['!']
    #     else:
    #         artist_data['collaborators'].insert(0,'!')
    #     if 'genre' not in artist_data:
    #         artist_data['genre'] = ['!']
    #     else:
    #         artist_data['genre'].insert(0,'!')
    #     response = fb.update_artist_data_by_push_id(push_id, artist_data)
    # response_in_process_push = fb.post('/in_process', ['!'])
    start = time.time()
    response = fb.get_pairs()
    print(time.time() - start)
    # print(response)

if __name__ == "__main__":
    run()