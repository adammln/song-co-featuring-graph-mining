import os
import spotipy
import time
from tqdm import tqdm
from spotipy.oauth2 import SpotifyClientCredentials
from db_api import FirebaseApi

os.environ['SPOTIPY_CLIENT_ID']='YOUR_SPOTIFY_WEB_API_CLIENT_ID'
os.environ['SPOTIPY_CLIENT_SECRET']='YOUR_SPOTIFY_WEB_API_CLIENT_SECRET'
os.environ['SPOTIPY_REDIRECT_URI']='http://localhost:9091/'

auth_manager = SpotifyClientCredentials()
sp = spotipy.Spotify(auth_manager=auth_manager)

PLAYLIST_IDS = {
    'Top Hits of 2014':"37i9dQZF1DX0h0QnLkMBl4",
    'Top Hits of 2015':"37i9dQZF1DX9ukdrXQLJGZ",
    'Top Hits of 2016':"37i9dQZF1DX8XZ6AUo9R4R",
    'IndieNesia':"37i9dQZF1DXd82NU5rAcTZ"
}

def run():
    fb = FirebaseApi()
    artist_data_payloads = {} 
    listed_artist = [] #traverse queue payload
    dashboard_payloads = {}
    pairs_payload = {}
    for playlist_name, playlist_id in PLAYLIST_IDS.items():
        print(f'Getting playlist: {playlist_name}')
        # get playlist
        playlist = sp.playlist(playlist_id)
        # each track in playlist:
        print(f'Processing tracks from playlist: {playlist_name}...')
        for track in tqdm(playlist['tracks']['items']):
            tmp_artists = track['track']['artists']
            artist_ids = [artist['id'] for artist in tmp_artists]
            existing_artists = list(set(artist_ids) & set(listed_artist))
            new_artists = list(set(artist_ids) - set(listed_artist))
            
            for artist in tmp_artists:
                artist_id = artist['id']
                # - check if artists already exist, if new,register
                if artist_id in new_artists:
                    full_artist_data = sp.artist(artist_id)
                    time.sleep(0.3)
                    initial_collaborators = artist_ids
                    initial_collaborators.remove(artist_id)
                    # register artist data
                    artist_data_payload = {
                        'artist_id': artist_id,
                        'name': artist['name'],
                        'collaborators': ['!'] + initial_collaborators,
                        'genre': ['!'] + full_artist_data['genres'],
                        'popularity': full_artist_data['popularity'],
                        'followers': full_artist_data['followers']['total'],
                        'playlist': playlist_name #or "IndieNesia" or None
                    }
                    artist_data_payloads[artist_id] = artist_data_payload
                    # register traverse queue
                    listed_artist.append(artist_id)
                    # # register dashboard
                    dashboard_payload = {
                        'traversed': False,
                        'degree': len(initial_collaborators)
                    }
                    dashboard_payloads[artist_id] = dashboard_payload
                
                else:
                    # interconnect to one another
                    for ids in existing_artists:
                        existing_collaborators_an_artist = artist_data_payloads[ids]['collaborators']
                        updated_collaborators = list(set(existing_collaborators_an_artist + artist_ids))
                        updated_collaborators.remove(ids)
                        artist_data_payloads[ids]['collaborators'] = updated_collaborators
                        # update dashboard
                        dashboard_payloads[ids]['degree'] = len(updated_collaborators)

    print("\nDONE CREATING DATA! PUSHING TO DATABASE...\n")
    # after listing done, start pushing artist_data into database
    for artist_id, payload in tqdm(artist_data_payloads.items()):
        c = payload['collaborators']
        response = fb.add_new_artist(payload)
        time.sleep(0.2)
        push_id = response['name']
 
        # register pairs
        pairs_payload[artist_id] = push_id

    # push pairs (initial)
    response_pairs_push = fb.post('/pairs', pairs_payload)
    print()
    print("RESPONSE: PAIRS PUSH\n=================", response_pairs_push)
    print()
    # push dashboard
    response_dashboard_push = fb.post('/dashboard', dashboard_payloads)
    print("RESPONSE: dashboard PUSH\n=================", response_dashboard_push)
    print()
    # push traverse_queue
    response_traverse_queue_push = fb.post('/traverse_queue', listed_artist)
    print("RESPONSE: queue PUSH\n=================", response_traverse_queue_push)
    print()
    # push empty in process
    response_in_process_push = fb.post('/traverse_queue', ['!'])
    print("RESPONSE: in process PUSH\n=================", response_in_process_push)
    print()

if __name__ == "__main__":
    run()