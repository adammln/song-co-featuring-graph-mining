import os
import spotipy
import time
from tqdm import tqdm
from spotipy.oauth2 import SpotifyClientCredentials
import firestore_api as fs
from google.cloud import firestore
import traceback
import sys

os.environ['SPOTIPY_CLIENT_ID']='YOUR_SPOTIFY_WEB_API_CLIENT_ID'
os.environ['SPOTIPY_CLIENT_SECRET']='YOUR_SPOTIFY_WEB_API_CLIENT_SECRET'
os.environ['SPOTIPY_REDIRECT_URI']='http://localhost:9091/'
auth_manager = SpotifyClientCredentials()
sp = spotipy.Spotify(auth_manager=auth_manager)

def get_artist_data_from_spotify_api(artist_id, spotify_api):
    data = spotify_api.artist(artist_id)
    payload = {
        'collaborators': [],
        'followers': data['followers']['total'],
        'genres': data['genres'],
        'is_traversed': False,
        'name': data['name'],
        'playlist': 'N/A',
        'popularity': data['popularity']
    }
    return payload

def get_collab_tracks_from_several_albums(artist_id, album_ids, spotify_api):
    several_albums = spotify_api.albums(album_ids)
    collab_tracks = set()
    merged_collaborators = set()
    for album in several_albums['albums']:
        # print(f'NAME ===> {album["name"]}')
        # print(f'TYPE ===> {album["type"]}')
        for track in album['tracks']['items']:
            collaborators_per_track = set()
            inspect = []
            artists = [c['id'] for c in track['artists']]
            # print(artists)
            if artist_id in artists:
                artists.remove(artist_id)
                if (len(artists)>0):
                    for collaborator in artists:
                        collaborators_per_track.add(collaborator)
                        merged_collaborators.add(collaborator)
                    # collaborators_per_track.remove(artist_id)
                    collab_tracks.add(frozenset(collaborators_per_track))
                    # print(f'track: {track["name"]} ==> {collaborators_per_track}')
    return artist_id, collab_tracks, merged_collaborators

def get_albums_ids_from_an_artist_id(artist_id, spotify_api):
    albums = spotify_api.artist_albums(artist_id, limit=50)
    ids = set()
    for album in albums['items']:
        # print(album)
        album_id = album['id']
        ids.add(album_id)
    return ids

def run():
    # get queue
    # pop out 15 artists
    sleep_count = 0
    collaborators_payloads = {}
    while sleep_count < 3:
        ids_to_traverse, is_traversable = fs.pop_from_queue(10)
        if is_traversable:
            sleep_count = 0
            for artist_id in ids_to_traverse:
                info = f'start traversing'
                fs.log_info({'type': "info", 'message': info, 'artist_id':artist_id})
                album_ids = get_albums_ids_from_an_artist_id(artist_id, sp)
                album_ids = list(album_ids)
                batch_size = 20
                batched_album_ids = [album_ids[i:i + batch_size] for i in range(0, len(album_ids), batch_size)]
                for batch_ids in batched_album_ids:
                    _, collab_tracks, merged_collaborators = get_collab_tracks_from_several_albums(artist_id, batch_ids, sp)
                    # traverse
                    update_success = fs.update_collaborators_of_an_artist(
                        artist_id=artist_id, 
                        collaborators=list(merged_collaborators)
                    )
                    
                    # GATHERING collaborators data
                    print(f'adding collaborators of {artist_id} to database...')
                    print("creating reusable data payloads...")
                    for collaborator_id in tqdm(merged_collaborators):
                        if collaborator_id not in collaborators_payloads:
                            is_exists = fs.is_data_exists(
                                artist_id=collaborator_id
                            )
                            if (is_exists):
                                collaborators_payloads[collaborator_id] = {'collaborators':[]}
                            else:
                                collaborators_payloads[collaborator_id] = get_artist_data_from_spotify_api(collaborator_id, sp)
                                time.sleep(0.15)
                        else:
                            pass
                    
                    print("pushing to database...")
                    for collaborators_per_track in tqdm(collab_tracks):
                        cpt = list(collaborators_per_track)
                        i = 0
                        while i < len(cpt):
                            a_id = cpt[i]
                            payload = collaborators_payloads[a_id]
                            prev_collaborators = payload['collaborators']
                            updated_collaborators = list(set(prev_collaborators + [artist_id] + list(collaborators_per_track)))
                            updated_collaborators.remove(a_id)
                            payload['collaborators'] = updated_collaborators
                            try:
                                success, message = fs.add_artist_data(
                                    artist_id=a_id, 
                                    data=payload
                                )
                                if (not success):
                                    fs.log_info({'type': "code error", 'message': message, 'collaborator_id':a_id, 'traversing':artist_id})
                                i+=1
                            except Exception:
                                message = f'DB Error: Failed when adding data of {artist_id}. Will be retried later...\n Keeping machine up... '
                                trace = str(traceback.format_exc())
                                sysnf = str(sys.exc_info()[2])
                                print(trace)
                                print(sysnf)
                                print(message)
                                print("Continuing in 3 seconds...")
                                time.sleep(3)
                                fs.log_info({'type': "db error", 'message': message, 'collaborator_id':a_id, 'traversing':artist_id, 'trace':trace, 'sysinfo':sysnf})
                                cpt.append(a_id)
                                i+=1
                                continue
                fs.traverse(artist_id)
                info = f'finished traversing'
                fs.log_info({'type': "info", 'message': info, 'artist_id':artist_id})
        
        else:
            sleep_count += 1
            time.sleep(5)
    message = f'IDLE: worker has been idle for {5*sleep_count} seconds, shutting down worker!'
    print(message)
    fs.log_info({'type': "IDLE", 'message':message})

if __name__ == "__main__":
    try:
        run()
    except Exception:
        tb = str(traceback.format_exc())
        sysinfo = str(sys.exc_info()[2])
        fs.log_info({
            'type': "other error", 
            'trace': tb,
            'sysinfo': sysinfo
        })
        print(tb)
        print(sysinfo)

