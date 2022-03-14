import json
"""
    1.  list out all exluded ids from dump
    
    2.  iterate over all the artist_data
    
    3.  update the collaborators in artist_data
        by removing the ids which exists in dump
    
    4.  remove the document if the degree after removal
        is zero (no collaborators)
    
    5.  count the remaining documents after documents removal
"""

DUMP_PATH = 'data/raw/staging-exceeds_dump_v3_1000.json'
ARTIST_DATA_PATH = 'data/raw/staging-artist_data_v3_1000.json'
TARGET_FILEPATH = 'data/clean/staging-artist_data_v2-clean_v3_1000.json'
MIN_DEGREE = 2

def load_ids(filepath):
    excluded_ids = []
    with open(filepath) as json_file:
        data = json.load(json_file)
        for x in data['collections']:
            excluded_ids.append(x)
    return excluded_ids

def load_ids_included_only(data, min_degree):
    included_ids = []
    for key, payload in data.items():
        if (len(payload['collaborators']) >= min_degree):
            included_ids.append(key)
    return included_ids

def load_data(filepath):
    data = {}
    with open(filepath) as json_file:
        data = json.load(json_file)
    return data

def update_collaborators(prev_collaborators, included_ids):
    new_collaborators = list(set(prev_collaborators).intersection(included_ids))
    return new_collaborators

def to_json(target_filepath, data):
    with open(target_filepath, 'a', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def run():
    difference = -1 
    included_ids = load_ids(ARTIST_DATA_PATH)
    artist_data = load_data(ARTIST_DATA_PATH)
    updated_artist_data = {}
    iteration = 0
    while (difference != 0):
        updated_artist_data = {'collections':{}}
        print(f'>--iteration: {iteration}')
        # excluded_ids = load_ids(DUMP_PATH)
        for key in artist_data['collections']:
            prev_collaborators = artist_data['collections'][key]['collaborators']
            # do update
            artist_data['collections'][key]['collaborators'] = update_collaborators(prev_collaborators, included_ids)
            name = artist_data['collections'][key]['name']
            updated = artist_data['collections'][key]['collaborators']
            if len(updated) >= MIN_DEGREE:
                updated_artist_data['collections'][key] = artist_data['collections'][key]
            # print(f'======\n\n{name} ==> from: \n{prev_collaborators}\n\n ===>to \n\n{updated}')
        new_included_ids = load_ids_included_only(updated_artist_data["collections"], MIN_DEGREE)
        complement = set(included_ids) -  set(new_included_ids)
        difference = len(complement)
        if difference > 0:
            included_ids = new_included_ids
        iteration+=1

    print(f'FINAL RESULT: {len(updated_artist_data["collections"])} DOCUMENTS WITH DEGREE >= {MIN_DEGREE}')
    
    to_json(TARGET_FILEPATH, updated_artist_data)

run()

  

