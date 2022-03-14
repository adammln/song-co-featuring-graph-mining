import json
import pandas as pd

DATAPATH = 'data/clean/staging-artist_data_v2-clean_v3_1000.json'
TARGET_DIR = 'data/graph/v2/'

def load_ids(filepath):
    excluded_ids = []
    with open(filepath) as json_file:
        data = json.load(json_file)
        for x in data['collections']:
            excluded_ids.append(x)
    return excluded_ids

def load_data(filepath):
    data = {}
    print(f'>-----loading: {filepath}')
    with open(filepath) as json_file:
        data = json.load(json_file)
    return data

def create_undirected_edges(data_dict):
    t_edges = set()
    vertices = []
    for key, data in data_dict.items():
        for collaborator in data['collaborators']:
            t_edges.add(frozenset([key, collaborator]))
        # delete unsude data
        del data['is_traversed']
        del data['collaborators']
        # join genres as a string separated with comma
        genres = '|'.join(data['genres'])
        data['genres'] = genres
        data['id'] = key
        vertices.append(data)
    edges = [list(edge) for edge in list(t_edges)]
    return edges, vertices


def read_and_create_csv(datapath, target_dir):
    loaded = load_data(DATAPATH)['collections']
    edges, vertices = create_undirected_edges(loaded)
    df_vertices = pd.DataFrame(vertices, columns=['id', 'name', 'genres', 'popularity', 'followers', 'playlist'])
    df_edges = pd.DataFrame(edges, columns=['vertice1', 'vertice2'])
    df_vertices.to_csv(target_dir+'vertices.csv', index=False)
    df_edges.to_csv(target_dir+'edges.csv', index=False)

if __name__ == "__main__":
    read_and_create_csv(DATAPATH, TARGET_DIR)


