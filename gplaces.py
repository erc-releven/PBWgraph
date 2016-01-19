import config
import requests


# Here is how we look something up
def lookup_place(querystring):
    search_endpoint = 'https://maps.googleapis.com/maps/api/place/textsearch/json'
    search_params = {
        'query': querystring,
        'key': config.places_apikey,
        'language': 'en'
        }

    r = requests.get(search_endpoint, params=search_params).json()
    if 'results' in r:
        return r['results']
    else:
        return []
