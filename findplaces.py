import pbw
import config
import geonames
from geopy.distance import vincenty
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('mysql+pymysql://' + config.dbstring)
smaker = sessionmaker(bind=engine)
session = smaker()


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
        return None


def filterloc(locresult):
    # Weed out results that don't seem like geographic places.
    banned_types = ['store', 'restaurant', 'premise', 'travel_agency', 'food', 'lodging', 'gym']
    if 'types' in locresult:
        for ltype in locresult['types']:
            if ltype in banned_types:
                return False
    boundary = {'lat': (0, 65), 'lng': (-9, 67)}
    locdata = locresult['geometry']['location']
    for key in locdata:
        if locdata[key] < boundary[key][0]:
            return False
        if locdata[key] > boundary[key][1]:
            return False
    return True


def getradius(viewport):
    sw = (viewport['southwest']['lat'], viewport['southwest']['lng'])
    ne = (viewport['northeast']['lat'], viewport['northeast']['lng'])
    return vincenty(sw, ne).kilometers / 4


# For each location in the database, look it up
for place in session.query(pbw.Location).filter(pbw.Location.geosource.like("google")).all():
    print("Looking up place %s: %s" % (place.locationKey, place.locName))
    lookupresult = lookup_place(place.locName)
    if lookupresult is None or len(lookupresult) == 0:
        print("No lookup results for %s" % place.locName)
        place.latitude = "NONE"
    else:
        results = [x for x in lookupresult if filterloc(x)]
        if len(results) == 0:
            print("No sensible results for %s" % place.locName)
            place.latitude = "RECHECK"
        else:
            firstresult = results[0]

            place.latitude = firstresult['geometry']['location']['lat']
            place.longitude = firstresult['geometry']['location']['lng']
            place.geosource = 'google'
            if 'viewport' in firstresult['geometry']:
                place.radius = getradius(firstresult['geometry']['viewport'])
            else:
                place.radius = None
            print("Found the result %s of type %s at %s %s, radius %s"
                  % (firstresult['name'], firstresult['types'], place.latitude, place.longitude, place.radius))
            session.commit()
