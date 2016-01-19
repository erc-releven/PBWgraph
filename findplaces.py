import pbw
import config
import geonames
import gplaces
import re
from geopy.distance import vincenty
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('mysql+pymysql://' + config.dbstring)
smaker = sessionmaker(bind=engine)
session = smaker()
resultcache = {}


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


def lookup_place(querystring):
    if querystring in resultcache:
        return resultcache[querystring]
    lookupsource = 'google'
    lookupresult = [x for x in gplaces.lookup_place(querystring) if filterloc(x)]
    if lookupresult is None or len(lookupresult) == 0:
        lookupsource = 'geonames'
        lookupresult = [x for x in geonames.lookup_place(querystring) if filterloc(x)]
    if len(lookupresult) > 0:
        resultcache[querystring] = (lookupsource, lookupresult)
    return lookupsource, lookupresult


# For each location in the database, look it up
found = 0
total = 0
for place in session.query(pbw.Location).filter(pbw.Location.longitude.is_(None)).all():
    print("Looking up place %s: %s" % (place.locationKey, place.locName))
    total += 1
    source, result = lookup_place(place.locName)
    if len(result) == 0:
        # Try again if we can dequalify the name. Is it something like
        # Athos: Phakenou ?
        colonmatch = re.match(r"\w+(?=:)", place.locName)
        if colonmatch is not None:
            source, result = lookup_place(colonmatch.group(0))
        else:
            # Or is it something like Philippi (Macedonia) ?
            parenmatch = re.search(r"\((.*)\)", place.locName)
            if parenmatch is not None:
                source, result = lookup_place(parenmatch.group(1))

    if result is None or len(result) == 0:
        print("No lookup results for %s" % place.locName)
        place.latitude = "NONE"
    else:
        found += 1
        if len(result) == 0:
            print("No sensible results for %s" % place.locName)
            place.latitude = "RECHECK"
        else:
            firstresult = result[0]

            place.latitude = firstresult['geometry']['location']['lat']
            place.longitude = firstresult['geometry']['location']['lng']
            place.geosource = source
            if 'viewport' in firstresult['geometry']:
                place.radius = getradius(firstresult['geometry']['viewport'])
            else:
                place.radius = None
            print("Found the result %s of type %s at %s %s, radius %s"
                  % (firstresult['name'], firstresult['types'], place.latitude, place.longitude, place.radius))
            session.commit()

print("Queried %d places, found %d" % (total, found))
