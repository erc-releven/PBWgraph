import pbw
import config
import re
import csv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('mysql+pymysql://' + config.dbstring)
smaker = sessionmaker(bind=engine)
session = smaker()


def collect_person_records(sqlsession):
    # Get a list of people whose floruit matches our needs
    floruit = r'([ML] )?XI(?!I)|10[3-8]\d'
    relevant = [x for x in sqlsession.query(pbw.Person).all() if re.search(floruit, x.floruit) and len(x.factoids) > 0]
    print("Found %d relevant people" % len(relevant))
    return relevant


seen_factoids = {}
with open('c11locs_other.csv', 'w', newline='', encoding='utf-8') as fh:
    cwriter = csv.writer(fh)
    relevant = collect_person_records(session)
    cwriter.writerow(['Name', 'Code', 'Factoid ID', 'Location', 'Pleiades', 'Geonames', 'Source', 'Source loc', 'Description'])
    for p in relevant:
        loc_factoids = p.main_factoids('Location')
        if len(loc_factoids) > 0:
            for lf in loc_factoids:
                if lf.factoidKey in seen_factoids:
                    continue
                seen_factoids[lf.factoidKey] = True
                if lf.locationInfo is None or lf.locationInfo.location is None:
                    # print("Person %s %d has a location-type factoid without a location record" % (p.name, p.mdbCode))
                    continue
                location = lf.locationInfo.location
                if location.origLang != "Greek" and location.origLang != "Latin":
                    cwriter.writerow([p.name, p.mdbCode, lf.factoidKey, location.locName,
                                      "https://pleiades.stoa.org/places/%s" % location.pleiades_id,
                                      location.geonames_id, lf.source, lf.sourceRef, lf.replace_referents()])
