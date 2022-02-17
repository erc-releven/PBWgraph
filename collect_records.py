import pbw
import config
import csv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('mysql+pymysql://' + config.dbstring)
smaker = sessionmaker(bind=engine)
session = smaker()


def inrange(floruit):
    allowed = {
        'E / M XI',
        'L XI',
        'M XI',
        'L XI / E XII',
        'M / L XI',
        'XI',
        'E / L XI',
        'M XI / E XII',
        'E  / M XI',
        'L X / E XI',
        'E XI',
        'L XI / M XII',
        'L  XI',
        'M XI / L XI',
        'M XI/L XI',
        'L X / M XI',
        'M / L  XI',
        'E /M XI',
        'E XI / M XI',
        'E XI/M XI',
        'E / M  XI',
        'XI-XII',
        'M  / L XI',
        'mid XI',
        '1041/2',
        'mid-XI',
        'late XI',
        'L XI?',
        '1088',
        '1060',
        'M-E XI',
        'M/L XI',
        'E-L XI',
        'E XI-',
        'L XI-E XII',
        'M-L XI',
        'L XI/E XII',
        'E XI-1071',
        'c. 1006-1067',
        'c. 1050-1103',
        'c. 1007-1061',
        'E XI-c. 1088',
        'M XI/LXI',
        '1066-1123?',
        '1083-c. 1154',
        '1057-1118',
        'E/M XI',
        'XI / XII',
        'X / XI',
        '1050',
        '1070',
        '1075',
        'L XI /  E XII',
        'E X1',
        '??',
        '(XI)',
        'E  / L XI',
        '1090',
        'L XI / L XII',
        'X / XII',
        'LXI / E XII',
        'L XI /  M XII',
        '1084',
        'L XI - E XII',
        '? / M XI',
        'L X/ E XI',
        '1025',
        '11th c.',
        ' L XI',
        'L XI / XII',
        '?  XI?',
    }
    return floruit in allowed


def collect_person_records(sqlsession):
    # Get a list of people whose floruit matches our needs
    relevant = [x for x in sqlsession.query(pbw.Person).all() if inrange(x.floruit) and len(x.factoids) > 0]
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
