import pbw
import config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('mysql+pymysql://' + config.dbstring)
smaker = sessionmaker(bind=engine)
session = smaker()


def lookup_person(name, num):
    q = session.query(pbw.Person).filter_by(name=name, mdbCode=num)
    return q.scalar()

def lookup_nfactoid():
    q = session.query(pbw.NarrativeFactoid).filter_by()

josc = lookup_person("Joscelin", 4001)
narrative_sources = {}
for nfact in josc.main_factoids("Narrative"):
    narrative_sources[nfact] = (nfact.sourceKey, nfact.sourceRef)

for lfact in josc.main_factoids("Location"):
    foundSource = False
    for nfact in narrative_sources.keys():
        source = narrative_sources[nfact]
        if (source[0] == lfact.sourceKey and source[1] == lfact.sourceRef):
            foundSource = True
            print("Joscelin was at %s around %s - %s" % (lfact.locationInfo.location.locName,
                                                         nfact.narrativeUnit.dates,
                                                         nfact.narrativeUnit.dateType))
    if not foundSource:
        print("%s: Joscelin was at %s at an unknown time according to %s %s"
              % (lfact.factoidKey, lfact.locationInfo.location.locName, lfact.sourceKey, lfact.sourceRef))

# Location factoids are not associated with a narrative unit
# Narrative factoids are; match them up by source string

# For each location factoid for a person, get the source, and match that source to narrative factoids about the person