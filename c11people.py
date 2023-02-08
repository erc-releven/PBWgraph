import config
import pbw
import PBWstarConstants
import re
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from neo4j import GraphDatabase
from dateparse import parse_date_info, julian_day

# Find people who have narrative factoids in our time period but are missed by our floruit filter
# First, set up the DB connections
engine = create_engine('mysql+pymysql://' + config.dbstring)
smaker = sessionmaker(bind=engine)
mysqlsession = smaker()
# Connect to the graph DB
driver = GraphDatabase.driver(config.graphuri, auth=(config.graphuser, config.graphpw))
# Retrieve the constants
constants = PBWstarConstants.PBWstarConstants(None, driver)

# Project dates: beginning of 1025 to end of 1095
project_mindate = julian_day(1025, 1, 1)
project_maxdate = julian_day(1095, 12, 31)

# Collect the narrative units that represent our years, and check the links this way too
# We are lazy here, and we hardcode the starting point
project_year_units = dict()
for decade in mysqlsession.query(pbw.NarrativeUnit).filter_by(year=18939):
    if re.match(r'^10[3-8].*', decade.description):
        project_year_units[decade.narrativeUnitID] = decade.description
    for year in mysqlsession.query(pbw.NarrativeUnit).filter_by(year=decade.narrativeUnitID):
        if 1024 < int(year.description) < 1096:
            project_year_units[year.narrativeUnitID] = year.description

# Keep track of which people we need to check
found_people = set()

for person in mysqlsession.query(pbw.Person).all():
    if not constants.inrange(person.floruit):
        for nf in person.main_factoids('Narrative'):
            if nf.narrativeUnit is None:
                continue
            # Check the date strings on the factoids
            result = parse_date_info(nf.narrativeUnit)
            if result is None:
                continue
            (dmin, dmax, datetype) = result
            if dmin is not None and project_mindate < dmin < project_maxdate:
                print("Found narrative factoid with date %s on person %s %d (fl. %s)" %
                      (nf.narrativeUnit.dates, person.name, person.mdbCode, person.floruit))
                found_people.add("%s %d" % (person.name, person.mdbCode))
            # Check the year sub-ordering on the narrative unit
            if nf.narrativeUnit.year in project_year_units:
                print("Found narrative factoid ordered under year/decade %s on person %s %d (fl. %s)"
                      % (project_year_units[nf.narrativeUnit.year], person.name, person.mdbCode, person.floruit))

print("Possibly mis-filed people to check: %s" % sorted(found_people))

# Results: ['Amalric 51', 'Anastasios 2101', 'Andronikos 17010', 'Anonyma 2129', 'Anonymus 101', 'Anonymus 2120',
# 'Anonymus 2223', 'Basileios 181', 'Basileios 182', 'Basileios 185', 'Basileios 2', 'Christophoros 106',
# 'Demetrios 146', 'Demetrios 161', 'Georgios 216', 'Ioannes 143', 'Ioannes 277', 'Ioannes 281', 'Ioannes 497',
# 'Konstantinos 187', 'Konstantinos 188', 'Konstantinos 189', 'Konstantinos 190', 'Konstantinos 8', 'Leontios 107',
# 'Manuel 1', 'Maria 166', 'Neophytos 107', 'Romanos 3', 'Shawar 101', 'Shirkuh 101', 'Stephanos 2101',
# 'Theodoulos 101', 'Theoktistos 104', 'Vladimirko 17001']

# Bad 'dates' string: ['Amalric 51', 'Andronikos 17010', 'Demetrios 161', 'Manuel 1', 'Shawar 101', 'Shirkuh 101',
# 'Vladimirko 17001']

# Bad floruit values (fixed): ['Anastasios 2101', 'Anonyma 2129', 'Anonymus 101', 'Anonymus 2120', 'Anonymus 2223',
# 'Basileios 181', 'Basileios 182', 'Basileios 185', 'Christophoros 106', 'Demetrios 146', 'Georgios 216',
# 'Ioannes 277', 'Ioannes 281', 'Ioannes 497', 'Konstantinos 187', 'Konstantinos 188', 'Konstantinos 189',
# 'Konstantinos 190', 'Maria 166', 'Theodoulos 101', 'Theoktistos 104']

# Corner cases: ['Basileios 2', 'Konstantinos 8', 'Neophytos 107', 'Romanos 3']

# Later reference: ['Ioannes 143', 'Leontios 107', 'Stephanos 2101']
