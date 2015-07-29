import cleandata as pbw
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('mysql+pymysql://pbw:Alexios@localhost/pbw?charset=utf8mb4')
smaker = sessionmaker(bind=engine)
session = smaker()


def lookup_person(name, num):
    q = session.query(pbw.Person).filter_by(name=name, mdbCode=num)
    return q.scalar()


person = lookup_person('Gregorios', 127)
for f in person.main_factoids('Narrative'):
    print(f.factoidKey, f.engDesc)


# test all factoid types have their associated information
# test all (but some) persons have at least a factoid
# - ANSWER: some persons don't; they don't appear in the web DB
# test that the narrative units all belong to a tree
# - ANSWER: all except decade units, two years with no units, and problem units
# - fmKey seems to be used for ordering within a year:
# e.g. select fmKey, description from NarrativeUnit where year = 25137; for ordered list for 1125
# for  years, fmkey == YEAR000
#   for reigns, 15000 < fmKey < 16000
#   for events, 16000 < fmKey < 17000
# All "real" NUs have an attached year; several have no attached reign; none has an extraneous parent.

type_vars = {
    '(Unspecified)': None,
    'Narrative': None,
    'Authorship': None,
    'Death': 'deathRecord',
    'Description': None,
    'Dignity/Office': 'dignityOffice',
    'Education': None,
    'Ethnic label': 'ethnicityInfo',
    'Second Name': 'secondName',
    'Kinship': 'kinshipType',
    'Language Skill': 'languageSkill',
    'Location': 'locationInfo',
    'Occupation/Vocation': 'occupation',
    'Possession': 'possession',
    'Religion': 'religion',
    'Eunuchs': None,
    'Alternative Name': 'vnameInfo',
    'Uncertain Ident': None}

counter = 0
for f in session.query(pbw.Factoid).all():
    counter += 1
    if counter % 1000 == 0:
        print("Checked %d factoids" % counter)
    if f.persons is None:
        # Only factoids with an attached person mean anything.
        continue
    # Get the person attached to this factoid
    mp = f.main_person()
    person = ''
    if len(mp) == 1:
        person = str(mp[0])
    elif len(mp) == 0:
        person = "(none - refs %s)" % str(f.referents())

    # Check the factoids
    if f.factoidType is None:
        print("Factoid %d (%s) has no listed type. Attached to person %s" % (f.factoidKey, f.engDesc, person))
        continue
    if type_vars[f.factoidType] is None:
        # Needn't worry about types
        continue
    extra_data = getattr(f, type_vars[f.factoidType])
    if extra_data is None:
        print("Factoid %d (%s) has type %s but lacks associated info. Attached to person %s" % (
            f.factoidKey, f.engDesc, f.factoidType, person))