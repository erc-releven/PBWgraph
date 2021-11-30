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


with open('c11deaths.csv', 'w', newline='', encoding='utf-8') as fh:
    cwriter = csv.writer(fh)
    relevant = collect_person_records(session)
    cwriter.writerow(['Name', 'Code', 'Death date', 'Source', 'Source loc', 'Description'])
    for p in relevant:
        death_factoids = p.main_factoids('Death')
        if len(death_factoids) > 0:
            for df in death_factoids:
                if df.deathRecord is None:
                    print("Person %s %d has a death-type factoid without a death record" % (p.name, p.mdbCode))
                else:
                    deathdate = df.deathRecord.sourceDate if df.deathRecord.sourceDate != '' else 'UNDATED'
                    cwriter.writerow([p.name, p.mdbCode, deathdate, df.source, df.sourceRef, df.replace_referents()])
