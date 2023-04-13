import pbw
import config
import csv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('mysql+pymysql://' + config.dbstring)
smaker = sessionmaker(bind=engine)
session = smaker()


def starts_with_any(key):
    for source in ['Hilandar', 'Iveron', 'Lavra', 'Nea Mone,', 'Panteleemon', 'Patmos: Acts', 'Protaton', 'Vatopedi',
                   'Xenophontos', 'Xeropotamou']:
        if key.startswith(source):
            return source
    return None


with open('pbw_sources.csv', encoding='utf-8', newline='') as f:
    rdr = csv.reader(f)
    with open('pbw_sources_out.csv', 'w', encoding='utf-8', newline='') as g:
        wrtr = csv.writer(g)
        for row in rdr:
            written = False
            source = starts_with_any(row[0])
            if source:
                factoid_text = row[9]
                for factoid in session.query(pbw.Factoid).filter_by(source=source).all():
                    engDesc = factoid.replace_referents().replace('>', '').replace('<', '')
                    if engDesc == factoid_text:
                        print(f"Matched row {row[0]} to factoid {factoid.factoidKey}")
                        row[9] = f"factoid {factoid.factoidKey}"
                        wrtr.writerow(row)
                        written = True
                        break
                if not written:
                    print(f"Did not match factoid for row {row[0]}")
            if not written:
                wrtr.writerow(row)
