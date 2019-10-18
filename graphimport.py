import pbw
import config
import re
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from neo4j import GraphDatabase

engine = create_engine('mysql+pymysql://' + config.dbstring)
smaker = sessionmaker(bind=engine)
sqlsession = smaker()


# Make our list of sources / authorities
def get_authmap():
    mj = 'Michael Jeffreys'
    tp = 'Tassos Papacostas'
    ta = 'Tara Andrews'
    jr = 'Judith Ryder'
    mw = 'Mary Whitby'
    wa = 'Wahid Amin'
    bs = 'Bruna Soravia'
    hm = 'Harry Munt'
    lo = 'Letizia Osti'

    authorities = {
        'Albert of Aachen': [mj],
        'Alexios Stoudites': [tp],
        'Anna Komnene': [mj],
        'Annales Barenses': [mj],
        'Anonymus Barensis': [mj],
        'Aristakes': [tp, ta],
        'Attaleiates: Diataxis': [tp],
        'Attaleiates: History': [tp],
        'Basilakios, Orationes et epistulae': [],
        'Basileios of Calabria to Nikolaos III': [mj],
        'Boilas': [tp],
        'Bryennios': [tp],
        'Christophoros of Mitylene': [mj],
        'Christos Philanthropos, note': [mj],
        'Chrysobull of 1079': [tp],
        'Clement III to Basileios of Calabria': [jr],
        'Council of 1147': [mj],
        'Council of 1157': [mj],
        'Dionysiou': [tp],
        'Docheiariou': [tp],
        'Documents d\'ecclesiologie ': [jr],
        'Domenico of Grado': [jr],
        'Droit matrimonial': [jr],
        'Edict on Clergy Reform': [mj],
        'Edict on clergy reform': [mj],
        'Eleousa: Acts': [tp],
        'Eleousa: Typikon': [mw],
        'Esphigmenou': [tp],
        'Eustathios Romaios': [mj],
        'Eustathios: Capture of Thessalonike': [mj],
        'Fulcher of Chartres': [mj],
        'Glykas': [tp],
        'Gregory VII, Epistolae vagantes': [jr],
        'Gregory VII, in Caspar': [jr],
        'Hilandar': [tp],
        'Humbert, Commemoratio': [jr],
        'Humbert, Dialogus': [jr],
        'Humbert, Excommunication': [jr],
        'Hypomnema on marriage': [mj],
        'Hypomnesis of May, 1094': [],
        'Ibn Shaddad': [bs, hm],
        'Ibn al-Athir': [wa],
        'Ioannes Italos': [mj],
        'Italikos': [mj],
        'Italos trial': [mj],
        'Iveron': [tp],
        'Jus Graeco-Romanum, III': [mj],
        'Kastamonitou': [tp],
        'Kecharitomene': [mj],
        'Kekaumenos': [tp],
        'Keroularios  ': [jr],
        'Kinnamos': [mj],
        'Kleinchroniken': [tp],
        'Kyrillos Phileotes': [tp],
        'Lavra': [tp],
        'Lazaros of Galesion': [tp],
        'Leo IX  ': [jr],
        'Leon of Chalcedon': [jr],
        'Leon of Ohrid (Greek)': [jr],
        'Leon of Ohrid (Latin)': [jr],
        'Lupus protospatharius': [mj],
        'Manasses, Chronicle': [mj],
        'Manasses, Chronicle: Dedication': [mj],
        'Manasses, Hodoiporikon': [mj],
        'Manganeios Prodromos': [mj],
        'Matthew of Edessa': [ta],
        'Mauropous: Letters': [tp],
        'Mauropous: Orations': [tp],
        'Michael the Rhetor, Regel': [mj],
        'Michel, Amalfi': [jr],
        'Nea Mone': [tp],
        'Nea Mone,': [tp],
        'Nicolas d\'Andida': [jr],
        'Nicole, Chartophylax': [jr],
        'Niketas Choniates, Historia': [mj],
        'Niketas Stethatos (Darrouzes)': [jr],
        'Niketas Stethatos, On the Holy Spirit': [jr],
        'Nikolaos III to Urban II': [jr],
        'Oath of Eudokia': [mj],
        'Odo of Deuil': [mj],
        'Pakourianos': [tp],
        'Panteleemon': [tp],
        'Pantokrator (Athos)': [tp],
        'Pantokrator Typikon': [tp],
        'Parthenon inscriptions': [mj],
        'Paschal II, Letter to Alexios I': [mj],
        'Patmos: Acts': [tp],
        'Patmos: Codicil': [mw],
        'Patmos: Testament': [mw],
        'Patmos: Typikon': [mw],
        'Peri metatheseon': [mj],
        'Petros of Antioch  ': [tp],
        'Petros of Antioch, ep. 2': [tp],
        'Prodromos, Historische Gedichte': [mj],
        'Protaton': [tp],
        'Psellos': [mj, tp],
        'Psellos: Chronographia': [mw],
        'Ralph of Caen': [mj],
        'Sakkos (1166)': [mj],
        'Sakkos (1170)': [mj],
        'Semeioma on Leon of Chalcedon': [jr],
        'Skylitzes': [tp],
        'Skylitzes Continuatus': [tp],
        'Synod of 1072': [mj],
        'Synod of 1094': [tp],
        'Synodal edict (1054)': [jr],
        'Synodal protocol (1089)': [jr],
        'Synopsis Chronike': [],
        'Thebes: Cadaster': [mj],
        'Thebes: Confraternity': [mj],
        'Theophylact of Ohrid, Speech to Alexios I': [mj],
        'Theophylaktos of Ohrid, Letters': [mj],
        'Tornikes, Georgios': [mj],
        'Tzetzes, Exegesis of Homer': [mj],
        'Tzetzes, Historiai': [mj],
        'Tzetzes, Homerica': [mj],
        'Tzetzes, Letters': [mj],
        'Tzetzes, Posthomerica': [mj],
        'Usama': [lo, hm],
        'Vatopedi': [tp],
        'Victor (pope)': [jr],
        'Walter the Chancellor': [mj],
        'William of Tyre': [mj],
        'Xenophontos': [tp],
        'Xeropotamou': [tp],
        'Yahya al-Antaki': [tp, lo, hm],
        'Zetounion': [jr],
        'Zonaras': [mw]
    }
    return authorities


def escape_text(t):
    return t.replace("'", "\\'")


# Get our authority map
authMap = get_authmap()
# Connect to the graph DB
driver = GraphDatabase.driver(config.graphuri, auth=(config.graphuser, config.passphrase))

# Get a list of people whose floruit matches our needs
floruit = r'[ML] XI(?!I)'
late11s = [x for x in sqlsession.query(pbw.Person).all() if re.match(floruit, x.floruit) and len(x.factoids) > 0]
print("Found %d relevant people" % len(late11s))

# Now start adding them to the graph database.
# Make our agent, who is PBW
with driver.session() as session:
    exists = session.run("MERGE (a:Agent {identifier:'PBW'}) return a").single()
    agent = exists['a']

# Now we are going to derive a bunch of assertions from the PBW database.
# For each of our people, they are a PBWPerson entity.
# The Person table has assertions about their descriptions and about their original-language names.
# First some setup of common objects from the Person table.
# - sex
with driver.session() as session:
    sexes = session.run(
        "MERGE (Female:Sex {value:'female'}) MERGE (Male:Sex {value:'male'}) MERGE (Eunuch:Sex {value:'eunuch'}) "
        "MERGE (Unknown:Sex {value:'unknown'}) RETURN Female, Male, Eunuch, Unknown").single()
    sex_predicate = session.run("MERGE (p:Predicate {label:'is_of_sex'}) RETURN p").single()['p']
    # - short description
    description_predicate = session.run("MERGE (p:Predicate {label:'has_description'}) RETURN p").single()['p']
    # - identifier in the Person record
    identified_predicate = session.run("MERGE (p:Predicate {label:'identified_as'}) RETURN p").single()['p']
    # - Make the predicates for the different sorts of factoids
    factoid_predicates = {}
    for r in sqlsession.query(pbw.FactoidType).all():
        label = r.typeName.lower().replace('/', '_').replace(' ', '_')
        if label not in ['uncertain_ident', 'alternative_name', 'eunuchs']:
            label = 'in_' + label if label == 'narrative' else 'has_' + label
            fpred = session.run("MERGE (p:Predicate {label:'%s'}) RETURN p" % label).single()['p']
            factoid_predicates[r.typeName] = fpred


for person in late11s:
    # Skip the Anonymi for now
    if person.name is 'Anonymi':
        continue
    # Create or find the person node
    print("Making/finding node for person %s %d" % (person.name, person.mdbCode))
    nodelookup = "MERGE (p:PBWPerson {name:'%s', code:%d}) RETURN p" % (person.name, person.mdbCode)
    with driver.session() as session:
        graphPerson = session.run(nodelookup).single()['p']
        # Add the assertions that are in the direct person record:
        # - sex
        uncertain = False
        ourSex = person.sex
        if ourSex == 'Mixed':  # we have already excluded Anonymi
            ourSex = 'Unknown'
        elif ourSex == '(Unspecified)':
            ourSex = 'Unknown'
        elif ourSex == 'Eunuch (Probable)':
            ourSex = 'Eunuch'
            uncertain = True
        if uncertain:
            assertionProps = ' {uncertain:true}'
        else:
            assertionProps = ''
        print("...setting sex to %s%s" % (ourSex, " (maybe)" if uncertain else ""))
        sexassertion = "MATCH (p), (sp), (s), (pbw) WHERE id(p) = %d AND id(sp) = %d AND id(s) = %d AND id(pbw) = %d " \
                       "MERGE (sp)<-[:PREDICATE]-(a:Assertion%s)-[:SUBJECT]->(p) MERGE (pbw)<-[:AUTHORITY]-(a)-[" \
                       ":OBJECT]->(s) RETURN a" % (graphPerson.id, sex_predicate.id, sexes[ourSex].id, agent.id,
                                                   assertionProps)
        session.run(sexassertion)

        # - description
        print("...setting description to %s" % person.descName)
        descassertion = "MATCH (p), (dp), (pbw) WHERE id(p) = %d AND id(dp) = %d AND id(pbw) = %d MERGE (dp)<-[" \
                        ":PREDICATE]-(a:Assertion)-[:SUBJECT]->(p) MERGE (pbw)<-[:AUTHORITY]-(a)-[:OBJECT]->(" \
                        "d:Description {text:'%s'}) RETURN a" % (graphPerson.id, description_predicate.id, agent.id,
                                                                 escape_text(person.descName))
        session.run(descassertion)

        # - name/identifier
        print("...setting identifier to %s" % person.nameOL)
        identassertion = "MATCH (p), (ip), (pbw) WHERE id(p) = %d AND id(ip) = %d AND id(pbw) = %d MERGE (ip)<-[" \
                         ":PREDICATE]-(a:Assertion)-[:SUBJECT]->(p) MERGE (pbw)<-[:AUTHORITY]-(a)-[:OBJECT]->(" \
                         "i:Identifier {text:'%s'}) RETURN a" % (graphPerson.id, identified_predicate.id, agent.id,
                                                                 escape_text(person.nameOL))
        session.run(identassertion)

        # Now add the different sorts of factoids
        for f in person.main_factoids('Dignity/Office'):
            # The factoid has the same subject, a predicate "held_dignity_office", an object which is the office,
            # a source from which the information comes, and an authority who interpreted the source.
            if f.dignityOffice is None:
                print("WARNING: Skipping un-factoided dignity on person %d (%s %d)"
                      % (person.personKey, person.name, person.mdbCode))
                continue
            dignityNotes = ", notes:'%s'" % escape_text(f.notes) if f.notes else ""
            dignityNode = session.run("MERGE (do:DignityOffice {name:'%s', name_en:'%s'%s}) RETURN do"
                                      % (escape_text(f.dignityOffice.stdNameOL), escape_text(f.dignityOffice.stdName),
                                         dignityNotes)).single()['do']
            sourceNode = session.run("MERGE (src:Source {identifier:'%s', reference:'%s'}) RETURN src"
                                     % (escape_text(f.source), escape_text(f.sourceRef))).single()['src']
            factoidAuth = authMap.get(f.source, [])
            # Make sure the assertion itself exists, and then add the source and authorities.
            # This lets us have multiple sources making the same assertion.
            doassertion = "MATCH (p), (dop), (do) WHERE id(p) = %d AND id(dop) = %d AND id(do) = %d MERGE (dop)<-[" \
                          ":PREDICATE]-(a)-[:SUBJECT]->(p) MERGE (a)-[:OBJECT]->(do) RETURN a" \
                          % (graphPerson.id, factoid_predicates['Dignity/Office'].id, dignityNode.id)
            a = session.run(doassertion).single()['a']
            # Now add the source
            session.run("MATCH (a), (src) WHERE id(a) = %d AND id(src) = %d MERGE (a)-[:SOURCE]->(src)"
                        % (a.id, sourceNode.id))
            for auth in factoidAuth:
                authNode = session.run("MERGE (agent:Agent {identifier:'%s'}) RETURN agent" % auth).single()['agent']
                session.run("MATCH (a), (ag) WHERE id(a) = %d AND id(ag) = %d MERGE (a)-[:AUTHORITY]->(ag)"
                            % (a.id, authNode.id))
