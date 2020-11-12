import pbw
import config
import re
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from neo4j import GraphDatabase


# Make our list of sources / authorities
def get_authmap():
    """Return a map of source -> authority (i.e. interpreter of that source) for the PBW data"""
    mj = 'Michael Jeffreys'
    tp = 'Tassos Papacostas'
    ta = 'Tara Andrews'
    jr = 'Judith Ryder'
    mw = 'Mary Whitby'
    wa = 'Wahid Amin'
    bs = 'Bruna Soravia'
    hm = 'Harry Munt'
    lo = 'Letizia Osti'
    cr = 'Charlotte Roueché'

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
        'Geonames': [cr],
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
        'Pleiades': [cr],
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
    """Escape any single quotes in strings that need to go into Neo4J properties"""
    return t.replace("'", "\\'")


def collect_person_records(sqlsession):
    # Get a list of people whose floruit matches our needs
    floruit = r'[ML] XI(?!I)'
    relevant = [x for x in sqlsession.query(pbw.Person).all() if re.match(floruit, x.floruit) and len(x.factoids) > 0]
    print("Found %d relevant people" % len(relevant))
    return relevant


def setup_constants(sqlsession, graphdriver):
    """Set up the necessary object and predicate nodes that will be shared by the individual records"""
    with graphdriver.session() as session:
        # Make our anonymous agent PBW for the un-sourced information
        generic_agent = session.run("MERGE (a:Agent {identifier:'PBW'}) return a").single()['a']
        # Set up the distinct sexes in the database
        pbw_sexes = session.run(
            "MERGE (Female:Sex {value:'female'}) MERGE (Male:Sex {value:'male'}) MERGE (Eunuch:Sex {value:'eunuch'}) "
            "RETURN Female, Male, Eunuch").single()
        # Set up the is_of_sex predicate
        sex_predicate = session.run("MERGE (p:Predicate {label:'is_of_sex'}) RETURN p").single()['p']
        # Set up the has_disambiguation predicate
        disambig_predicate = session.run("MERGE (p:Predicate {label:'has_disambiguation'}) RETURN p").single()['p']
        # Set up the identified_as predicate
        identified_predicate = session.run("MERGE (p:Predicate {label:'identified_as'}) RETURN p").single()['p']
        # Collect these into a dictionary
        factoid_predicates = {
            'sex': sex_predicate,
            'disambiguation': disambig_predicate,
            'identified': identified_predicate
        }
        # Create and collect predicates for the explicit factoid categories
        for r in sqlsession.query(pbw.FactoidType).all():
            label = r.typeName.lower().replace('/', '_').replace(' ', '_')
            if label == "authorship":
                label = "authored"
            elif label == "narrative":
                label = "involved_in"
            elif label == "kinship":
                label = "kin_to"
            elif label in ['uncertain_ident', 'alternative_name', 'eunuchs', '(unspecified)']:
                label = None
            else:
                label = "has_" + label
            if label is not None:
                fpred = session.run("MERGE (p:Predicate {label:'%s'}) RETURN p" % label).single()['p']
                factoid_predicates[r.typeName] = fpred
    # Return our agent, our sex objects, and our predicates
    return generic_agent, pbw_sexes, factoid_predicates


def get_source_node(session, factoid):
    return session.run("MERGE (src:Source {identifier:'%s', reference:'%s'}) RETURN src"
                       % (escape_text(factoid.source), escape_text(factoid.sourceRef))).single()['src']


def get_location_node(session, pbwloc):
    # The location record has an identifer, plus a couple of assertions by Charlotte about its
    # correspondence in the GeoNames and/or Pleiades database.
    loc_query = session.run("MATCH (l:Location {identifier: '%s', pbwid: %d}) RETURN l"
                            % (escape_text(pbwloc.locName), pbwloc.locationKey)).single()
    if loc_query is None:
        # We need to create it.
        loc_node = session.run("CREATE (l:Location {identifier: '%s', pbwid: %d}) RETURN l"
                               % (escape_text(pbwloc.locName), pbwloc.locationKey)).single()['l']
        for db in ("pleiades", "geonames"):
            dbid = pbwloc.__getattribute__("%s_id" % db)
            if dbid is not None:
                ag = session.run("MERGE (ag:Agent {identifier:'Charlotte Roueché'}) RETURN ag").single()['ag']
                dbloc = session.run("MERGE (l:DatabaseLocation {db: '%s', id: %d}) RETURN l" % (db, dbid)).single()['l']
                pred = session.run("MERGE (p:Predicate {label:'same_as'}) RETURN p").single()['p']
                session.run(
                    "MATCH (l), (ag), (dbl), (p) WHERE id(l) = %d AND id(ag) = %d AND id(dbl) = %d AND id(p) = %d "
                    "MERGE (a:Assertion)-[:SUBJECT]->(l) MERGE (a)-[:PREDICATE]->(p) MERGE (a)-[:OBJECT]->(dbl) "
                    "MERGE (a)-[:AUTHORITY]->(ag)" % (loc_node.id, ag.id, dbloc.id, pred.id))
    else:
        loc_node = loc_query['l']
    return loc_node


def get_object_node(session, factoid_type, factoid, person):
    if factoid_type == "Narrative":
        pass
    elif factoid_type == "Authorship":
        # We are asserting that the person authored the source in question, so the source is the object.
        return get_source_node(session, factoid)
    elif factoid_type == "Death":
        return session.run("MERGE (d:DeathRecord {text:'%s'}) RETURN d" % escape_text(factoid.engDesc)).single()['d']
    elif factoid_type == "Description":
        return session.run("MERGE (d:Description {text:'%s', orig_text:'%s'}) RETURN d"
                           % (escape_text(factoid.engDesc), escape_text(factoid.origLDesc))).single()['d']
    elif factoid_type == "Dignity/Office":
        if factoid.dignityOffice is None:
            return None
        return session.run("MERGE (do:DignityOffice {name:'%s', name_en:'%s'}) RETURN do"
                           % (escape_text(factoid.dignityOffice.stdNameOL),
                              escape_text(factoid.dignityOffice.stdName))).single()['do']
    elif factoid_type == "Education":
        return session.run("MERGE (e:Education {description:'%s'}) RETURN e"
                           % escape_text(factoid.engDesc)).single()['e']
    elif factoid_type == "Ethnic label":
        return None if factoid.ethnicityInfo is None else \
            session.run("MERGE (e:Ethnicity {identifier:'%s'}) RETURN e"
                        % escape_text(factoid.ethnicityInfo.ethnicity.ethName)).single()['e']
    elif factoid_type == "Second Name":
        return session.run("MERGE (i:Identifier {text:'%s'}) RETURN i").single()['i']
    elif factoid_type == "Kinship":
        if factoid.kinshipType is None:
            return None
        # The object is the other person in the factoid.
        kin = [x for x in factoid.persons if x != person]
        if len(kin) != 1:
            print("Skipping kinship factoid with more or less than 1 other person")
            return None
        return session.run("MERGE (p:PBWPerson {name:'%s', code:%d}) RETURN p"
                           % (kin[0].name, kin[0].mdbCode)).single()['p']
    elif factoid_type == "Language Skill":
        return session.run("MERGE (l:Language {identifier: '%s'}) RETURN l" % factoid.languageSkill).single()['l']
    elif factoid_type == "Location":
        if factoid.locationInfo is None:
            return None
        if factoid.locationInfo.location is None:
            print("Broken location link for factoid %d: %s" % (factoid.factoidKey, factoid.engDesc))
            return None
        return get_location_node(session, factoid.locationInfo.location)
    elif factoid_type == "Occupation/Vocation":
        return session.run("MERGE (o:Occupation {identifier: '%s'}) RETURN o" % factoid.occupation).single()['o']
    elif factoid_type == "Possession":
        pass  # Need to see if these records make any sense
    elif factoid_type == "Religion":
        return session.run("MERGE (r:Religion {identifier:'%s'}) RETURN r" % factoid.religion).single()['r']
    else:
        print("Unhandled factoid type: %s" % factoid_type)
    return None


def get_qualifier_properties(factoid):
    props = []
    if factoid.notes:
        props.append("notes: '%s'" % escape_text(factoid.notes))
    if factoid.factoidType == "Ethnic label":
        # Is the ethnicity doubtful?
        props.append("certainty: %s" % ("true" if factoid.ethnicityInfo.isDoubtful == 0 else "false"))
    elif factoid.factoidType == "Kinship":
        # What is the given kin relationship?
        props.append("relation_general: '%s'" % escape_text(factoid.kinshipType.gunspecRelat))
        props.append("relation_specific: '%s'" % escape_text(factoid.kinshipType.gspecRelat))
    return " {%s}" % ', '.join(props) if len(props) else ""


def process_persons(personlist, graphdriver, agent, sexes, predicates):
    """Go through the relevant person records and process them for factoids"""
    processed = 0
    for person in personlist:
        # Skip the Anonymi for now
        if person.name is 'Anonymi':
            continue
        processed += 1
        # Create or find the person node
        print("Making/finding node for person %s %d" % (person.name, person.mdbCode))
        nodelookup = "MERGE (p:PBWPerson {name:'%s', code:%d}) RETURN p" % (person.name, person.mdbCode)
        with graphdriver.session() as session:
            graph_person = session.run(nodelookup).single()['p']
            # Add the assertions that are in the direct person record:
            # - sex
            for ftype in predicates:
                predicate = predicates[ftype]
                if ftype == 'sex':
                    uncertain = False
                    our_sex = person.sex
                    if our_sex == 'Mixed':  # we have already excluded Anonymi
                        our_sex = 'Unknown'
                    elif our_sex == '(Unspecified)':
                        our_sex = 'Unknown'
                    elif our_sex == 'Eunuch (Probable)':
                        our_sex = 'Eunuch'
                        uncertain = True
                    if uncertain:
                        assertion_props = ' {uncertain:true}'
                    else:
                        assertion_props = ''
                    if our_sex != "Unknown":
                        print("...setting sex to %s%s" % (our_sex, " (maybe)" if uncertain else ""))
                        sexassertion = "MATCH (p), (sp), (s), (pbw) WHERE id(p) = %d AND id(sp) = %d AND id(s) = %d " \
                                       "AND id(pbw) = %d MERGE (sp)<-[:PREDICATE]-(a:Assertion%s)-[:SUBJECT]->(p) " \
                                       "MERGE (pbw)<-[:AUTHORITY]-(a)-[:OBJECT]->(s) RETURN a" \
                                       % (graph_person.id, predicate.id, sexes[our_sex].id, agent.id, assertion_props)
                        session.run(sexassertion)
                elif ftype == 'disambiguation':
                    # - description / disambiguator
                    print("...setting disambiguating description to %s" % person.descName)
                    disamb_node = session.run("MERGE (d:Disambiguation {text:'%s'}) RETURN d"
                                              % escape_text(person.descName)).single()['d']
                    disassertion = "MATCH (p), (dp), (pbw), (d) WHERE id(p) = %d AND id(dp) = %d AND id(pbw) = %d AND "\
                                   "id(d) = %d MERGE (dp)<-[:PREDICATE]-(a:Assertion)-[:SUBJECT]->(p) MERGE (pbw)<-[" \
                                   ":AUTHORITY]-(a)-[:OBJECT]->(d) RETURN a" % (graph_person.id, predicate.id,
                                                                                agent.id, disamb_node.id)
                    session.run(disassertion)
                elif ftype == 'identified':
                    # - name/identifier
                    print("...setting identifier to %s" % person.nameOL)
                    ident_node = session.run("MERGE (i:Identifier {text:'%s'}) RETURN i").single()['i']
                    identassertion = "MATCH (p), (ip), (pbw), (i) WHERE id(p) = %d AND id(ip) = %d AND id(pbw) = %d " \
                                     "AND id(i) = %d MERGE (ip)<-[:PREDICATE]-(a:Assertion)-[:SUBJECT]->(p) MERGE (" \
                                     "pbw)<-[:AUTHORITY]-(a)-[:OBJECT]->(i) RETURN a" \
                                     % (graph_person.id, predicate.id, agent.id, ident_node.id)
                    session.run(identassertion)
                else:
                    # Now add the different sorts of explicit factoids
                    for f in person.main_factoids(ftype):
                        # The factoid has the same subject, the predicate we created, some object (which will depend
                        # on the factoid type), a source from which the information comes, and an authority who
                        # interpreted the source. The predicate might also have qualifier properties.
                        # First thing: collect the object in question.
                        obj = get_object_node(session, ftype, f, person)
                        if obj is None:
                            print("Skipping un-factoided %s on person %d (%s %d)"
                                  % (ftype, person.personKey, person.name, person.mdbCode))
                            continue
                        # Get the qualifier, if there is one
                        qualifier = get_qualifier_properties(f)
                        # Get the source and the authority
                        source_node = get_source_node(session, f)
                        factoid_auth = auth_map.get(f.source, [])
                        # Make sure the assertion itself exists, and then add the source and authorities.
                        # This lets us have multiple sources making the same assertion, or multiple notes on
                        # the assertion predicate.
                        doassertion = "MATCH (p), (dop), (do) WHERE id(p) = %d AND id(dop) = %d AND id(do) = %d MERGE "\
                                      "(do)<-[:OBJECT]-(a:Assertion)-[:SUBJECT]->(p) MERGE (a)-[:PREDICATE%s]->(dop) " \
                                      "RETURN DISTINCT a" % (graph_person.id, predicate.id, obj.id, qualifier)
                        a = session.run(doassertion).single()['a']
                        # Now add the source
                        session.run("MATCH (a), (src) WHERE id(a) = %d AND id(src) = %d MERGE (a)-[:SOURCE]->(src)"
                                    % (a.id, source_node.id))
                        for auth in factoid_auth:
                            auth_node = session.run("MERGE (ag:Agent {identifier:'%s'}) RETURN ag"
                                                    % auth).single()['ag']
                            session.run("MATCH (a), (ag) WHERE id(a) = %d AND id(ag) = %d MERGE (a)-[:AUTHORITY]->(ag)"
                                        % (a.id, auth_node.id))

    print("Processed %d person records." % processed)


# If we are running as main, execute the script
if __name__ == '__main__':
    # Connect to the SQL DB
    engine = create_engine('mysql+pymysql://' + config.dbstring)
    smaker = sessionmaker(bind=engine)
    mysqlsession = smaker()
    # Connect to the graph DB
    driver = GraphDatabase.driver(config.graphuri, auth=(config.graphuser, config.graphpw))
    # Get our authority map
    auth_map = get_authmap()
    # Make / retrieve the global nodes
    (pbwagent, pbwsexes, pbwpredicates) = setup_constants(mysqlsession, driver)
    # Process the person records
    process_persons(collect_person_records(mysqlsession), driver, pbwagent, pbwsexes, pbwpredicates)
    print("Done!")
