import pbw
import config
import re
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from neo4j import GraphDatabase


# Make our list of sources / authorities
def get_authmap():
    """Return, for the sources in PBW, a list of contemporary authors of those sources and a list of the
    scholars responsible for ingesting the source info into the database. Information on the latter is taken
    from https://pbw2016.kdl.kcl.ac.uk/ref/sources/ and https://pbw2016.kdl.kcl.ac.uk/ref/seal-editions/"""

    # These are the people in the PBW database credited with authorship of the source
    authors = {
        'Albert of Aachen': ['Albert', 26101],
        'Alexios Stoudites': ['Alexios', 11],
        'Anna Komnene': ['Anna', 62],
        'Aristakes': ['Aristakes', 101],
        'Attaleiates: Diataxis': ['Michael', 202],
        'Attaleiates: History': ['Michael', 202],
        'Basilakios, Orationes et epistulae': ['Nikephoros', 17003],
        'Basileios of Calabria to Nikolaos III': ['Basileios', 254],
        'Boilas': ['Eustathios', 105],
        'Bryennios': ['Nikephoros', 117],
        'Christophoros of Mitylene': ['Christophoros', 13102],
        'Chrysobull of 1079': ['Nikephoros', 3],
        'Clement III to Basileios of Calabria': ['Klemes', 23],
        'Domenico of Grado': ['Dominikos', 101],
        'Edict on Clergy Reform': ['Alexios', 1],
        'Edict on clergy reform': ['Alexios', 1],
        'Eustathios Romaios': ['Eustathios', 61],
        'Eustathios: Capture of Thessalonike': ['Eustathios', 20147],
        'Fulcher of Chartres': ['Fulcher', 101],
        'Glykas': ['Michael', 305],
        'Gregory VII, Epistolae vagantes': ['Gregorios', 27],
        'Gregory VII, in Caspar': ['Gregorios', 27],
        'Humbert, Commemoratio': ['Humbert', 101],
        'Humbert, Dialogus': ['Humbert', 101],
        'Humbert, Excommunication': ['Humbert', 101],
        'Ioannes Italos': ['Ioannes', 66],
        'Italikos': ['Michael', 20130],
        'Kekaumenos': ['Anonymus', 274],
        'Keroularios  ': ['Michael', 11],
        'Kinnamos': ['Ioannes', 17001],
        'Leo IX  ': ['Leon', 29],
        'Leon of Chalcedon': ['Leon', 114],
        'Leon of Ohrid (Greek)': ['Leon', 108],
        'Leon of Ohrid (Latin)': ['Leon', 108, 'Humbert', 101],
        'Manasses, Chronicle': ['Konstantinos', 302],
        'Manasses, Chronicle: Dedication': ['Konstantinos', 302],
        'Manasses, Hodoiporikon': ['Konstantinos', 302],
        'Manganeios Prodromos': ['Manganeios', 101],
        'Mauropous: Letters': ['Ioannes', 289],
        'Mauropous: Orations': ['Ioannes', 289],
        'Michael the Rhetor, Regel': ['Michael', 17004],
        'Michel, Amalfi': ['Laycus', 101],
        'Nicolas d\'Andida': ['Nikolaos', 257],
        'Nicole, Chartophylax': ['Alexios', 1],
        'Niketas Choniates, Historia': ['Niketas', 25001],
        'Niketas Stethatos (Darrouzes)': ['Niketas', 105],
        'Niketas Stethatos, On the Holy Spirit': ['Niketas', 105],
        'Nikolaos III to Urban II': ['Nikolaos', 13],
        'Oath of Eudokia': ['Eudokia', 1],
        'Odo of Deuil': ['Odo', 102],
        'Pakourianos': ['Gregorios', 61],
        'Paschal II, Letter to Alexios I': ['Paschales', 22],
        'Petros of Antioch  ': ['Petros', 103],
        'Petros of Antioch, ep. 2': ['Petros', 103],
        'Prodromos, Historische Gedichte': ['Theodoros', 25001],
        'Psellos': ['Michael', 61],
        'Psellos: Chronographia': ['Michael', 61],
        'Ralph of Caen': ['Radulf', 112],
        'Semeioma on Leon of Chalcedon': ['Alexios', 1],
        'Skylitzes': ['Ioannes', 110],
        'Skylitzes Continuatus': ['Anonymus', 102],
        'Theophylact of Ohrid, Speech to Alexios I': ['Theophylaktos', 105],
        'Theophylaktos of Ohrid, Letters': ['Theophylaktos', 105],
        'Tornikes, Georgios': ['Georgios', 25002],
        'Tzetzes, Exegesis of Homer': ['Ioannes', 459],
        'Tzetzes, Historiai': ['Ioannes', 459],
        'Tzetzes, Homerica': ['Ioannes', 459],
        'Tzetzes, Letters': ['Ioannes', 459],
        'Tzetzes, Posthomerica': ['Ioannes', 459],
        'Usama': ['Usama', 101],
        'Victor (pope)': ['Victor', 23],
        'Walter the Chancellor': ['Walter', 101],
        'William of Tyre': ['William', 4001],
        'Zetounion': ['Nikolaos', 13],
        'Zonaras': ['Ioannes', 6007]
    }

    # These are the modern scholars who put the source information into PBW records
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
    ok = 'Olga Karagiorgiou'

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
        'Cheynet, Antioche et Tarse': [ok],
        'Christophoros of Mitylene': [mj],
        'Christos Philanthropos, note': [mj],
        'Chrysobull of 1079': [tp],
        'Clement III to Basileios of Calabria': [jr],
        'Codice Diplomatico Barese IV': [mw],
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
        'Koltsida-Makri': [ok],
        'Kyrillos Phileotes': [tp],
        'Laurent, Corpus V.2': [ok],
        'Laurent, Corpus V.3': [ok],
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
        'Seibt – Zarnitz': [ok],
        'Semeioma on Leon of Chalcedon': [jr],
        'Sode, Berlin': [ok],
        'Skylitzes': [tp],
        'Skylitzes Continuatus': [tp],
        'Speck, Berlin': [ok],
        'Stavrakos': [ok],
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
        'Wassiliou, Hexamilites': [ok],
        'William of Tyre': [mj],
        'Xenophontos': [tp],
        'Xeropotamou': [tp],
        'Yahya al-Antaki': [tp, lo, hm],
        'Zacos II': [ok],
        'Zetounion': [jr],
        'Zonaras': [mw]
    }
    return {'authors': authors, 'authorities': authorities}


def escape_text(t):
    """Escape any single quotes or double quotes in strings that need to go into Neo4J properties"""
    return t.replace("'", "\\'").replace('"', '\\"')


def collect_person_records(sqlsession):
    """Get a list of people whose floruit matches our needs"""
    floruit = r'XI(?!I)|10[3-8]\d'
    relevant = [x for x in sqlsession.query(pbw.Person).all() if re.search(floruit, x.floruit) and len(x.factoids) > 0]
    print("Found %d relevant people" % len(relevant))
    # return relevant
    # Debugging / testing: restrict the list of relevant people
    debugnames = ['Herve', 'Ioannes', 'Konstantinos', 'Anna']
    debugcodes = [62, 68, 101, 102]
    return [x for x in relevant if x.name in debugnames and x.mdbCode in debugcodes]


def _init_typology(session, superclass, instances):
    """Initialize the typologies that are in the PBW database, knowing that the type names were not chosen
    for ease of variable expression. Returns a map of type name -> Neo4J node ID."""
    retmap = dict()
    # Batch these into groups of 100
    doloop = True
    while doloop:
        if len(instances) > 100:
            batch = instances[0:100]
            del instances[0:100]
        else:
            batch = instances
            doloop = False
        i = 0
        varmap = dict()
        cypherq = "MERGE (super:crm_E55_Type {value:\"%s\", constant:TRUE}) " % superclass
        for inst in batch:
            # Leave out blank instances
            if inst == '':
                continue
            var = "inst%d" % i
            i += 1
            varmap[var] = inst
            cypherq += "MERGE (%s:crm_E55_Type {value:\"%s\", constant:TRUE})-[:crm_P127_has_broader_term]->(super) " \
                       % (var, inst)
        cypherq += "RETURN %s" % ', '.join(["%s" % x for x in varmap.keys()])
        print(cypherq)
        types = session.run(cypherq).single()
        for k, v in types.items():
            retmap[varmap[k]] = v.id
    return retmap


def _smooth_labels(label):
    if label == 'Dignity/Office':
        return 'Dignity'
    if label == 'Occupation/Vocation':
        return 'Occupation'
    if label == 'Language Skill':
        return 'Language'
    if label == 'Ethnic label':
        return 'Ethnicity'
    if label == 'Second Name' or label == 'Alternative Name':
        return 'Appellation'
    if label == 'Uncertain Ident':
        return 'UncertainIdent'
    return label


def setup_constants(sqlsession, graphdriver):
    """Set up the necessary object and predicate nodes that will be shared by the individual records"""
    with graphdriver.session() as session:
        # Make our anonymous agent PBW for the un-sourced information
        generic_agent = session.run("MERGE (a:crm_E39_Actor {identifier:'PBW', constant:TRUE}) return a").single()['a']
        # Get the list of factoid types
        pbw_factoid_types = [x.typeName for x in sqlsession.query(pbw.FactoidType).all()
                             if x.typeName != '(Unspecified)']
        # Add the info that is directly in the person record
        pbw_pr_info = ['Gender', 'Disambiguation', 'Identifier']

        # Some of these factoid types have their own controlled vocabularies. Extract them here and
        # simplify the broader term.
        controlled_vocabs = dict()
        controlled_vocabs['Gender'] = _init_typology(session, 'Gender', ['Female', 'Male', 'Eunuch'])
        controlled_vocabs['Occupation'] = _init_typology(session, 'Occupation',
                                                         [x.occupationName for x
                                                          in sqlsession.query(pbw.Occupation).all()])
        # Not sure we will use this vocabulary as nodes...
        # controlled_vocabs['Kinship'] = _init_typology(session, 'Kinship',
        #                                               [x.gspecRelat for x in sqlsession.query(pbw.KinshipType).all()])
        controlled_vocabs['Ethnicity'] = _init_typology(session, 'Ethnicity',
                                                        [x.ethName for x in sqlsession.query(pbw.Ethnicity).all()])
        controlled_vocabs['Religion'] = _init_typology(session, 'Religion',
                                                       [x.religionName for x in sqlsession.query(pbw.Religion).all()])
        # Dignities in PBW tend to be specific to institutions / areas;
        # make an initial selection by breaking on the 'of'
        all_dignities = set()
        for x in sqlsession.query(pbw.DignityOffice).all():
            if ' of the ' in x.stdName:  # Don't split (yet) titles that probably don't refer to places
                dignity = [x.stdName]
            else:
                dignity = x.stdName.split(' of ')
            all_dignities.add(dignity[0])
        controlled_vocabs['Dignity'] = _init_typology(session, 'Dignity', list(all_dignities))

        # Language has its own subtype so handle this separately
        langnodes = {}
        for x in sqlsession.query(pbw.LanguageSkill).all():
            lang = x.languageName
            cypherq = "MERGE (lang:crm_E56_Language {value:\"%s\", constant:TRUE}) RETURN lang"
            result = session.run(cypherq).single()
            langnodes[lang] = result['lang']
        controlled_vocabs['Language'] = langnodes

        # Set up the predicates that we will be using
        our_predicates = [
            'crm_P1_is_identified_by',
            'crm_P3_has_note',
            'crm_P4_has_time_span',
            'crm_P14_carried_out_by',
            'crm_P41_classified',
            'crm_P94_has_created',
            'crm_P100_was_death_of',
            'crm_P107_has_current_or_former_member',
            'crm_P128_carries',
            'crm_P165_incorporates'
        ]
        prednodes = dict()
        for pred in our_predicates:
            # Make the predicate node
            npred = session.run("MERGE (n:%s {constant:TRUE}) RETURN n" % pred).single()['n']
            # Store the predicates by their short code
            predcode = pred.replace('crm_', '')
            predcode = predcode.split('_')[0]
            prednodes[predcode] = npred.id
        controlled_vocabs['Predicates'] = prednodes

    return generic_agent, pbw_factoid_types, pbw_pr_info, controlled_vocabs


def _create_assertion_query(subj, pred, obj, auth, src, var="a"):
    st = "COMMAND (%s:crm_E13_Attribute_Assignment)-[:crm_P140_assigned_attribute_to]->(%s), " % (var, subj)
    st += "(%s)-[:crm_P177_assigned_property_type]->(%s), " % (var, pred)
    st += "(%s)-[:crm_P141_assigned]->(%s), " % (var, obj)
    if auth is not None:
        st += "(%s)-[:crm_P14_carried_out_by]->(%s) " % (var, auth)
    if src is not None:
        st += ", (%s)-[:crm_P70r_is_documented_in]->(%s) " % (var, src)
    return st


def gender_handler(graphdriver, agent, sqlperson, graphperson, constants):
    with graphdriver.session() as session:
        uncertain = False
        pbw_sex = sqlperson.sex
        if pbw_sex == 'Mixed':  # we have already excluded Anonymi
            pbw_sex = 'Unknown'
        elif pbw_sex == 'Eunach':  # correct misspelling in source DB
            pbw_sex = 'Eunuch'
        elif pbw_sex == '(Unspecified)':
            pbw_sex = 'Unknown'
        elif pbw_sex == 'Eunuch (Probable)':
            pbw_sex = 'Eunuch'
            uncertain = True
        if uncertain:
            assertion_props = ' {uncertain:true}'
        else:
            assertion_props = ''
        if pbw_sex != "Unknown":
            print("...setting gender assignment to %s%s" % (pbw_sex, " (maybe)" if uncertain else ""))
            # Make the event tied to this person
            genderassertion = "MATCH (p), (s), (pbw), (sp41) " \
                              "WHERE id(p) = %d AND id(s) = %d AND id(pbw) = %d AND id(sp41) = %d " % \
                              (graphperson.id, constants[pbw_sex], agent.id, constants['P41'])
            genderassertion += "MERGE (sp42:crm_P42_assigned%s) " % assertion_props
            genderassertion += "WITH p, s, pbw, sp41, sp42 "
            genderassertion += _create_assertion_query('ga:crm_E17_Type_Assignment', 'sp41', 'p', 'pbw', None, 'a1')
            genderassertion += _create_assertion_query('ga', 'sp42', 's', 'pbw', None, 'a2')
            genderassertion += "RETURN a1, a2"
            # print(genderassertion % (graphperson.id, constants[pbw_sex], agent.id, assertion_props))
            result = session.run(genderassertion.replace('COMMAND', 'MATCH')).single()
            if result is None:
                session.run(genderassertion.replace('COMMAND', 'CREATE'))


def identifier_handler(graphdriver, agent, sqlperson, graphperson, constants):
    """The identifier in this context is the 'origName' field, thus an identifier assigned by PBW
    not on the basis of any particular source"""
    with graphdriver.session() as session:
        idassertion = "MATCH (p), (pbw), (pred) WHERE id(p) = %d AND id(pbw) = %d AND id(pred) = %d " \
                      % (graphperson.id, agent.id, constants['P1'])
        idassertion += "MERGE (app:crm_E41_Appellation {value: \"%s\"}) " % sqlperson.nameOL
        idassertion += "WITH p, pbw, pred, app "
        idassertion += _create_assertion_query('p', 'pred', 'app', 'pbw', None)
        idassertion += "RETURN a"
        result = session.run(idassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            return session.run(idassertion.replace('COMMAND', 'CREATE'))


def disambiguation_handler(graphdriver, agent, sqlperson, graphperson, constants):
    """The short description of the person provided by PBW"""
    with graphdriver.session() as session:
        disassertion = "MATCH (p), (pbw), (pred) WHERE id(p) = %d AND id(pbw) = %d AND id(pred) = %d " % \
                       (graphperson.id, agent.id, constants['P3'])
        disassertion += "MERGE (desc:crm_E62_String {value:\"%s\"}) " % escape_text(sqlperson.descName)
        disassertion += "WITH p, pred, desc, pbw "
        disassertion += _create_assertion_query('p', 'pred', 'desc', 'pbw', None)
        disassertion += "RETURN a"
        result = session.run(disassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            return session.run(disassertion.replace('COMMAND', 'CREATE'))


def _find_or_create_event(graphdriver, person, crm_eventclass, crm_predicate):
    """Helper function to find the relevant event for event-based factoids"""
    with graphdriver.session() as session:
        query = "MATCH (pers), (pred) WHERE id(pers) = %d AND id(pred) = %d " % (person.id, crm_predicate)
        query += "MATCH (a:crm_E13_Attribute_Assignment)-[:crm_P140_assigned_attribute_to]->(event:%s), " % crm_eventclass
        query += "(a)-[:crm_P177_assigned_property_type]->(pred), "
        query += "(a)-[:crm_P141_assigned]->(pers) "
        query += "RETURN DISTINCT event"  # There may well be multiple assertions about this death
        result = session.run(query).single()
        if result is None:
            # If we don't have this event tied to this person yet, create a new event of the
            # given class and return it for use in the assertion being made about it.
            result = session.run("CREATE (event:%s) RETURN event" % crm_eventclass).single()
    return result['event']


def get_source_and_agent(session, factoid, predicates):
    """Returns a node that represents the source for this factoid. Creates the network of nodes and
    relationships to describe that source, if necessary. The source will either be a physical E22 Human-Made Object
    (the boulloterion) or an E31 Document (the written primary source)."""
    # Is this a 'seals' source without a boulloterion? If so warn and return None
    authoritylist = get_authmap()
    author = None
    if authoritylist["authorities"].get(factoid.source) is None:
        if factoid.source != 'Seals' or factoid.boulloterion is None:
            print("No boulloterion found for seal-sourced factoid %d" % factoid.factoidKey
                  if factoid.source == 'Seals'
                  else "Source %s of factoid %d not known" % (factoid.source, factoid.factoidKey))
            return None, None
    if factoid.boulloterion is not None:
        # This factoid is taken from a seal inscription. Let's pull that out into CRM objects.
        # If the boulloterion has no associated publications, we shouldn't use it.
        if len(factoid.boulloterion.publication) == 0:
            print("No published source found for boulloterion %d" % factoid.boulloterion.boulloterionKey)
            return None, None
        # First find who did the analysis
        alist = set()
        for pub in factoid.boulloterion.publication:
            # If the publication isn't in the authority list, Michael analysed it
            if pub.bibSource is not None:
                thispubauth = authoritylist["authorities"].get(pub.bibSource.shortName, ["Michael Jeffreys"])
                alist.update(thispubauth)
        agent = get_authority_node(session, list(alist))
        # Then get the node that points to the boulloterion's sources
        srclist = get_boulloterion_sourcelist(session, factoid.boulloterion)
        if srclist is not None:
            q = "MATCH (pred), (agent), (srclist) WHERE id(pred) = %d AND id(agent) = %d AND id(srclist) = %d " \
                % (predicates['P128'], agent.id, srclist.id)
        else:
            q = "MATCH (pred), (agent) WHERE id(pred) = %d AND id(agent) = %d " % (predicates['P128'], agent.id)
        # boulloterion is an E22 Human-Made Object
        q += "MERGE (boul:crm_E22_Boulloterion {reference:%s}) " % factoid.boulloterion.boulloterionKey
        # which is asserted by the agent to P128 carry an E34 Inscription (we can even record the inscription)
        q += "MERGE (src:crm_E34_Inscription:crm_E31_Document {text:\"%s\"}) " % factoid.boulloterion.origLText
        q += "WITH boul, pred, src, agent%s " % (", srclist" if srclist else "")
        q += _create_assertion_query("boul", "pred", "src", "agent", "srclist" if srclist else None)
        # MAYBE LATER: which is asserted by MJ to P108 have produced various E22 Human-Made Objects (the seals)
        # - which seals are asserted by the collection authors (with pub source) to also carry the inscriptions?
    else:
        # This factoid is taken from a document.
        # Do we have a known author for this text?
        author = get_author_node(session, authoritylist['authors'].get(factoid.source))
        # If not, we use the PBW scholar as the authority.
        agent = get_authority_node(session, authoritylist['authorities'].get(factoid.source))
        # If there is no PBW scholar known for this source, we use the generic PBW agent.
        if agent is None:
            agent = pbwagent
        # Now we find an E31 Document (the whole work), its author (if any), and the PBW scholar who analyzed it
        work = get_source_work(session, factoid, author, predicates)
        q = "MATCH (work), (agent), (p165) WHERE id(work) = %d AND id(agent) = %d AND id(p165) = %d " % (
            work.id, agent.id, predicates['P165'])
        q += "MERGE (src:crm_E31_Passage {reference:'%s', text:'%s'}) " % \
             (escape_text(factoid.sourceRef), escape_text(factoid.origLDesc))
        q += "WITH work, p165, src, agent "
        q += _create_assertion_query("work", "p165", "src", "agent", None)
    q += "RETURN src"  # SOMEDAY work out why this query needs a 'distinct'
    source_result = session.run(q.replace('COMMAND', 'MATCH')).single()
    if source_result is None:
        source_result = session.run(q.replace('COMMAND', 'CREATE')).single()
    return source_result['src'], author if author else agent


def get_boulloterion_sourcelist(session, boulloterion):
    """A helper function to create the list of publications where the seals allegedly produced by a
    given boulloterion were published. Returns either a single E31 Document (if there was a single
    publication) or an E73 Information Object that represents a group of Documents."""
    if len(boulloterion.publication) == 0:
        return None
    i = 0
    q = ""
    for pub in boulloterion.publication:
        # Make sure with 'merge' that each publication node exists
        q += "MERGE (src%d:crm_E31_Document {identifier:'%s', reference:'%s'}) " % \
            (i, pub.bibSource.shortName, pub.publicationRef)
        i += 1
    if i > 1:
        # Check to see whether we have a matching group with only these publication nodes.
        # Teeeeechnically speaking, an Information Object cannot P70 document anything, so
        # we also assign E31 Document as a stopgap since the group of sources, taken together,
        # documents something.
        parts = []
        retvar = "srcgrp"
        q += "WITH %s " % ", ".join(["src%d" % x for x in range(i)])
        q += "MATCH (srcgrp:crm_E73_Information_Object:crm_E31_Document) " \
             "WHERE size((srcgrp)-[:crm_P165_incorporates]->(:crm_E31_Document)) = %d " % i
        for n in range(i):
            parts.append("(srcgrp)-[:crm_P165_incorporates]->(src%d)" % n)
        q += "MATCH " + ", ".join(parts) + " "
    else:
        # We simply return the one node we created
        retvar = "src0"
    q += "RETURN %s" % retvar
    ret = session.run(q).single()
    if ret is None:
        # The plural sources exist, but the source group doesn't. Create it
        i = 0
        matchparts = []
        createparts = ["(srcgrp:crm_E73_Information_Object:crm_E31_Document)"]
        for pub in boulloterion.publication:
            matchparts.append("(src%d:crm_E31_Document {identifier:'%s', reference:'%s'}) " % (
                i, pub.bibSource.shortName, pub.publicationRef))
            createparts.append("(srcgrp)-[:crm_P165_incorporates]->(src%d)" % i)
            i += 1
        q = "MATCH " + ", ".join(matchparts) + " "
        q += "CREATE " + ", ".join(createparts) + " "
        q += "RETURN srcgrp"
        ret = session.run(q).single()
    return ret[retvar]


def get_source_work(session, factoid, author, predicates):
    # Ensure the existence of the work and, if it has a declared author, link the author to it via
    # a CREATION event, asserted by TLA
    q = "MERGE (work:crm_E31_Work {identifier:'%s'}) " % factoid.source
    if author is not None:
        # Ensure the existence of the assertions that the author authored the work
        tla = get_authority_node(session, ['Tara Andrews'])
        q += "WITH work "
        q += "MATCH (author), (tla), (p14), (p94) " \
            "WHERE id(author) = %d AND id(tla) = %d AND id(p14) = %d AND id(p94) = %d " % (
                author.id, tla.id, predicates['P14'], predicates['P94'])
        q += _create_assertion_query('aship:crm_E65_Creation', 'p14', 'work', 'tla', None, 'a1')
        q += _create_assertion_query('aship', 'p94', 'author', 'tla', None, 'a2')
    q += "RETURN work"
    work_result = session.run(q.replace('COMMAND', 'MATCH')).single()
    if work_result is None:
        work_result = session.run(q.replace('COMMAND', 'CREATE')).single()
    return work_result['work']


def get_author_node(session, authorlist):
    """Return the E21 Person node for the author of a text, or a group of authors if authorship was composite"""
    if authorlist is None or len(authorlist) == 0:
        return None
    if len(authorlist) == 2:  # It is a single name and mdbCode
        return _find_or_create_graphperson(session, pbwagent, authorlist[0], authorlist[1])
    # It is our multi-authored text. Make a group because both authors share authority.
    authors = []
    while len(authorlist) > 0:
        pname = authorlist.pop(0)
        pcode = authorlist.pop(1)
        authors.append(_find_or_create_graphperson(session, pbwagent, pname, pcode))
    return _find_or_create_authority_group(session, authors)


def get_authority_node(session, authoritylist):
    if authoritylist is None or len(authoritylist) == 0:
        return None
    if len(authoritylist) == 1:
        return session.run("MERGE (p:crm_E21_Person {identifier:'%s'}) RETURN p" % authoritylist[0]).single()['p']
    # If we get here, we have more than one authority for this source.
    # Ensure the existence of the people, and then ensure the existence of their group
    scholars = []
    for p in authoritylist:
        scholars.append(session.run("MERGE (p:crm_E21_Person {identifier:'%s'}) RETURN p" % p).single()['p'])
    return _find_or_create_authority_group(session, scholars)


def _find_or_create_authority_group(session, members):
    mc = []
    wc = []
    gc = []
    i = 1
    for a in members:
        mc.append("(a%d)" % i)
        wc.append("id(a%d) = %d" % (i, a.id))
        gc.append("(group)-[:crm_P107_has_current_or_former_member]->(a%d)" % i)
        i += 1
    q = "MATCH " + ', '.join(mc) + " WHERE " + " AND ".join(wc) + " "
    q += "%s (group:crm_E74_Group), " + ", ".join(gc) + " RETURN group"
    g = session.run(q % "MATCH").single()
    if g is None:
        g = session.run(q % "CREATE").single()
    return g['group']


def death_handler(graphdriver, sourcenode, agent, factoid, graphperson, constants):
    with graphdriver.session() as session:
        # Each factoid is its own set of assertions pertaining to the single death of a person.
        # When there are multiple sources, we will have to review them for consistency and make
        # proxies for the death event as necessary.
        # See if we can find the death event
        deathevent = _find_or_create_event(graphdriver, graphperson, 'crm_E69_Death', constants['P100'])
        # Create the new assertion that says the death happened. Start by gathering all our existing
        # nodes and reified predicates:
        # - the person
        # - the agent
        # - the source
        # - the event node
        # - the main event predicate
        # - the event description predicate
        # - the event dating predicate
        if factoid.deathRecord is None:
            print("Someone has a death factoid (%d, \"%s\") without a death record! Go check it out."
                  % (factoid.factoidKey, factoid.engDesc))
        else:
            # TODO parse this later into a real date range
            deathdate = factoid.deathRecord.sourceDate
            if deathdate == '':
                deathdate = None
        deathassertion = "MATCH (p), (agent), (source), (devent), (p100), (p3), (p4) " \
                         "WHERE id(p) = %d AND id(agent) = %d AND id(source) = %d AND id(devent) = %d " \
                         "AND id(p100) = %d AND id(p3) = %d AND id(p4) = %d " \
                         % (graphperson.id, agent.id, sourcenode.id, deathevent.id, constants['P100'],
                            constants['P3'], constants['P4'])
        deathassertion += "MERGE (desc:crm_E62_String {content:\"%s\"}) " % escape_text(factoid.replace_referents())
        if deathdate is not None:
            deathassertion += "MERGE (datedesc:crm_E52_Time_Span {content:\"%s\"}) " % deathdate
        deathassertion += "WITH p, agent, source, devent, p100, p3, p4, desc%s " % (', datedesc' if deathdate else '')
        deathassertion += _create_assertion_query('devent', 'p100', 'p', 'agent', 'source')
        # Create an assertion about how the death is described
        deathassertion += _create_assertion_query('devent', 'p3', 'desc', 'agent', 'source', 'a1')
        # Create an assertion about when the death happened.
        if deathdate is not None:
            deathassertion += _create_assertion_query('devent', 'p4', 'datedesc', 'agent', 'source', 'a2')
        deathassertion += "RETURN a, a1%s" % (", a2" if deathdate else '')

        print(deathassertion)
        result = session.run(deathassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(deathassertion.replace('COMMAND', 'CREATE'))


def _find_or_create_group(graphdriver, idlabel, category):
    with graphdriver.session() as session:
        groupassertion = "MATCH (l:crm_E55_Type {value:\"%s\"})-[:crm_P127_has_broader_term]" \
                         "->(super:crm_E55_Type {value:\"%s\"}) " % (idlabel, category)
        groupassertion += "MERGE (g:crm_E74_Group)-[:crm_P1_is_identified_by]->(l) RETURN g"
        result = session.run(groupassertion).single()
    return result['g']


def _assign_group_membership(graphdriver, sourcenode, agent, graphperson, constants, grouptype, grouplabel):
    with graphdriver.session() as session:
        # Get the ethnic group in question
        groupnode = _find_or_create_group(graphdriver, grouplabel, grouptype)
        # Need the person, the group
        gassertion = "MATCH (p), (agent), (source), (group), (mpred)  " \
                     "WHERE id(p) = %d AND id(agent) = %d AND id(source) = %d AND id(group) = %d " \
                     "AND id(mpred) = %d " % (graphperson.id, agent.id, sourcenode.id, groupnode.id, constants['P107'])
        gassertion += _create_assertion_query('group', 'mpred', 'p', 'agent', 'source')
        gassertion += "RETURN a"
        result = session.run(gassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(gassertion.replace('COMMAND', 'CREATE'))


def ethnicity_handler(graphdriver, sourcenode, agent, factoid, graphperson, constants):
    if factoid.ethnicityInfo is None or factoid.ethnicityInfo.ethnicity is None:
        # We can't assign any ethnicity without the ethnicity info
        print("Empty ethnicity factoid found: id %s" % factoid.factoidKey)
        return
    elabel = factoid.ethnicityInfo.ethnicity.ethName
    _assign_group_membership(graphdriver, sourcenode, agent, graphperson, constants, 'Ethnicity', elabel)


def religion_handler(graphdriver, sourcenode, agent, factoid, graphperson, constants):
    if factoid.religion is None:
        print("Empty religion factoid found: id %d" % factoid.factoidKey)
        return
    rlabel = factoid.religion
    # Special case
    if factoid.religion == '':
        rlabel = 'Heretic'
    _assign_group_membership(graphdriver, sourcenode, agent, graphperson, constants, 'Religion', rlabel)


def description_handler(graphdriver, sourcenode, agent, factoid, graphperson, constants):
    # Get the descriptions and the relevant languages
    langdesc = {'en': escape_text(factoid.replace_referents())}
    langkey = {2: 'gr', 3: 'la', 4: 'ar'}
    if factoid.origLang != '(Unspecified)':
        langdesc[langkey[factoid.oLangKey]] = escape_text(factoid.origLDesc)
    descattributes = []
    for k, v in langdesc.items():
        descattributes.append('%s: \"%s\"' % (k, v))
    # Make the query
    with graphdriver.session() as session:
        descassertion = "MATCH (p), (agent), (source), (pred) " \
                        "WHERE id(p) = %d AND id(agent) = %d AND id (source) = %d AND id(pred) = %d " % \
                        (graphperson.id, agent.id, sourcenode.id, constants['P3'])
        descassertion += 'MERGE (desc:crm_E62_String {%s}) ' % ','.join(descattributes)
        descassertion += "WITH p, pred, desc, agent, source "
        descassertion += _create_assertion_query('p', 'pred', 'desc', 'agent', 'source')
        descassertion += 'RETURN a'
        result = session.run(descassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(descassertion.replace('COMMAND', 'CREATE'))


def _find_or_create_kinship(session, graphperson, graphkin, source):
    # See if there is an existing kinship group between the person and their kin according to the
    # source in question. UNUSED
    kinquery = "MATCH (p), (kin), (source) WHERE id(p) = %d AND id(kin) = %d AND id(source) = %d " \
               % (graphperson.id, graphkin.id, source.id)
    kinquery += "COMMAND (a1:crm_E13_attribute_assignment)-[:crm_p140_assigned_attribute_to]->(kg:crm_E74_Kinship_Group), "
    kinquery += "(a1)-[:crm_p141_assigned]->(p), (a1)-[:crm_P70r_is_documented_in]->(source) "
    kinquery += "COMMAND (a2:crm_E13_attribute_assignment)-[:crm_p140_assigned_attribute_to]->(kg), "
    kinquery += "(a2)-[:crm_p141_assigned]->(kin), (a2)-[:crm_P70r_is_documented_in]->(source) RETURN kg"
    result = session.run(kinquery.replace('COMMAND', 'MATCH')).single()
    if result is None:
        result = session.run(kinquery.replace('COMMAND', 'CREATE')).single()
    return result['kg']


def kinship_handler(graphdriver, sourcenode, agent, factoid, graphperson, constants):
    # Kinships are modeled as two-person groups with their own separate label (because, honestly)
    # and with .1 types as property attributes as per the CRM spec.
    if factoid.kinshipType is None:
        print("Empty kinship factoid found: id %d" % factoid.factoidKey)
        return
    with graphdriver.session() as session:
        for kin in factoid.referents():
            graphkin = _find_or_create_graphperson(session, agent, kin.name, kin.mdbCode)
            kinassertion = "MATCH (p), (kin), (agent), (source) " \
                           "WHERE id(p) = %d AND id(kin) = %d AND id(agent) = %d AND id(source) = %d " \
                           % (graphperson.id, graphkin.id, agent.id, sourcenode.id)
            kinassertion += "MERGE (kp1:crm_P107_has_current_or_former_member {type:\"%s\"}) " \
                            "MERGE (kp2:crm_P107_has_current_or_former_member {type:\"%s_inverse\"}) " \
                            % (factoid.kinshipType.gunspecRelat, factoid.kinshipType.gunspecRelat)
            kinassertion += "CREATE (kg:crm_E74_Kinship_Group) "
            kinassertion += "WITH kg, kp1, kp2, p, kin, agent, source "
            kinassertion += _create_assertion_query('kg', 'kp1', 'p', 'agent', 'source', 'a1')
            kinassertion += _create_assertion_query('kg', 'kp2', 'kin', 'agent', 'source', 'a2')
            kinassertion += "RETURN a1, a2"
            result = session.run(kinassertion.replace('COMMAND', 'MATCH')).single()
            if result is None:
                session.run(kinassertion.replace('COMMAND', 'CREATE'))


def _find_or_create_graphperson(session, agent, name, code):
    nodelookup = "MATCH (pbw) WHERE id(pbw) = %d " \
                 "MERGE (idlabel:crm_E42_Identifier {value:'%s %d'}) " \
                 "MERGE (pbw)<-[:crm_P14_carried_out_by]-(idass:crm_E15_Identifier_Assignment)" \
                 "-[:crm_P37_assigned]->(idlabel) " \
                 "MERGE (idass)-[:crm_P140_assigned_attribute_to]->(p:crm_E21_Person) RETURN p" % \
                 (agent.id, name, code)
    graph_person = session.run(nodelookup).single()['p']
    return graph_person


def get_location_node(session, pbwloc):
    # The location record has an identifer, plus a couple of assertions by Charlotte about its
    # correspondence in the GeoNames and/or Pleiades database.
    loc_query = session.run("MATCH (l:crm_E53_Place {identifier: '%s', pbwid: %d}) RETURN l"
                            % (escape_text(pbwloc.locName), pbwloc.locationKey)).single()
    if loc_query is None:
        # We need to create it.
        loc_node = session.run("CREATE (l:crm_E53_Place {identifier: '%s', pbwid: %d}) RETURN l"
                               % (escape_text(pbwloc.locName), pbwloc.locationKey)).single()['l']
        for db in ("pleiades", "geonames"):
            dbid = pbwloc.__getattribute__("%s_id" % db)
            if dbid is not None:
                ag = session.run("MERGE (ag:crm_E39_Actor {identifier:'Charlotte Roueché'}) RETURN ag").single()['ag']
                dbloc = session.run("MERGE (l:crm_E94_Space_Primitive {db: '%s', id: %d}) "
                                    "RETURN l" % (db, dbid)).single()['l']
                pred = session.run(
                    "MERGE (p:crm_E55_Type {property:'crm_P168_place_is_defined_by'}) RETURN p").single()['p']
                session.run(
                    "MATCH (l), (ag), (dbl), (p) WHERE id(l) = %d AND id(ag) = %d AND id(dbl) = %d AND id(p) = %d "
                    "MERGE (a:crm_E13_Attribute_Assignment)-[:crm_P140_assigned_attribute_to]->(l) "
                    "MERGE (a)-[:crm_P177_assigned_property_type]->(p) MERGE (a)-[:crm_P141_assigned]->(dbl) "
                    "MERGE (a)-[:crm_P14_carried_out_by]->(ag)" % (loc_node.id, ag.id, dbloc.id, pred.id))
    else:
        loc_node = loc_query['l']
    return loc_node


def process_persons(personlist, graphdriver, pbwagent, pbwfactoids, pbwrecordinfo, pbwvocabs):
    """Go through the relevant person records and process them for factoids"""
    processed = 0
    for person in personlist:
        # Skip the anonymous groups for now
        if person.name == 'Anonymi':
            continue
        processed += 1
        # Create or find the person node
        print("Making/finding node for person %s %d" % (person.name, person.mdbCode))
        # In theory this should be an E14 Identifier Assignment. Start the merge from the
        # specific information we have, which is the identifier itself.
        with graphdriver.session() as session:
            graph_person = _find_or_create_graphperson(session, pbwagent, person.name, person.mdbCode)

        # Get the 'factoids' that are directly in the person record
        for ftype in pbwrecordinfo:
            ourftype = _smooth_labels(ftype)
            ourvocab = pbwvocabs.get(ourftype, dict())
            ourvocab.update(pbwvocabs.get('Predicates'))
            try:
                method = eval("%s_handler" % ourftype.lower())
                method(graphdriver, pbwagent, person, graph_person, ourvocab)
            except NameError:
                print("No handler for %s record info; skipping." % ourftype)

        # Now get the factoids that are really factoids
        for ftype in pbwfactoids:
            ourftype = _smooth_labels(ftype)
            ourvocab = pbwvocabs.get(ourftype, dict())
            ourvocab.update(pbwvocabs.get('Predicates'))
            try:
                method = eval("%s_handler" % ourftype.lower())
                for f in person.main_factoids(ftype):
                    # Get the source, either a text passage or a seal inscription, and the authority
                    # for the factoid. Authority will either be the author of the text, or the PBW
                    # colleague who read the text and ingested the information.
                    (source_node, authority_node) = get_source_and_agent(session, f, pbwvocabs.get('Predicates'))
                    # If the factoid has no source then we skip it
                    if source_node is None:
                        print("Skipping factoid %d without a traceable source" % f.factoidKey)
                        continue
                    # If the factoid has no authority then we assign it to the generic PBW agent
                    if authority_node is None:
                        authority_node = pbwagent
                    # Call the handler for this factoid type
                    method(graphdriver, source_node, authority_node, f, graph_person, ourvocab)

            except NameError:
                print("No handler for %s factoids; skipping." % ftype)

    print("Processed %d person records." % processed)


# If we are running as main, execute the script
if __name__ == '__main__':
    # Connect to the SQL DB
    engine = create_engine('mysql+pymysql://' + config.dbstring)
    smaker = sessionmaker(bind=engine)
    mysqlsession = smaker()
    # Connect to the graph DB
    driver = GraphDatabase.driver(config.graphuri, auth=(config.graphuser, config.graphpw))
    # Make / retrieve the global nodes and constants
    (pbwagent, pbwfactoids, pbwrecordinfo, pbwvocabs) = setup_constants(mysqlsession, driver)
    # Process the person records
    process_persons(collect_person_records(mysqlsession), driver, pbwagent, pbwfactoids, pbwrecordinfo, pbwvocabs)
    print("Done!")
