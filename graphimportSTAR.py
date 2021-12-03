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
    """Get a list of people whose floruit matches our needs"""
    floruit = r'XI(?!I)|10[3-8]\d'
    relevant = [x for x in sqlsession.query(pbw.Person).all() if re.search(floruit, x.floruit) and len(x.factoids) > 0]
    print("Found %d relevant people" % len(relevant))
    # Debugging / testing: restrict the list of relevant people
    debugnames = ['Herve', 'Ioannes', 'Konstantinos', 'Anna']
    debugcodes = [62, 68, 101, 102]
    return [x for x in relevant if x.name in debugnames and x.mdbCode in debugcodes]
    # return relevant


def _init_typology(graphdriver, superclass, instances):
    """Initialize the typologies that are in the PBW database, knowing that the type names were not chosen
    for ease of variable expression. Returns a map of type name -> Neo4J node."""
    with graphdriver.session() as session:
        cypherq = "MERGE (super:crm_E55_Type {value:\"%s\"}) " % superclass
        i = 0
        varmap = dict()
        for inst in instances:
            # Leave out blank instances
            if inst == '':
                continue
            var = "inst%d" % i
            i += 1
            varmap[var] = inst
            cypherq += "MERGE (%s:crm_E55_Type {value:\"%s\"}) " \
                       "MERGE (%s)-[:P127_has_broader_term]->(super) " % (var, inst, var)
        cypherq += "RETURN %s" % ', '.join(["%s" % x for x in varmap.keys()])
        print(cypherq)
        types = session.run(cypherq).single()
    retmap = {varmap[k]:v.id for (k, v) in types.items()}
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
        generic_agent = session.run("MERGE (a:E39_Actor {identifier:'PBW'}) return a").single()['a']
        # Get the list of factoid types
        pbw_factoid_types = [x.typeName for x in sqlsession.query(pbw.FactoidType).all()
                             if x.typeName != '(Unspecified)']
        # Add the info that is directly in the person record
        pbw_pr_info = ['Gender', 'Disambiguation', 'Identifier']

        # Some of these factoid types have their own controlled vocabularies. Extract them here and
        # simplify the broader term.
        controlled_vocabs = dict()
        controlled_vocabs['Gender'] = _init_typology(graphdriver, 'Gender', ['Female', 'Male', 'Eunuch'])
        controlled_vocabs['Occupation'] = _init_typology(graphdriver, 'Occupation',
                                                         [x.occupationName for x
                                                          in sqlsession.query(pbw.Occupation).all()])
        controlled_vocabs['Kinship'] = _init_typology(graphdriver, 'Kinship',
                                                      [x.gspecRelat for x in sqlsession.query(pbw.KinshipType).all()])
        # controlled_vocabs['Dignity'] = _init_typology(graphdriver, 'Dignity',
        #                                               [x.stdName for x in sqlsession.query(pbw.DignityOffice).all()])
        controlled_vocabs['Ethnicity'] = _init_typology(graphdriver, 'Ethnicity',
                                                        [x.ethName for x in sqlsession.query(pbw.Ethnicity).all()])
        controlled_vocabs['Language'] = _init_typology(graphdriver, 'Language',
                                                       [x.languageName for x
                                                        in sqlsession.query(pbw.LanguageSkill).all()])
        controlled_vocabs['Religion'] = _init_typology(graphdriver, 'Religion',
                                                       [x.religionName for x in sqlsession.query(pbw.Religion).all()])

        # Set up the predicates that we will be using
        our_predicates = [
            'crm_P1_is_identified_by',
            'crm_P41_classified',
            'crm_P100_was_death_of',
            'crm_P3_has_note',
            'crm_P4_has_time_span'
        ]
        prednodes = dict()
        for pred in our_predicates:
            npred = session.run("MERGE (n:`%s`) RETURN n").single()['n']
            prednodes[pred] = npred.id
        controlled_vocabs['Predicates'] = prednodes

    return generic_agent, pbw_factoid_types, pbw_pr_info, controlled_vocabs


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
            genderassertion = "MATCH (p), (s), (pbw) WHERE id(p) = %d AND id(s) = %d AND id(pbw) = %d "
            genderassertion += "MERGE (sp42:crm_P42_assigned%s) MERGE (sp41:crm_P41_classified) "
            genderassertion += "CREATE (a1:crm_E13_Attribute_Assignment)-[:crm_P140_assigned_attribute_to]" \
                               "->(ga:crm_E17_Type_Assignment), "
            genderassertion += "(a1)-[:crm_P177_assigned_property_type]->(sp41), (a1)-[:crm_P141_assigned]->(p), "
            genderassertion += "(a1)-[:crm_P14_carried_out_by]->(pbw) "
            genderassertion += "CREATE (a2:crm_E13_Attribute_Assignment)-[:crm_P140_assigned_attribute_to]" \
                               "->(ga), "
            genderassertion += "(a2)-[:crm_P177_assigned_property_type]->(sp42), (a2)-[:crm_P141_assigned]->(s), "
            genderassertion += "(a2)-[:crm_P14_carried_out_by]->(pbw)"
            print(genderassertion % (graphperson.id, constants[pbw_sex], agent.id, assertion_props))
            session.run(genderassertion % (graphperson.id, constants[pbw_sex], agent.id, assertion_props))


def identifier_handler(graphdriver, agent, sqlperson, graphperson, constants):
    pass


def death_handler(graphdriver, agent, factoid, graphperson, constants):
    with graphdriver.session() as session:
        # Each factoid is its own set of assertions pertaining to the single death of a person.
        # When there are multiple sources, we will have to review them for consistency and make
        # proxies for the death event as necessary.
        # Get the factoid source
        sourcenode = get_source_node(session, factoid)
        # See if we can find the death event
        deathclass = 'crm_E69_Death'
        deathpred = 'crm_P100_was_death_of'
        deathevent = _find_or_create_event(graphdriver, graphperson, deathclass, deathpred)
        # Create the new assertion that says the death happened. Start by gathering all our existing
        # nodes and reified predicates:
        # - the person
        # - the agent
        # - the source
        # - the event node
        # - the main event predicate
        # - the event description predicate
        # - the event dating predicate
        deathassertion = "MATCH (p), (agent), (source), (devent), (dpred), (descpred), (datepred) " \
                         "WHERE id(p) = %d AND id(agent) = %d AND id(source) = %d AND id(devent) = %d " \
                         "AND id(dpred) = %d AND id(descpred) = %d AND id(datepred) = %d " \
                         % (graphperson.id, agent.id, sourcenode.id, deathevent.id, constants[deathpred],
                            constants['crm_P3_has_note'], constants['crm_P4_has_time_span'])
        deathassertion += "CREATE (a:crm_E13_Attribute_Assignment)-[:crm_P140_assigned_attribute_to]->(de), "
        deathassertion += "(a)-[:crm_P177_assigned_property_type]->(dpred), "
        deathassertion += "(a)-[:crm_P141_assigned]->(p), "
        deathassertion += "(a)-[:crm_P14_carried_out_by]->(agent), "
        deathassertion += "(a)-[:crm_P70r_is_documented_in]->(source) "
        # Create an assertion about how the death is described
        deathassertion += "CREATE (a1:crm_E13_Attribute_Assignment)-[:crm_P140_assigned_attribute_to]->(de), "
        deathassertion += "(a1)-[:crm_P177_assigned_property_type]->(descpred), "
        deathassertion += "(a1)-[:crm_P141_assigned]->(desc:crm_E62_String {content:\"%s\"}), " % escape_text(
            factoid.replace_referents())
        deathassertion += "(a1)-[:crm_P14_carried_out_by]->(agent), "
        deathassertion += "(a1)-[:crm_P70r_is_documented_in]->(source) "
        # Create an assertion about when the death happened.
        returnstring = "RETURN a, a1"
        # TODO parse this later into a real date range
        deathdate = factoid.deathRecord.sourceDate
        if deathdate is not None and deathdate != '':
            deathassertion += "CREATE (a2:crm_E13_Attribute_Assignment)-[:crm_P140_assigned_attribute_to]->(de), "
            deathassertion += "(a2)-[:crm_P177_assigned_property_type]->(datepred), "
            deathassertion += "(a2)-[:crm_P141_assigned]->(datedesc:crm_E52_Time_Span {content:\"%s\"}), " % deathdate
            deathassertion += "(a2)-[:crm_P14_carried_out_by]->(agent), "
            deathassertion += "(a2)-[:crm_P70r_is_documented_in]->(source) "
            returnstring += ", a2"
        # Now actually run the query! Heaven help us.
        deathassertion += returnstring
        print(deathassertion)
        result = session.run(deathassertion).single()
    # This contains the node for all assertions created. Will we use the return value? Who knows?
    return result


def _find_or_create_event(graphdriver, person, crm_class, crm_predicate):
    with graphdriver.session() as session:
        query = "MATCH (pers) WHERE id(pers) = %d " % person.id
        query += "MATCH (a:crm_E13_Attribute_Assignment)-[:crm_P140_assigned]->(pers) "
        query += "MATCH (a)-[:crm_P141_assigned_attribute_to]->(event:%s)" % crm_class
        if crm_predicate is not None:
            query += "MATCH (a)-[:crm_P177_assigned_property_type]->(pred:%s) " % crm_predicate
        query += "RETURN event"
        result = session.run(query).single()
        if result is None:
            # If we don't have this event tied to this person yet, create the event and
            # make sure the predicate exists
            result = session.run("CREATE (event:%s) RETURN event" % crm_class).single()
    return result['event']


def get_source_node(session, factoid):
    return session.run("MERGE (src:crm_E31_Document {identifier:'%s', reference:'%s'}) RETURN src"
                       % (escape_text(factoid.source), escape_text(factoid.sourceRef))).single()['src']


def get_authority_node(session, authoritylist):
    if len(authoritylist) == 0:
        return None
    if len(authoritylist) == 1:
        return session.run("MERGE (p:crm_E21_Person {identifier:'%s'}) RETURN p" % authoritylist[0]).single()['p']
    if len(authoritylist) > 1:
        # Make/find a group including the people and return the group node
        nodes = {}
        for idx, p in enumerate(authoritylist):
            v = chr(idx+97)
            nodes[v] = "(%s:crm_E21_Person {identifier:'%s'})" % (v, p)
        # First we try to match a group containing all these people; if we match nothing,
        # then we create the group and return it.
        find_individuals = ' '.join(["MERGE %s" % x for x in nodes.values()])
        with_individuals = ','.join(nodes.keys())
        find_group = ', '.join(["(group)-[:crm_P107_has_current_or_former_member]->(%s)" % x for x in nodes.keys()])
        g = session.run("%s WITH %s MATCH (group:crm_E74_Group), %s RETURN group" % (
            find_individuals, with_individuals, find_group)).single()
        if g is None:
            g = session.run("%s WITH %s CREATE (group:crm_E74_Group), %s RETURN group" % (
                find_individuals, with_individuals, find_group)).single()
        return g['group']


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


def get_object_node(session, factoid_type, factoid, person):
    if factoid_type == "Narrative":
        pass
    elif factoid_type == "Authorship":
        # TODO model this as event
        # We are asserting that the person authored the source in question, so the source is the object.
        return get_source_node(session, factoid)
    elif factoid_type == "Death":
        # TODO model this as event
        return session.run("MERGE (d:E69_Death {text:'%s'}) RETURN d" % escape_text(factoid.engDesc)).single()['d']
    elif factoid_type == "Description":
        return session.run("MERGE (d:E62_String {class:'description', text:'%s', orig_text:'%s'}) RETURN d"
                           % (escape_text(factoid.engDesc), escape_text(factoid.origLDesc))).single()['d']
    elif factoid_type == "Dignity/Office":
        if factoid.dignityOffice is None:
            return None
        return session.run("MERGE (do:E62_String {class:'dignity_office', name:'%s', name_en:'%s'}) RETURN do"
                           % (escape_text(factoid.dignityOffice.stdNameOL),
                              escape_text(factoid.dignityOffice.stdName))).single()['do']
    elif factoid_type == "Education":
        return session.run("MERGE (e:E62_String {class:'education', description:'%s'}) RETURN e"
                           % escape_text(factoid.engDesc)).single()['e']
    elif factoid_type == "Ethnic label":
        return None if factoid.ethnicityInfo is None else \
            session.run("MERGE (e:E74_Group {class:'ethnicity', identifier:'%s'}) RETURN e"
                        % escape_text(factoid.ethnicityInfo.ethnicity.ethName)).single()['e']
    elif factoid_type == "Second Name":
        return None if factoid.secondName is None else \
            session.run("MERGE (i:E41_Appellation {text:'%s'}) RETURN i"
                        % escape_text(factoid.secondName.famName)).single()['i']
    elif factoid_type == "Kinship":
        if factoid.kinshipType is None:
            return None
        # The object is the other person in the factoid.
        kin = [x for x in factoid.persons if x != person]
        if len(kin) != 1:
            print("Skipping kinship factoid with more or less than 1 other person")
            return None
        return session.run("MERGE (p:E21_Person {name:'%s', code:%d}) RETURN p"
                           % (kin[0].name, kin[0].mdbCode)).single()['p']
    elif factoid_type == "Language Skill":
        return session.run("MERGE (l:E56_Language {identifier: '%s'}) RETURN l" % factoid.languageSkill).single()['l']
    elif factoid_type == "Location":
        if factoid.locationInfo is None:
            return None
        if factoid.locationInfo.location is None:
            print("Broken location link for factoid %d: %s" % (factoid.factoidKey, factoid.engDesc))
            return None
        return get_location_node(session, factoid.locationInfo.location)
    elif factoid_type == "Occupation/Vocation":
        return session.run("MERGE (o:E7_Activity {class:'occupation_vocation', identifier: '%s'}) "
                           "RETURN o" % factoid.occupation).single()['o']
    elif factoid_type == "Possession":
        pass  # Need to see if these records make any sense
    elif factoid_type == "Religion":
        return session.run("MERGE (r:E74_Group {class:'religion', identifier:'%s'}) "
                           "RETURN r" % factoid.religion).single()['r']
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
        nodelookup = "MERGE (p:crm_E21_Person {name:'%s', code:%d}) RETURN p" % (person.name, person.mdbCode)
        with graphdriver.session() as session:
            graph_person = session.run(nodelookup).single()['p']

        # Get the 'factoids' that are directly in the person record
        for ftype in pbwrecordinfo:
            ourftype = _smooth_labels(ftype)
            ourvocab = pbwvocabs.get(ourftype, dict())
            ourvocab.update(pbwvocabs.get('Predicates'))
            try:
                method = eval("%s_handler" % ourftype.lower())
                method(graphdriver, pbwagent, person, graph_person, ourvocab)
            except NameError:
                pass

        # Now get the factoids that are really factoids
        for ftype in pbwfactoids:
            ourftype = _smooth_labels(ftype)
            ourvocab = pbwvocabs.get(ourftype, dict())
            ourvocab.update(pbwvocabs.get('Predicates'))
            try:
                method = eval("%s_handler" % ourftype.lower())
                for f in person.main_factoids(ftype):
                    # Get the right agent for this source
                    authority_node = get_authority_node(session, auth_map.get(f.source, [])) or pbwagent
                    # Call the handler for this factoid type
                    method(graphdriver, authority_node, f, graph_person, ourvocab)

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
    # Get our hardcoded authority map
    auth_map = get_authmap()
    # Make / retrieve the global nodes and constants
    (pbwagent, pbwfactoids, pbwrecordinfo, pbwvocabs) = setup_constants(mysqlsession, driver)
    # Process the person records
    process_persons(collect_person_records(mysqlsession), driver, pbwagent, pbwfactoids, pbwrecordinfo, pbwvocabs)
    print("Done!")
