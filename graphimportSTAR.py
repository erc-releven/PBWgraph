import pbw
import config
import datetime
import re
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from neo4j import GraphDatabase
from warnings import warn

# Global variable for our constants object
constants = None


def escape_text(t):
    """Escape any single quotes or double quotes in strings that need to go into Neo4J properties"""
    return t.replace("'", "\\'").replace('"', '\\"')


class PBWstarConstants:
    """A class to deal with all of our constants, where the data is nicely encapsulated"""

    def __init__(self, sqlsession, graphdriver):
        self.graphdriver = graphdriver

        self.authorlist = {
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

        self.authoritylist = {
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

        self.entitylabels = {
            'C5': 'sdh-so__C5_Membership',
            'C11': 'sdh-so__C11_Gender',
            'C12': 'sdh-so__C12_Actors_Social_Role',
            'C21': 'sdh__C21_Skill',
            'C24': 'sdh-so__C24_Religion_or_religious_denomination',
            'E13': 'crm__E13_Assertion',
            'E17': 'crm_E17_Type_Assignment',
            'E22': 'crm__E22_Human-Made_Object',
            'E31': 'crm__E31_Document',
            'E34': 'crm__E34_Inscription',
            'E39': 'crm__E39_Actor',
            'E41': 'crm__E41_Appellation',
            'E52': 'crm__E52_Time-Span',
            'E55': 'crm__E55_Type',
            'E56': 'crm__E56_Language',
            'E62': 'crm__E62_String',
            'E74': 'crm__E74_Group',
            'F1': 'lrmer__F1_Work',
            'F2': 'lrmer__F2_Expression',
            'F22': 'lrmer__F22_Self-contained_Expression',
            'F28': 'lrmer__F28_Expression_Creation'
        }

        self.predicates = {
            'P1': 'crm__P1_is_identified_by',
            'P3': 'crm__P3_has_note',
            'P4': 'crm__P4_has_time_span',
            'P14': 'crm__P14_carried_out_by',
            'P41': 'crm__P41_classified',
            'P42': 'crm__P42_assigned',
            'P51': 'crm__P51_has_former_or_current_owner',
            'P70r': 'crm__P70i_is_documented_in',
            'P94': 'crm__P94_has_created',
            'P100': 'crm__P100_was_death_of',
            'P107': 'crm__P107_has_current_or_former_member',
            'P127': 'crm__P127_has_broader_term',
            'P128': 'crm__P128_carries',
            'P140': 'crm__P140_assigned_attribute_to',
            'P141': 'crm__P141_assigned',
            'P165': 'crm__P165_incorporates',
            'P177': 'crm__P177_assigned_property_type',
            'R3': 'lrmer__R3_is_realised_in',
            'R5': 'lrmer__R5_has_component',
            'R17': 'lrmer__R17_created',
            'SP13': 'sdh-so__P13_pertains_to',
            'SP38': 'sdh__P38_has_skill'
        }

        self.allowed = {
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

        # Define our STAR model predicates
        self.star_subject = 'crm__P140_assigned_attribute_to'
        self.star_predicate = 'crm__P177_assigned_property_type'
        self.star_object = 'crm__P141_assigned'
        self.star_source = 'crm__P17_was_motivated_by'
        self.star_auth = 'crm__P14_carried_out_by'

        # Get the list of factoid types
        self.factoidTypes = [x.typeName for x in sqlsession.query(pbw.FactoidType).all()
                             if x.typeName != '(Unspecified)']
        # Get the classes of info that are directly in the person record
        self.directPersonRecords = ['Gender', 'Disambiguation', 'Identifier']

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
                cypherq = "MERGE (super:%s {value:\"%s\", constant:TRUE}) " % (self.get_label('E55'), superclass)
                for inst in batch:
                    # Leave out blank instances
                    if inst == '':
                        continue
                    var = "inst%d" % i
                    i += 1
                    varmap[var] = inst
                    cypherq += "MERGE (%s:%s {value:\"%s\", constant:TRUE})-[:%s]->(super) " \
                               % (var, self.get_label('E55'), inst, self.get_label('P127'))
                cypherq += "RETURN %s" % ', '.join(["%s" % x for x in varmap.keys()])
                # print(cypherq)
                types = session.run(cypherq).single()
                for k, v in types.items():
                    retmap[varmap[k]] = v.id
            return retmap

        # Skill has type LanguageSkill has subtype [language]
        # Skill has type [occupation]?
        # Group has type Ethnicity has subtype [ethnicity]
        # Group has type Religious Group has subtype [religion]
        # SocialQuality has type [vocation]

        #  Initialise constants held in the SQL database
        print("Setting up PBW constants...")
        with graphdriver.session() as session:
            # Make our anonymous agent PBW for the un-sourced information
            self.generic_agent = session.run("MERGE (a:%s {identifier:'PBW editors', constant:TRUE}) return a"
                                             % self.entitylabels.get('E39')).single()['a']

            # Some of these factoid types have their own controlled vocabularies. Extract them here and
            # simplify the broader term.
            self.cv = dict()
            self.cv['Gender'] = _init_typology(session, self.get_label('C11'), ['Female', 'Male', 'Eunuch'])
            self.cv['SocietyRole'] = _init_typology(session, self.get_label('C12'),
                                                    [x.occupationName for x in sqlsession.query(pbw.Occupation).all()])
            self.cv['Ethnicity'] = _init_typology(session, self.get_label('E74'),
                                                  [x.ethName for x in sqlsession.query(pbw.Ethnicity).all()])
            self.cv['Religion'] = _init_typology(session, self.get_label('C24'),
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
            self.cv['Dignity'] = _init_typology(session, 'Dignity', list(all_dignities))

            # Kinship is expressed as typed predicates as opposed to E55 Types.
            kinnodes = {}
            for x in sqlsession.query(pbw.KinshipType).all():
                kt = x.gspecRelat
                cypherq = "MERGE (kt:%s {type:\"%s\"}) RETURN kt" % (self.get_predicate('P107'), kt)
                result = session.run(cypherq).single()
                kinnodes[kt] = result['kt'].id
            self.cv['Kinship'] = kinnodes

            # Language has its own subtype so handle this separately
            langnodes = {}
            for x in sqlsession.query(pbw.LanguageSkill).all():
                lang = x.languageName
                cypherq = "MERGE (lang:%s {value:'%s', constant:TRUE}) RETURN lang" % (self.get_label('E56'), lang)
                result = session.run(cypherq).single()
                langnodes[lang] = result['lang'].id
            self.cv['Language'] = langnodes

    ### Lookup functions

    def author(self, a):
        """Return the PBW person identifier for the given source author."""
        return self.authorlist.get(a, None)

    def authorities(self, a):
        """Return the name(s) of the scholar(s) responsible for ingesting the info from the given source
        into the database. Information on the latter is taken from https://pbw2016.kdl.kcl.ac.uk/ref/sources/
        and https://pbw2016.kdl.kcl.ac.uk/ref/seal-editions/"""
        return self.authoritylist.get(a, None)

    def get_label(self, lbl):
        """Return the fully-qualified entity or predicate label given the short name"""
        return self.entitylabels.get(lbl, self.predicates.get(lbl, None))

    def get_predicate(self, p):
        """Return the reified predicate node for the given short name"""
        if isinstance(self.predicates[p], str):
            fqname = self.predicates[p]
            with self.graphdriver.session() as session:
                npred = session.run("MERGE (n:%s {constant:TRUE}) RETURN n" % fqname).single()['n']
                self.predicates[p] = npred
        return self.predicates[p]

    def inrange(self, floruit):
        """Return true if the given floruit tag falls within RELEVEN's range"""
        return floruit in self.allowed


def collect_person_records(sqlsession):
    """Get a list of people whose floruit matches our needs"""
    relevant = [x for x in sqlsession.query(pbw.Person).all() if constants.inrange(x.floruit) and len(x.factoids) > 0]
    print("Found %d relevant people" % len(relevant))
    return relevant
    # Debugging / testing: restrict the list of relevant people
    # debugnames = ['Herve', 'Ioannes', 'Konstantinos', 'Anna']
    # debugcodes = [62, 68, 101, 102]
    # return [x for x in relevant if x.name in debugnames and x.mdbCode in debugcodes]


def _smooth_labels(label):
    if label == 'Dignity/Office':
        return 'Dignity'
    if label == 'Occupation/Vocation':
        return 'SocietyRole'
    if label == 'Language Skill':
        return 'LanguageSkill'
    if label == 'Ethnic label':
        return 'Ethnicity'
    if label == 'Second Name' or label == 'Alternative Name':
        return 'Appellation'
    if label == 'Uncertain Ident':
        return 'UncertainIdent'
    return label


def _create_assertion_query(factoid, subj, pred, obj, auth, src, var="a"):
    """Create the query pattern for an assertion with the given connections. Use 'var' to control
    the variable name for the assertion. Attempts to build the query with specific information first,
    assuming that plain node variable names indicate an already known node."""
    apreds = {'subj': '[:%s]' % constants.star_subject,
              'pred': '[:%s]' % constants.star_predicate,
              'obj': '[:%s]' % constants.star_object,
              'auth': '[:%s]' % constants.star_auth,
              'src': '[:%s]' % constants.star_source}
    # Do the subject and object first, then source, authority and predicate
    # as search area probably increases for each in that order
    anodes = [('auth', auth), ('pred', pred)]
    if src is not None:
        anodes.insert(0, ('src', src))
    if re.match(r'^\w+$', obj):
        anodes.insert(0, ('obj', obj))
    else:
        anodes.append(('obj', obj))
    if re.match(r'^\w+$', subj):
        anodes.insert(0, ('subj', subj))
    else:
        anodes.append(('subj', subj))

    # Now build the query using the order in anodes
    aclass = ':crm__E13_Attribute_Assignment:lrmer__F22_Self-contained_Expression'
    afromfact = 'https://pbw2016.kdl.kcl.ac.uk/rdf/factoid/'
    if factoid is not None:
        afromfact += factoid.factoidKey
    else:
        afromfact += 'NONE'
    aclassed = False
    aqparts = []
    for nt in anodes:
        aqparts.append("(%s%s%s)-%s->(%s)" % (var,
                                              aclass if not aclassed else '',
                                              ' {"lrmer__R76_is_derivative_of": "%s"}' % afromfact if not aclassed else '',
                                              apreds[nt[0]],
                                              nt[1]))
        aclassed = True
    return "COMMAND %s " % ", ".join(aqparts)


def gender_handler(agent, sqlperson, graphperson):
    with constants.graphdriver.session() as session:
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
            # print("...setting gender assignment to %s%s" % (pbw_sex, " (maybe)" if uncertain else ""))
            # Make the event tied to this person
            genderassertion = "MATCH (p), (s), (pbw), (sp41) " \
                              "WHERE id(p) = %d AND id(s) = %d AND id(pbw) = %d AND id(sp41) = %d " % \
                              (graphperson.id, constants.cv['Gender'][pbw_sex],
                               agent.id, constants.get_predicate('P41'))
            genderassertion += "MERGE (sp42:%s%s) " % (constants.get_predicate('P42'), assertion_props)
            genderassertion += "WITH p, s, pbw, sp41, sp42 "
            genderassertion += _create_assertion_query(None, 'ga:%s' % constants.get_label('E17'),
                                                       'sp41', 'p', 'pbw', None, 'a1')
            genderassertion += _create_assertion_query(None, 'ga', 'sp42', 's', 'pbw', None, 'a2')
            genderassertion += "RETURN a1, a2"
            # print(genderassertion % (graphperson.id, constants[pbw_sex], agent.id, assertion_props))
            result = session.run(genderassertion.replace('COMMAND', 'MATCH')).single()
            if result is None:
                session.run(genderassertion.replace('COMMAND', 'CREATE'))


def identifier_handler(agent, sqlperson, graphperson):
    """The identifier in this context is the 'origName' field, thus an identifier assigned by PBW
    not on the basis of any particular source"""
    with constants.graphdriver.session() as session:
        idassertion = "MATCH (p), (pbw), (pred) WHERE id(p) = %d AND id(pbw) = %d AND id(pred) = %d " \
                      % (graphperson.id, agent.id, constants.get_predicate('P1'))
        idassertion += "MERGE (app:%s {value: \"%s\"}) " % (constants.get_label('E41'), sqlperson.nameOL)
        idassertion += "WITH p, pbw, pred, app "
        idassertion += _create_assertion_query(None, 'p', 'pred', 'app', 'pbw', None)
        idassertion += "RETURN a"
        result = session.run(idassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            return session.run(idassertion.replace('COMMAND', 'CREATE'))


def disambiguation_handler(agent, sqlperson, graphperson):
    """The short description of the person provided by PBW"""
    with constants.graphdriver.session() as session:
        disassertion = "MATCH (p), (pbw), (pred) WHERE id(p) = %d AND id(pbw) = %d AND id(pred) = %d " % \
                       (graphperson.id, agent.id, constants.get_predicate('P3'))
        disassertion += "MERGE (desc:crm__E62_String {value:\"%s\"}) " % escape_text(sqlperson.descName)
        disassertion += "WITH p, pred, desc, pbw "
        disassertion += _create_assertion_query(None, 'p', 'pred', 'desc', 'pbw', None)
        disassertion += "RETURN a"
        result = session.run(disassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            return session.run(disassertion.replace('COMMAND', 'CREATE'))


def _find_or_create_event(person, eventclass, predicate):
    """Helper function to find the relevant event for event-based factoids"""
    with constants.graphdriver.session() as session:
        query = "MATCH (pers), (pred) WHERE id(pers) = %d AND id(pred) = %d " % (person.id, predicate)
        query += "MATCH (a:%s)-[:%s]->(event:%s), " % (constants.get_label('P13'), constants.star_subject, eventclass)
        query += "(a)-[:%s]->(pred), " % constants.star_predicate
        query += "(a)-[:%s]->(pers) " % constants.star_object
        query += "RETURN DISTINCT event"  # There may well be multiple assertions about this event
        result = session.run(query).single()
        if result is None:
            # If we don't have this event tied to this person yet, create a new event of the
            # given class and return it for use in the assertion being made about it.
            result = session.run("CREATE (event:%s) RETURN event" % eventclass).single()
    return result['event']


def get_source_and_agent(session, factoid):
    """Returns a node that represents the source for this factoid. Creates the network of nodes and
    relationships to describe that source, if necessary. The source will either be a physical E22 Human-Made Object
    (the boulloterion) or an E31 Document (the written primary source)."""
    # Is this a 'seals' source without a boulloterion? If so warn and return None
    author = None
    if constants.authorities(factoid.source) is None:
        if factoid.source != 'Seals' or factoid.boulloterion is None:
            warn("No boulloterion found for seal-sourced factoid %d" % factoid.factoidKey
                 if factoid.source == 'Seals'
                 else "Source %s of factoid %d not known" % (factoid.source, factoid.factoidKey))
            return None, None
    if factoid.boulloterion is not None:
        # This factoid is taken from a seal inscription. Let's pull that out into CRM objects.
        # If the boulloterion has no associated publications, we shouldn't use it.
        if len(factoid.boulloterion.publication) == 0:
            warn("No published source found for boulloterion %d" % factoid.boulloterion.boulloterionKey)
            return None, None
        # First find who did the analysis
        alist = set()
        for pub in factoid.boulloterion.publication:
            # If the publication isn't in the authority list, Michael analysed it
            if pub.bibSource is not None:
                thispubauth = constants.authoritylist.get(pub.bibSource.shortName, ["Michael Jeffreys"])
                alist.update(thispubauth)
        agent = get_authority_node(session, list(alist))
        # Then get the node that points to the boulloterion's sources
        srclist = get_boulloterion_sourcelist(session, factoid.boulloterion)
        if srclist is not None:
            q = "MATCH (pred), (agent), (srclist) WHERE id(pred) = %d AND id(agent) = %d AND id(srclist) = %d " \
                % (constants.get_predicate('P128'), agent.id, srclist.id)
        else:
            q = "MATCH (pred), (agent) WHERE id(pred) = %d AND id(agent) = %d " \
                % (constants.get_predicate('P128'), agent.id)
        # boulloterion is an E22 Human-Made Object
        q += "MERGE (boul:%s {reference:%s}) " % (constants.get_label('E22'), factoid.boulloterion.boulloterionKey)
        # which is asserted by the agent to P128 carry an E34 Inscription (we can even record the inscription)
        q += "MERGE (src:%s:%s {text:\"%s\"}) " \
             % (constants.get_label('E34'), constants.get_label('E31'), factoid.boulloterion.origLText)
        q += "WITH boul, pred, src, agent%s " % (", srclist" if srclist else "")
        q += _create_assertion_query(factoid, "boul", "pred", "src", "agent", "srclist" if srclist else None)
        # MAYBE LATER: which is asserted by MJ to P108 have produced various E22 Human-Made Objects (the seals)
        # - which seals are asserted by the collection authors (with pub source) to also carry the inscriptions?
    else:
        # This factoid is taken from a document.
        # Do we have a known author for this text?
        author = get_author_node(session, constants.author(factoid.source))
        # If not, we use the PBW scholar as the authority.
        agent = get_authority_node(session, constants.authorities(factoid.source))
        # If there is no PBW scholar known for this source, we use the generic PBW agent.
        if agent is None:
            agent = constants.generic_agent
        # Now we find an E31 Document (the whole work), its author (if any), and the PBW scholar who analyzed it
        work = get_source_work(session, factoid, author)
        q = "MATCH (work), (agent), (p165) WHERE id(work) = %d AND id(agent) = %d AND id(p165) = %d " % (
            work.id, agent.id, constants.get_predicate('P165'))
        q += "MERGE (src:%s {reference:'%s', text:'%s'}) " % \
             (constants.get_label('E31'), escape_text(factoid.sourceRef), escape_text(factoid.origLDesc))
        q += "WITH work, p165, src, agent "
        q += _create_assertion_query(factoid, "work", "p165", "src", "agent", None)
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
    # Get some labels
    e31 = constants.get_label('E31')
    e73 = constants.get_label('E73')
    p165 = constants.get_label('P165')
    i = 0
    q = ""
    for pub in boulloterion.publication:
        # Make sure with 'merge' that each publication node exists
        q += "MERGE (src%d:%s {identifier:'%s', reference:'%s'}) " % \
            (i, e31, pub.bibSource.shortName, pub.publicationRef)
        i += 1
    if i > 1:
        # Check to see whether we have a matching group with only these publication nodes.
        # Teeeeechnically speaking, an Information Object cannot P70 document anything, so
        # we also assign E31 Document as a stopgap since the group of sources, taken together,
        # documents something.
        parts = []
        retvar = "srcgrp"
        q += "WITH %s " % ", ".join(["src%d" % x for x in range(i)])
        q += "MATCH (srcgrp:%s:%s) WHERE size((srcgrp)-[:%s]->(:%s)) = %d " \
             % (e73, e31, p165, e31, i)
        for n in range(i):
            parts.append("(srcgrp)-[:%s]->(src%d)" % (constants.get_label('P165'), n))
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
        createparts = ["(srcgrp:%s:%s)" % (e73, e31)]
        for pub in boulloterion.publication:
            matchparts.append("(src%d:%s {identifier:'%s', reference:'%s'}) " % (
                i, e31, pub.bibSource.shortName, pub.publicationRef))
            createparts.append("(srcgrp)-[:%s]->(src%d)" % (p165, i))
            i += 1
        q = "MATCH " + ", ".join(matchparts) + " "
        q += "CREATE " + ", ".join(createparts) + " "
        q += "RETURN srcgrp"
        ret = session.run(q).single()
    return ret[retvar]


def get_source_work(session, factoid, author):
    # Ensure the existence of the work and, if it has a declared author, link the author to it via
    # a CREATION event, asserted by TLA
    q = "MERGE (source:%s:%s {identifier:'%s'}) " % (constants.get_label('E31'), constants.get_label('F22'), escape_text(factoid.source))
    if author is not None:
        # Ensure the existence of the assertions that the author authored the work
        tla = get_authority_node(session, ['Tara Andrews'])
        q += "WITH source "
        q += "MATCH (author), (tla), (p14), (p94) " \
            "WHERE id(author) = %d AND id(tla) = %d AND id(p14) = %d AND id(p94) = %d " % (
                author.id, tla.id, constants.get_predicate('P14'), constants.get_predicate('P94'))
        q += _create_assertion_query(factoid, 'aship:crm__E65_Creation', 'p14', 'work', 'tla', None, 'a1')
        q += _create_assertion_query(factoid, 'aship', 'p94', 'author', 'tla', None, 'a2')
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
        return _find_or_create_graphperson(authorlist[0], authorlist[1])
    # It is our multi-authored text. Make a group because both authors share authority.
    authors = []
    while len(authorlist) > 0:
        pname = authorlist.pop(0)
        pcode = authorlist.pop(1)
        authors.append(_find_or_create_graphperson(pname, pcode))
    return _find_or_create_authority_group(session, authors)


def get_authority_node(session, authoritylist):
    if authoritylist is None or len(authoritylist) == 0:
        return None
    if len(authoritylist) == 1:
        return session.run("MERGE (p:%s {identifier:'%s'}) RETURN p"
                           % (constants.get_label('E21'), authoritylist[0])).single()['p']
    # If we get here, we have more than one authority for this source.
    # Ensure the existence of the people, and then ensure the existence of their group
    scholars = []
    for p in authoritylist:
        scholars.append(session.run("MERGE (p:%s {identifier:'%s'}) RETURN p" % (constants.get_label('E21'), p)).single()['p'])
    return _find_or_create_authority_group(session, scholars)


def _find_or_create_authority_group(session, members):
    mc = []
    wc = []
    gc = []
    i = 1
    for a in members:
        mc.append("(a%d)" % i)
        wc.append("id(a%d) = %d" % (i, a.id))
        gc.append("(group)-[:%s]->(a%d)" % (constants.get_label('P107'), i))
        i += 1
    q = "MATCH %s WHERE %s " % (', '.join(mc), " AND ".join(wc))
    q += "(group:%s), %s RETURN group" % (constants.get_label('E74'), ", ".join(gc))
    g = session.run("MATCH " + q).single()
    if g is None:
        g = session.run("CREATE " + q).single()
    return g['group']


def _get_source_lang(factoid):
    lkeys = {2: 'gr', 3: 'la', 4: 'ar', 5: 'hy'}
    try:
        return lkeys.get(factoid.oLangKey)
    except NameError:
        return None


def appellation_handler(graphdriver, sourcenode, agent, factoid, graphperson, constants):
    """This handler deals with Second Name factoids and also Alternative Name factoids.
    The Second Names might be in all sorts of languages in the factoid itself, but refer
    to a canonical version of the name in the FamilyName table, which is probably usually
    Greek. The Alternative Name factoids should exclusively use the information in the
    base factoid."""
    appassertion = "MATCH (p), (agent), (source), (pred) " \
                   "WHERE id(p) = %d AND id(agent) = %d AND id(source) = %d AND id(pred) = %d " % (
                       graphperson.id, agent.id, sourcenode.id, constants.get_predicate('P1'))
    name_en = None
    if factoid.factoidType == 'Alternative Name':
        # We need to do some data cleaning here, since the engDesc is not particularly clean.
        captures = [
            r'^([\w\s]+):.*$',
            r'^([\w\s]+) \(Different Name.*\).*$',
            r'^([\w\s]+) \(monastic name\).*$',
            r'^.*name changed to (\w+).*$',
            r'^.*also known as (\w+).*$',
            r'^.*was called (\w+).*$',
            r'^.*was renamed (\w+).*$',
            r'^Baptised (\w+).*$'
            r'^.*name.*? was (\w+).*$',
            r'^.*changed.*? name to (\w+).*$',
        ]
        for exp in captures:
            appel = re.match(exp, factoid.engDesc)
            if appel is not None:
                name_en = appel.group(1)
                break
        if name_en is None:
            name_en = factoid.engDesc
        if name_en == '':
            # The name is in the origLDesc
            name_en = factoid.origLDesc
        if len(' '.split(name_en)) > 3:
            warn("Could not resolve alternative name from description '%s'" % factoid.engDesc)
            return None
        name_ol = factoid.origLDesc
        olang = _get_source_lang(factoid)
        print("Adding alternative name %s (%s '%s')" % (name_en, olang, name_ol))
    else:  # factoidType is 'Second Name'
        # We need to fish out the canonical family name, which is in secondName.famName
        if factoid.secondName is not None:
            name_en = factoid.secondName.famName
            name_ol = factoid.secondName.famNameOL
            olang = _get_source_lang(factoid.secondName) or 'gr'
        else:
            name_en = factoid.engDesc
            name_ol = factoid.origLDesc
            olang = _get_source_lang(factoid) or 'gr'
        print("Adding second name %s (%s '%s')" % (name_en, olang, name_ol))

    appassertion += "MERGE (n:%s {en:'%s', %s:'%s'}) " % (
        constants.get_label('E41'), escape_text(name_en), olang, escape_text(name_ol))
    appassertion += "WITH p, agent, source, pred, n "
    appassertion += _create_assertion_query(factoid, 'p', 'pred', 'n', 'agent', 'source')
    appassertion += "RETURN a"
    with graphdriver.session() as session:
        result = session.run(appassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(appassertion.replace('COMMAND', 'CREATE'))


def death_handler(graphdriver, sourcenode, agent, factoid, graphperson, constants):
    with graphdriver.session() as session:
        # Each factoid is its own set of assertions pertaining to the single death of a person.
        # When there are multiple sources, we will have to review them for consistency and make
        # proxies for the death event as necessary.
        # See if we can find the death event
        deathevent = _find_or_create_event(graphperson, constants.get_label('E69'), constants.get_predicate('P100'))
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
            warn("Someone has a death factoid (%d, \"%s\") without a death record! Go check it out." % (
                factoid.factoidKey, factoid.engDesc))
        else:
            # TODO parse this later into a real date range
            deathdate = factoid.deathRecord.sourceDate
            if deathdate == '':
                deathdate = None
        deathassertion = "MATCH (p), (agent), (source), (devent), (p100), (p3), (p4) " \
                         "WHERE id(p) = %d AND id(agent) = %d AND id(source) = %d AND id(devent) = %d " \
                         "AND id(p100) = %d AND id(p3) = %d AND id(p4) = %d " \
                         % (graphperson.id, agent.id, sourcenode.id, deathevent.id, constants.get_predicate('P100'),
                            constants.get_predicate('P3'), constants.get_predicate('P4'))
        deathassertion += "MERGE (desc:%s {content:\"%s\"}) " % (constants.get_label('E62'), escape_text(factoid.replace_referents()))
        if deathdate is not None:
            deathassertion += "MERGE (datedesc:%s {content:\"%s\"}) " % (constants.get_label('E52'), deathdate)
        deathassertion += "WITH p, agent, source, devent, p100, p3, p4, desc%s " % (', datedesc' if deathdate else '')
        deathassertion += _create_assertion_query(factoid, 'devent', 'p100', 'p', 'agent', 'source')
        # Create an assertion about how the death is described
        deathassertion += _create_assertion_query(factoid, 'devent', 'p3', 'desc', 'agent', 'source', 'a1')
        # Create an assertion about when the death happened.
        if deathdate is not None:
            deathassertion += _create_assertion_query(factoid, 'devent', 'p4', 'datedesc', 'agent', 'source', 'a2')
        deathassertion += "RETURN a, a1%s" % (", a2" if deathdate else '')

        # print(deathassertion)
        result = session.run(deathassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(deathassertion.replace('COMMAND', 'CREATE'))


def _find_or_create_group(idlabel, category):
    grouplabels = {'Ethnicity': 'Ethnic_Group',
                   'Religion': 'Religious_Community',
                   'SocietyRole': 'Society_Role',
                   'Language': 'Language_Skilled'}
    if category == 'Language':
        labelquery = 'MATCH (l:%s {value:"%s"}) ' % (constants.get_label('E56'), idlabel)
    else:
        labelquery = 'MATCH (l:%s {value:"%s"})-[:%s]->(super:%s {value:"%s"}) ' \
                     % (constants.get_label('E55'), idlabel, constants.get_label('P127'), constants.get_label('E55'), category)
    with constants.graphdriver.session() as session:
        groupassertion = labelquery + "MERGE (g:crm__E74_Group)-[:%s]->(l) RETURN g" \
                         % (constants.get_label('P1'), grouplabels[category])
        result = session.run(groupassertion).single()
    return result['g']


def _assign_group_membership(sourcenode, agent, graphperson, grouptype, grouplabel):
    with constants.graphdriver.session() as session:
        # Get the ethnic group in question
        groupnode = _find_or_create_group(grouplabel, grouptype)
        # Need the person, the group
        gassertion = "MATCH (p), (agent), (source), (group), (mpred)  " \
                     "WHERE id(p) = %d AND id(agent) = %d AND id(source) = %d AND id(group) = %d " \
                     "AND id(mpred) = %d " % (graphperson.id, agent.id, sourcenode.id, groupnode.id, constants['P107'])
        gassertion += _create_assertion_query(None, 'group', 'mpred', 'p', 'agent', 'source')
        gassertion += "RETURN a"
        result = session.run(gassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(gassertion.replace('COMMAND', 'CREATE'))


def ethnicity_handler(sourcenode, agent, factoid, graphperson):
    """Assign a group membership for the given ethnicity to the person"""
    if factoid.ethnicityInfo is None or factoid.ethnicityInfo.ethnicity is None:
        # We can't assign any ethnicity without the ethnicity info
        warn("Empty ethnicity factoid found: id %s" % factoid.factoidKey)
        return
    elabel = factoid.ethnicityInfo.ethnicity.ethName
    _assign_group_membership(sourcenode, agent, graphperson, 'Ethnicity', elabel)


def religion_handler(sourcenode, agent, factoid, graphperson):
    """Assign a group membership for the given religious confession to the person"""
    if factoid.religion is None:
        warn("Empty religion factoid found: id %d" % factoid.factoidKey)
        return
    rlabel = factoid.religion
    # Special case
    if factoid.religion == '':
        rlabel = 'Heretic'
    _assign_group_membership(sourcenode, agent, graphperson, 'Religion', rlabel)


def societyrole_handler(sourcenode, agent, factoid, graphperson):
    if factoid.occupation is None:
        return
    _assign_group_membership(sourcenode, agent, graphperson, 'SocietyRole', factoid.occupation)


def language_handler(sourcenode, agent, factoid, graphperson):
    """Assign a language skill to the person"""
    if factoid.languageSkill is None:
        return
    _assign_group_membership(sourcenode, agent, graphperson, 'Language', factoid.languageSkill)


def description_handler(graphdriver, sourcenode, agent, factoid, graphperson, constants):
    # Get the descriptions and the relevant languages
    langdesc = {'en': escape_text(factoid.replace_referents())}
    langkey = _get_source_lang(factoid)
    if langkey is not None:
        langdesc[langkey] = escape_text(factoid.origLDesc)
    descattributes = []
    for k, v in langdesc.items():
        descattributes.append('%s: \"%s\"' % (k, v))
    # Make the query
    with graphdriver.session() as session:
        descassertion = "MATCH (p), (agent), (source), (pred) " \
                        "WHERE id(p) = %d AND id(agent) = %d AND id (source) = %d AND id(pred) = %d " % \
                        (graphperson.id, agent.id, sourcenode.id, constants['P3'])
        descassertion += 'MERGE (desc:crm__E62_String {%s}) ' % ','.join(descattributes)  # TODO get rid of E62
        descassertion += "WITH p, pred, desc, agent, source "
        descassertion += _create_assertion_query(factoid, 'p', 'pred', 'desc', 'agent', 'source')
        descassertion += 'RETURN a'
        result = session.run(descassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(descassertion.replace('COMMAND', 'CREATE'))


def _find_or_create_kinship(session, graphperson, graphkin):
    # See if there is an existing kinship group between the person and their kin according to the
    # source in question. If not, return a new (not yet connected) kinship group node.
    e74 = constants.get_label('E74')
    p140 = constants.get_label('P140')
    p141 = constants.get_label('P141')
    kinquery = "MATCH (p), (kin) WHERE id(p) = %d AND id(kin) = %d " % (graphperson.id, graphkin.id)
    kinquery += "MATCH (p)<-[:%s]-(a1:crm__E13_Assertion)-[:%s]->(kg:%s)<-[:%s]-(a2)-[:%s]->(kin) " \
                "RETURN DISTINCT kg" % (p141, p140, e74, p140, p141)
    result = session.run(kinquery).single()
    if result is None:
        # If the kinship pair hasn't been referenced yet, then create a new empty kinship and return it
        # for use in the assertions below.
        result = session.run("CREATE (kg:%s) RETURN kg" % e74).single()
    return result['kg']


def kinship_handler(sourcenode, agent, factoid, graphperson):
    # Kinships are modeled as two-person groups with their own separate label (because, honestly)
    # and with .1 types as property attributes as per the CRM spec.
    if factoid.kinshipType is None:
        warn("Empty kinship factoid found: id %d" % factoid.factoidKey)
        return
    predspec = constants.cv.get('Kinship')[factoid.kinshipType.gspecRelat]
    predgen = constants.get_predicate('P107')
    with constants.graphdriver.session() as session:
        for kin in factoid.referents():
            graphkin = _find_or_create_graphperson(kin.name, kin.mdbCode)
            if graphkin.id == graphperson.id:
                warn("Person %s listed as related to self" % kin)
                continue
            kgroup = _find_or_create_kinship(session, graphperson, graphkin)
            kinassertion = "MATCH (p), (kin), (kg), (agent), (source), (pspec), (pgen) WHERE id(p) = %d " \
                           "AND id(kin) = %d AND id(kg) = %d AND id(agent) = %d AND id(source) = %d " \
                           "AND id(pspec) = %d AND id(pgen) = %d " \
                           % (graphperson.id, graphkin.id, kgroup.id, agent.id, sourcenode.id, predspec, predgen)
            kinassertion += _create_assertion_query(factoid, 'kg', 'pspec', 'p', 'agent', 'source', 'a1')
            kinassertion += _create_assertion_query(factoid, 'kg', 'pgen', 'kin', 'agent', 'source', 'a2')
            kinassertion += "RETURN a1, a2"
            result = session.run(kinassertion.replace('COMMAND', 'MATCH')).single()
            if result is None:
                session.run(kinassertion.replace('COMMAND', 'CREATE'))


def possession_handler(sourcenode, agent, factoid, graphperson):
    """Ensure the existence of an E18 Physical Thing (we don't have any more category info about
    the possessions. For now we ssume that a possession with an identical description is, in fact,
    the same possession."""
    possession_attributes = {constants.get_label('P1'): escape_text(factoid.engDesc)}
    if factoid.possession is not None and factoid.possession != '':
        possession_attributes[constants.get_label('P3')] = escape_text(factoid.possession)
    possession_attrs = ", ".join(["%s: '%s'" % (k, v) for k, v in possession_attributes.items()])
    posassertion = "MATCH (p), (agent), (source), (pred) " \
                    "WHERE id(p) = %d AND id(agent) = %d AND id (source) = %d AND id(pred) = %d " % \
                    (graphperson.id, agent.id, sourcenode.id, constants.get_predicate('P51'))
    posassertion += "MERGE (poss:%s {%s}) " % (constants.get_label('E18'), possession_attrs)
    posassertion += "WITH p, agent, source, pred, poss "
    posassertion += _create_assertion_query(factoid, 'poss', 'pred', 'p', 'agent', 'source')
    posassertion += "RETURN a"
    with constants.graphdriver.session() as session:
        result = session.run(posassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(posassertion.replace('COMMAND', 'CREATE'))


def _find_or_create_graphperson(name, code):
    """Return an E21 Person, labeled with the name and code via an E14 Identifier Assignment carried out by PBW."""
    # We can't merge with comma statements, so we have to do it with successive one-liners.
    # Start the merge from the specific information we have, which is the identifier itself.
    nodelookup = "MATCH (pbw) WHERE id(pbw) = %d " \
                 "MERGE (idlabel:%s {value:'%s %d'}) " \
                 "MERGE (pbw)<-[:%s]-(idass:%s)" \
                 "-[:%s]->(idlabel) " \
                 "MERGE (idass)-[:%s]->(p:%s) RETURN p" % \
                 (constants.generic_agent.id, constants.get_label('E42'), name, code, constants.get_label('P14'),
                  constants.get_label('E15'), constants.get_label('P37'),
                  constants.get_label('P140'), constants.get_label('E21'))
    with constants.graphdriver.session() as session:
        graph_person = session.run(nodelookup).single()['p']
    return graph_person


# def get_location_node(session, pbwloc):
#     # The location record has an identifer, plus a couple of assertions by Charlotte about its
#     # correspondence in the GeoNames and/or Pleiades database.
#     loc_query = session.run("MATCH (l:crm__E53_Place {identifier: '%s', pbwid: %d}) RETURN l"
#                             % (escape_text(pbwloc.locName), pbwloc.locationKey)).single()
#     if loc_query is None:
#         # We need to create it.
#         loc_node = session.run("CREATE (l:crm__E53_Place {identifier: '%s', pbwid: %d}) RETURN l"
#                                % (escape_text(pbwloc.locName), pbwloc.locationKey)).single()['l']
#         for db in ("pleiades", "geonames"):
#             dbid = pbwloc.__getattribute__("%s_id" % db)
#             if dbid is not None:
#                 ag = session.run("MERGE (ag:crm__E39_Actor {identifier:'Charlotte Roueché'}) RETURN ag").single()['ag']
#                 dbloc = session.run("MERGE (l:crm__E94_Space_Primitive {db: '%s', id: %d}) "
#                                     "RETURN l" % (db, dbid)).single()['l']
#                 pred = session.run(
#                     "MERGE (p:crm__E55_Type {property:'crm__P168_place_is_defined_by'}) RETURN p").single()['p']
#                 session.run(
#                     "MATCH (l), (ag), (dbl), (p) WHERE id(l) = %d AND id(ag) = %d AND id(dbl) = %d AND id(p) = %d "
#                     "MERGE (a:crm__E13_Assertion)-[:crm__P140_assigned_attribute_to]->(l) "
#                     "MERGE (a)-[:crm__P177_assigned_property_type]->(p) MERGE (a)-[:crm__P141_assigned]->(dbl) "
#                     "MERGE (a)-[:crm__P14_carried_out_by]->(ag)" % (loc_node.id, ag.id, dbloc.id, pred.id))
#     else:
#         loc_node = loc_query['l']
#     return loc_node


def process_persons(sqlsession, graphdriver):
    """Go through the relevant person records and process them for factoids"""
    processed = 0
    for person in collect_person_records(sqlsession):
        # Skip the anonymous groups for now
        if person.name == 'Anonymi':
            continue
        processed += 1
        # Create or find the person node
        print("*** Making/finding node for person %s %d ***" % (person.name, person.mdbCode))
        graph_person = _find_or_create_graphperson(person.name, person.mdbCode)

        # Get the 'factoids' that are directly in the person record
        for ftype in constants.directPersonRecords:
            ourftype = _smooth_labels(ftype)
            try:
                method = eval("%s_handler" % ourftype.lower())
                method(constants.generic_agent, person, graph_person)
            except NameError:
                warn("No handler for %s record info; skipping." % ourftype)

        # Now get the factoids that are really factoids
        for ftype in constants.factoidTypes:
            ourftype = _smooth_labels(ftype)
            ourvocab = constants.cv.get(ourftype, dict())
            try:
                method = eval("%s_handler" % ourftype.lower())
                fprocessed = 0
                for f in person.main_factoids(ftype):
                    # Get the source, either a text passage or a seal inscription, and the authority
                    # for the factoid. Authority will either be the author of the text, or the PBW
                    # colleague who read the text and ingested the information.
                    with graphdriver.session() as session:
                        (source_node, authority_node) = get_source_and_agent(session, f)
                    # If the factoid has no source then we skip it
                    if source_node is None:
                        warn("Skipping factoid %d without a traceable source" % f.factoidKey)
                        continue
                    # If the factoid has no authority then we assign it to the generic PBW agent
                    if authority_node is None:
                        authority_node = constants.generic_agent
                    # Call the handler for this factoid type
                    method(source_node, authority_node, f, graph_person)
                    fprocessed += 1
                if fprocessed > 0:
                    print("Ingested %d %s factoid(s)" % (fprocessed, ftype))

            except NameError:
                pass

    print("Processed %d person records." % processed)


# If we are running as main, execute the script
if __name__ == '__main__':
    # Connect to the SQL DB
    starttime = datetime.datetime.now()
    engine = create_engine('mysql+pymysql://' + config.dbstring)
    smaker = sessionmaker(bind=engine)
    mysqlsession = smaker()
    # Connect to the graph DB
    driver = GraphDatabase.driver(config.graphuri, auth=(config.graphuser, config.graphpw))
    # Make / retrieve the global nodes and constants
    constants = PBWstarConstants(mysqlsession, driver)
    # Process the person records
    process_persons(mysqlsession, driver)
    duration = datetime.datetime.now() - starttime
    print("Done! Ran in %s" % str(duration))
