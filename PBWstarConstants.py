import pbw
# This package contains a bunch of information curated from the PBW website about authority, authorship
# and so forth. It is a huge laundry list of data and some initialiser and accessor functions for it; the
# class requires a graph driver in order to do the initialisation.


class PBWstarConstants:
    """A class to deal with all of our constants, where the data is nicely encapsulated"""

    def __init__(self, sqlsession, graphdriver):
        self.graphdriver = graphdriver

        self.authorlist = {
            'Albert of Aachen': {'author': ['Albert', 26101], 'factoid': 432193,
                                 'work': 'Historia Ierosolimitana',
                                 'expression': 'Susan B. Edgington, Historia Ierosolimitana, '
                                               'History of the Journey to Jerusalem, Oxford 2007'},
            'Alexios Stoudites': {'author': ['Alexios', 11],
                                  'expression': 'G. Ficker, Erlasse des Patriarchen von Konstantinopel Alexios '
                                                'Studites, Kiel 1911'},
            'Anna Komnene': {'author': ['Anna', 62], 'factoid': 437887, 'work': 'Alexias',
                             'expression': 'D.R. Reinsch and A. Kambylis, Annae Comnenae Alexias, '
                                           'Corpus fontium historiae Byzantinae 40/1, Berlin – New York 2001'},
            'Aristakes': {'author': ['Aristakes', 101], 'factoid': 375021},
            'Attaleiates: Diataxis': {'author': ['Michael', 202], 'factoid': 254236},
            'Attaleiates: History': {'author': ['Michael', 202], 'factoid': 235092},
            'Basilakios, Orationes et epistulae': {'author': ['Nikephoros', 17003], 'factoid': 437070},
            'Basileios of Calabria to Nikolaos III': {'author': ['Basileios', 254]},
            'Boilas': {'author': ['Eustathios', 105], 'factoid': 226498},
            'Bryennios': {'author': ['Nikephoros', 117], 'factoid': 237218},
            'Christophoros of Mitylene': {'author': ['Christophoros', 13102]},  # opera
            'Chrysobull of 1079': {'author': ['Nikephoros', 3], 'factoid': 224945},
            'Clement III to Basileios of Calabria': {'author': ['Klemes', 23], 'factoid': 444557},
            'Domenico of Grado': {'author': ['Dominikos', 101], 'factoid': 434887},
            'Edict on Clergy Reform': {'author': ['Alexios', 1], 'factoid': 444531},
            'Edict on clergy reform': {'author': ['Alexios', 1], 'factoid': 444531},
            'Eustathios Romaios': {'author': ['Eustathios', 61], 'factoid': 374066},
            'Eustathios: Capture of Thessalonike': {'author': ['Eustathios', 20147], 'factoid': 451468},
            'Fulcher of Chartres': {'author': ['Fulcher', 101], 'factoid': 442407},
            # http://apps.brepolis.net.uaccess.univie.ac.at/lexiema/test/Default2.aspx
            'Glykas': {'author': ['Michael', 305]},  # Authority/source is actually Schreiner!
            'Gregory VII, Epistolae vagantes': {'author': ['Gregorios', 27], 'factoid': 444761},
            'Gregory VII, in Caspar': {'author': ['Gregorios', 27]},  # opera
            'Humbert, Commemoratio': {'author': ['Humbert', 101]},  # no explicit factoid
            'Humbert, Dialogus': {'author': ['Humbert', 101], 'factoid': 445015},
            'Humbert, Excommunication': {'author': ['Humbert', 101], 'factoid': 435209},
            'Ioannes Italos': {'author': ['Ioannes', 66]},  # opera
            'Italikos': {'author': ['Michael', 20130]},  # opera
            'Kekaumenos': {'author': ['Anonymus', 274], 'factoid': 228104},
            'Keroularios  ': {'author': ['Michael', 11]},  # opera
            'Kinnamos': {'author': ['Ioannes', 17001], 'factoid': 356015},
            'Leo IX  ': {'author': ['Leon', 29]},  # opera
            'Leon of Chalcedon': {'author': ['Leon', 114], 'factoid': 444848},
            'Leon of Ohrid (Greek)': {'author': ['Leon', 108], 'factoid': 434954},
            'Leon of Ohrid (Latin)': {'author': ['Humbert', 101], 'factoid': 434954},
            'Manasses, Chronicle': {'author': ['Konstantinos', 302], 'factoid': 441043},
            'Manasses, Chronicle: Dedication': {'author': ['Konstantinos', 302], 'factoid': 440958},
            'Manganeios Prodromos': {'author': ['Manganeios', 101]},  # opera
            'Mauropous: Letters': {'author': ['Ioannes', 289]},  # opera
            'Mauropous: Orations': {'author': ['Ioannes', 289]},  # opera
            'Michael the Rhetor, Regel': {'author': ['Michael', 17004], 'factoid': 449588},
            'Michel, Amalfi': {'author': ['Laycus', 101], 'factoid': 445024},
            'Nicolas d\'Andida': {'author': ['Nikolaos', 257], 'factoid': 444805},
            'Nicole, Chartophylax': {'author': ['Alexios', 1], 'factoid': 444947},
            'Niketas Choniates, Historia': {'author': ['Niketas', 25001], 'factoid': 435679},
            'Niketas Stethatos (Darrouzes)': {'author': ['Niketas', 105]},  # opera
            'Niketas Stethatos, On the Holy Spirit': {'author': ['Niketas', 105], 'factoid': 445329},
            'Nikolaos III to Urban II': {'author': ['Nikolaos', 13], 'factoid': 444667},
            'Oath of Eudokia': {'author': ['Eudokia', 1], 'factoid': 380209},
            'Odo of Deuil': {'author': ['Odo', 102], 'factoid': 445727},
            'Pakourianos': {'author': ['Gregorios', 61], 'factoid': 254714},
            'Paschal II, Letter to Alexios I': {'author': ['Paschales', 22], 'factoid': 444991},
            'Petros of Antioch  ': {'author': ['Petros', 103]},  # multiple
            'Petros of Antioch, ep. 2': {'author': ['Petros', 103], 'factoid': 435035},
            'Prodromos, Historische Gedichte': {'author': ['Theodoros', 25001]},  # opera
            'Psellos': {'author': ['Michael', 61]},  # opera
            'Psellos: Chronographia': {'author': ['Michael', 61], 'factoid': 249646},
            'Ralph of Caen': {'author': ['Radulf', 112]},  # no explicit factoid
            'Semeioma on Leon of Chalcedon': {'author': ['Alexios', 1], 'factoid': 444854},
            'Skylitzes': {'author': ['Ioannes', 110], 'factoid': 223966},
            'Skylitzes Continuatus': {'author': ['Anonymus', 102]},  # this is a placeholder person from PBW
            'Theophylact of Ohrid, Speech to Alexios I': {'author': ['Theophylaktos', 105], 'factoid': 444549},
            'Theophylaktos of Ohrid, Letters': {'author': ['Theophylaktos', 105]},  # opera
            'Tornikes, Georgios': {'author': ['Georgios', 25002]},  # opera
            'Tzetzes, Historiai': {'author': ['Ioannes', 459], 'factoid': 449306},
            'Tzetzes, Letters': {'author': ['Ioannes', 459]},  # opera
            'Usama': {'author': ['Usama', 101]},  # no explicit factoid
            'Victor (pope)': {'author': ['Victor', 23], 'factoid': 444676},
            'Walter the Chancellor': {'author': ['Walter', 101], 'factoid': 441713},
            'William of Tyre': {'author': ['William', 4001], 'factoid': 450027},
            'Zetounion': {'author': ['Nikolaos', 13], 'factoid': 445037},
            'Zonaras': {'author': ['Ioannes', 6007]}  # no explicit factoid
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
            'C1': '`sdh-so__C1_Social_Quality_of_an_Actor`',
            'C5': '`sdh-so__C5_Membership`',
            'C7': '`sdh-so__C7_Occupation`',
            'C11': '`sdh-so__C11_Gender`',
            'C12': '`sdh-so__C12_Actors_Social_Role`',
            'C21': 'sdh__C21_Skill',
            'C23': '`sdh-so__C23_Religious_identity`',
            'C24': '`sdh-so__C24_Religion_or_religious_denomination`',
            'C29': '`sdh__C29_Know-How`',
            'E13': 'crm__E13_Attribute_Assignment',
            'E15': 'crm__E15_Identifier_Assignment',
            'E17': 'crm__E17_Type_Assignment',
            'E18': 'crm__E18_Physical_Thing',
            'E21': 'crm__E21_Person',
            'E22': '`crm__E22_Human-Made_Object`',
            'E31': 'crm__E31_Document',
            'E34': 'crm__E34_Inscription',
            'E39': 'crm__E39_Actor',
            'E41': 'crm__E41_Appellation',
            'E42': 'crm__E42_Identifier',
            'E52': '`crm__E52_Time-Span`',
            'E55': 'crm__E55_Type',
            'E56': 'crm__E56_Language',
            'E62': 'crm__E62_String',
            'E69': 'crm__E69_Death',
            'E73': 'crm__E73_Information_Object',
            'E74': 'crm__E74_Group',
            'F1': 'lrmer__F1_Work',
            'F2': 'lrmer__F2_Expression',
            'F27': 'lrmer__F27_Work_Creation',
            'F28': 'lrmer__F28_Expression_Creation'
        }

        self.predicates = {
            'P1': 'crm__P1_is_identified_by',
            'P2': 'crm__P2_has_type',
            'P3': 'crm__P3_has_note',
            'P4': 'crm__P4_has_time_span',
            'P14': 'crm__P14_carried_out_by',
            'P17': 'crm__P17_was_motivated_by',
            'P37': 'crm__P37_assigned',
            'P41': 'crm__P41_classified',
            'P42': 'crm__P42_assigned',
            'P51': 'crm__P51_has_former_or_current_owner',
            'P70': 'crm__P70_documents',
            'P94': 'crm__P94_has_created',
            'P100': 'crm__P100_was_death_of',
            'P107': 'crm__P107_has_current_or_former_member',
            'P127': 'crm__P127_has_broader_term',
            'P128': 'crm__P128_carries',
            'P140': 'crm__P140_assigned_attribute_to',
            'P141': 'crm__P141_assigned',
            'P148': 'crm__P148_has_component',
            'P165': 'crm__P165_incorporates',
            'P177': 'crm__P177_assigned_property_type',
            'R3': 'lrmer__R3_is_realised_in',
            'R5': 'lrmer__R5_has_component',
            'R16': 'lrmer__R16_created',
            'R17': 'lrmer__R17_created',
            'R76': 'lrmer__R76_is_derivative_of',
            'SP13': '`sdh-so__P13_pertains_to`',
            'SP14': '`sdh-so__P14_is_defined_by`',
            'SP35': '`sdh-so__P35_is_defined_by`',
            'SP36': '`sdh-so__P36_pertains_to`',
            'SP37': '`sdh-so__P37_concerns`',
            'SP38': 'sdh__P38_has_skill'
        }

        self.prednodes = dict()

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
        self.star_subject = self.predicates['P140']
        self.star_predicate = self.predicates['P177']
        self.star_object = self.predicates['P141']
        self.star_source = self.predicates['P17']
        self.star_auth = self.predicates['P14']

        # Get the list of factoid types
        self.factoidTypes = [x.typeName for x in sqlsession.query(pbw.FactoidType).all()
                             if x.typeName != '(Unspecified)']
        # Get the classes of info that are directly in the person record
        self.directPersonRecords = ['Gender', 'Disambiguation', 'Identifier']

        def _init_typology(session, crmclass, supertype, instances):
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
                cypherq = ''
                if supertype is not None:
                    cypherq = "MERGE (super:%s {value:\"%s\", constant:TRUE}) " % (self.get_label('E55'), supertype)
                for inst in batch:
                    # Leave out blank instances
                    if inst == '':
                        continue
                    var = "inst%d" % i
                    i += 1
                    varmap[var] = inst
                    cypherq += "MERGE (%s:%s {value:\"%s\", constant:TRUE})" % (var, crmclass, inst)
                    if supertype is not None:
                        cypherq += "-[:%s]->(super)" % self.get_label('P2')
                cypherq += " RETURN %s" % ', '.join(["%s" % x for x in varmap.keys()])
                # print(cypherq)
                types = session.run(cypherq).single()
                for k, v in types.items():
                    retmap[varmap[k]] = v.id
            return retmap

        #  Initialise constants held in the SQL database
        print("Setting up PBW constants...")
        with graphdriver.session() as session:
            # Make our anonymous agent PBW for the un-sourced information
            self.generic_agent = session.run("MERGE (a:%s {identifier:'PBW editors', constant:TRUE}) return a"
                                             % self.entitylabels.get('E39')).single()['a']

            # Some of these factoid types have their own controlled vocabularies. Extract them here and
            # simplify the broader term.
            self.cv = dict()
            self.cv['Gender'] = _init_typology(session, self.get_label('C11'), None, ['Female', 'Male', 'Eunuch'])
            self.cv['Ethnicity'] = _init_typology(session, self.get_label('E74'), 'Ethnic Group',
                                                  [x.ethName for x in sqlsession.query(pbw.Ethnicity).all()])
            self.cv['Religion'] = _init_typology(session, self.get_label('C24'), None,
                                                 [x.religionName for x in sqlsession.query(pbw.Religion).all()])
            self.cv['Language'] = _init_typology(session, self.get_label('C29'), 'Language Skill',
                                                 [x.languageName for x in sqlsession.query(pbw.LanguageSkill).all()])

            # Special-case 'slave' and possibly other items out of the occupation list
            sc_occupation = ['Slave']
            self.cv['SocietyRole'] = _init_typology(session, self.get_label('C7'), None,
                                                    [x.occupationName for x in sqlsession.query(pbw.Occupation).all()
                                                     if x.occupationName not in sc_occupation])
            for sc in sc_occupation:
                cypherq = 'MERGE (s:%s {value:"%s"}) RETURN s' % (self.get_label('C12'), sc)
                self.cv['SocietyRole'][sc] = session.run(cypherq).single()['s'].id

            # Dignities in PBW tend to be specific to institutions / areas;
            # make an initial selection by breaking on the 'of'
            all_dignities = set()
            for x in sqlsession.query(pbw.DignityOffice).all():
                if ' of the ' in x.stdName:  # Don't split (yet) titles that probably don't refer to places
                    dignity = [x.stdName]
                else:
                    dignity = x.stdName.split(' of ')
                all_dignities.add(dignity[0])
            self.cv['Dignity'] = _init_typology(session, self.get_label('C12'), 'Dignity or Office',
                                                list(all_dignities))

            # Kinship is expressed as typed predicates as opposed to E55 Types.
            kinnodes = {}
            for x in sqlsession.query(pbw.KinshipType).all():
                kt = x.gspecRelat
                cypherq = "MERGE (kt:%s {type:\"%s\"}) RETURN kt" % (self.get_label('P107'), kt)
                result = session.run(cypherq).single()
                kinnodes[kt] = result['kt'].id
            self.cv['Kinship'] = kinnodes

    # Lookup functions

    def author(self, a):
        """Return the PBW person identifier for the given source author."""
        return self.authorlist.get(a, None)

    def authorities(self, a):
        """Return the name(s) of the scholar(s) responsible for ingesting the info from the given source
        into the database. Information on the latter is taken from https://pbw2016.kdl.kcl.ac.uk/ref/sources/
        and https://pbw2016.kdl.kcl.ac.uk/ref/seal-editions/"""
        return self.authoritylist.get(a, None)

    def get_label(self, lbl):
        """Return the fully-qualified entity or predicate label given the short name.
        We want this to throw an exception if nothing is found."""
        try:
            return self.entitylabels[lbl]
        except KeyError:
            return self.predicates[lbl]

    def get_predicate(self, p):
        """Return the reified predicate node for the given short name. We want this to throw
        an exception if no predicate with this key is defined."""
        if p not in self.prednodes:
            fqname = self.predicates[p]
            with self.graphdriver.session() as session:
                npred = session.run("MERGE (n:%s {constant:TRUE}) RETURN n" % fqname).single()['n']
                self.prednodes[p] = npred.id
        return self.prednodes[p]

    def inrange(self, floruit):
        """Return true if the given floruit tag falls within RELEVEN's range"""
        return floruit in self.allowed
