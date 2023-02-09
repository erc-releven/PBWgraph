import pbw
# This package contains a bunch of information curated from the PBW website about authority, authorship
# and so forth. It is a huge laundry list of data and some initialiser and accessor functions for it; the
# class requires a graph driver in order to do the initialisation.


class PBWstarConstants:
    """A class to deal with all of our constants, where the data is nicely encapsulated"""

    def __init__(self, sqlsession, graphdriver):
        self.sqlsession = sqlsession
        self.graphdriver = graphdriver

        # These are the modern scholars who put the source information into PBW records
        mj = {'identifier': 'Michael Jeffreys', 'viaf': '73866641'}
        # We need Michael and Tara on the outside
        self.mj = mj
        tp = {'identifier': 'Tassos Papacostas', 'viaf': '316793603'}
        ta = {'identifier': 'Tara Andrews', 'viaf': '316505144'}
        self.ta = ta
        jr = {'identifier': 'Judith Ryder', 'viaf': '51999624'}
        mw = {'identifier': 'Mary Whitby', 'viaf': '34477027'}
        wa = {'identifier': 'Wahid Amin', 'viaf': '213149617100303751518'}
        bs = {'identifier': 'Bruna Soravia', 'viaf': '69252167'}
        hm = {'identifier': 'Harry Munt', 'viaf': '78568758'}
        lo = {'identifier': 'Letizia Osti', 'viaf': '236145542536996640148'}
        cr = {'identifier': 'Charlotte Roueché', 'viaf': '44335536'}
        ok = {'identifier': 'Olga Karagiorgiou', 'viaf': '253347413'}

        self.sourcelist = {
            'Albert of Aachen': {'author': ['Albert', 26101], 'factoid': 432193, 'authority': [mj],
                                 'work': 'Historia Ierosolimitana',
                                 'expression': 'Susan B. Edgington, Historia Ierosolimitana, '
                                               'History of the Journey to Jerusalem, Oxford 2007'},
            'Alexios Stoudites': {'author': ['Alexios', 11], 'authority': [tp],
                                  'expression': 'G. Ficker, Erlasse des Patriarchen von Konstantinopel Alexios '
                                                'Studites, Kiel 1911'},
            'Anna Komnene': {'author': ['Anna', 62], 'factoid': 437887, 'work': 'Alexias', 'authority': [mj],
                             'expression': 'D.R. Reinsch and A. Kambylis, Annae Comnenae Alexias, '
                                           'Corpus fontium historiae Byzantinae 40/1, Berlin – New York 2001'},
            'Annales Barenses': {'authority': [mj]},
            'Anonymus Barensis': {'authority': [mj]},
            'Aristakes': {'author': ['Aristakes', 101], 'factoid': 375021, 'authority': [tp, ta]},
            'Attaleiates: Diataxis': {'author': ['Michael', 202], 'factoid': 254236, 'authority': [tp]},
            'Attaleiates: History': {'author': ['Michael', 202], 'factoid': 235092, 'authority': [tp]},
            'Basilakios, Orationes et epistulae': {'author': ['Nikephoros', 17003], 'factoid': 437070, 'authority': []},
            'Basileios of Calabria to Nikolaos III': {'author': ['Basileios', 254], 'authority': [mj]},
            'Boilas': {'author': ['Eustathios', 105], 'factoid': 226498, 'authority': [tp]},
            'Bryennios': {'author': ['Nikephoros', 117], 'factoid': 237218, 'authority': [tp],
                          'work': 'Hyle Historias',
                          'expresssion': 'P. Gautier, Nicéphore Bryennios: Histoire. Introduction, texte, '
                                         'traduction et notes, Brussels 1975'},
            'Cheynet, Antioche et Tarse': {'authority': [ok]},
            'Christophoros of Mitylene': {'author': ['Christophoros', 13102], 'authority': [mj]},  # opera
            'Christos Philanthropos, note': {'authority': [mj]},
            'Chrysobull of 1079': {'author': ['Nikephoros', 3], 'factoid': 224945, 'authority': [tp]},
            'Clement III to Basileios of Calabria': {'author': ['Klemes', 23], 'factoid': 444557, 'authority': [jr]},
            'Codice Diplomatico Barese IV': {'authority': [mw]},
            'Council of 1147': {'authority': [mj]},
            'Council of 1157': {'authority': [mj]},
            'Dionysiou': {'authority': [tp]},
            'Docheiariou': {'authority': [tp]},
            'Documents d\'ecclesiologie ': {'authority': [jr]},
            'Domenico of Grado': {'author': ['Dominikos', 101], 'factoid': 434887, 'authority': [jr]},
            'Droit matrimonial': {'authority': [jr]},
            'Edict on Clergy Reform': {'author': ['Alexios', 1], 'factoid': 444531, 'authority': [mj]},
            'Edict on clergy reform': {'author': ['Alexios', 1], 'factoid': 444531, 'authority': [mj]},
            'Eleousa: Acts': {'authority': [tp]},
            'Eleousa: Typikon': {'authority': [mw]},
            'Esphigmenou': {'authority': [tp]},
            'Eustathios Romaios': {'author': ['Eustathios', 61], 'factoid': 374066, 'authority': [mj]},
            'Eustathios: Capture of Thessalonike': {'author': ['Eustathios', 20147], 'factoid': 451468,
                                                    'authority': [mj], 'work': 'De Thessalonica a Latinis capta',
                                                    'expression': 'S. Kyriakides, La espugnazione di Tessalonica, '
                                                                  'Palermo 1961'},
            'Fulcher of Chartres': {'author': ['Fulcher', 101], 'factoid': 442407, 'authority': [mj]},
            'Geonames': {'authority': [cr]},
            # http://apps.brepolis.net.uaccess.univie.ac.at/lexiema/test/Default2.aspx
            'Glykas': {'author': ['Michael', 305], 'authority': [tp]},  # Authority/source is actually Schreiner!
            'Gregory VII, Epistolae vagantes': {'author': ['Gregorios', 27], 'factoid': 444761, 'authority': [jr]},
            'Gregory VII, in Caspar': {'author': ['Gregorios', 27], 'authority': [jr]},  # opera
            'Hilandar': {'authority': [tp]},
            'Humbert, Commemoratio': {'author': ['Humbert', 101], 'authority': [jr]},  # no explicit factoid
            'Humbert, Dialogus': {'author': ['Humbert', 101], 'factoid': 445015, 'authority': [jr]},
            'Humbert, Excommunication': {'author': ['Humbert', 101], 'factoid': 435209, 'authority': [jr]},
            'Hypomnema on marriage': {'authority': [mj]},
            'Hypomnesis of May, 1094': {'authority': []},
            'Ibn Shaddad': {'authority': [bs, hm]},
            'Ibn al-Athir': {'authority': [wa]},
            'Ioannes Italos': {'author': ['Ioannes', 66], 'authority': [mj]},  # opera
            'Italikos': {'author': ['Michael', 20130], 'authority': [mj]},  # opera
            'Italos trial': {'authority': [mj]},
            'Iveron': {'authority': [tp]},
            'Jus Graeco-Romanum, III': {'authority': [mj]},
            'Kastamonitou': {'authority': [tp]},
            'Kecharitomene': {'authority': [mj]},
            'Kekaumenos': {'author': ['Anonymus', 274], 'factoid': 228104, 'authority': [tp]},
            'Keroularios  ': {'author': ['Michael', 11], 'authority': [jr]},  # opera
            'Kinnamos': {'author': ['Ioannes', 17001], 'factoid': 356015, 'authority': [mj]},
            'Kleinchroniken': {'authority': [tp]},
            'Koltsida-Makri': {'authority': [ok]},
            'Kyrillos Phileotes': {'authority': [tp]},
            'Laurent, Corpus V.2': {'authority': [ok]},
            'Laurent, Corpus V.3': {'authority': [ok]},
            'Lavra': {'authority': [tp]},
            'Lazaros of Galesion': {'authority': [tp]},
            'Leo IX  ': {'author': ['Leon', 29], 'authority': [jr]},  # opera
            'Leon of Chalcedon': {'author': ['Leon', 114], 'factoid': 444848, 'authority': [jr]},
            'Leon of Ohrid (Greek)': {'author': ['Leon', 108], 'factoid': 434954, 'authority': [jr]},
            'Leon of Ohrid (Latin)': {'author': ['Leon', 108, 'Humbert', 101], 'factoid': 434954, 'authority': [jr]},
            'Lupus protospatharius': {'authority': [mj]},
            'Manasses, Chronicle': {'author': ['Konstantinos', 302], 'factoid': 441043, 'authority': [mj]},
            'Manasses, Chronicle: Dedication': {'author': ['Konstantinos', 302], 'factoid': 440958, 'authority': [mj]},
            'Manasses, Hodoiporikon': {'authority': [mj]},
            'Manganeios Prodromos': {'author': ['Manganeios', 101], 'authority': [mj]},  # opera
            'Matthew of Edessa': {'authority': [ta]},
            'Mauropous: Letters': {'author': ['Ioannes', 289], 'authority': [tp]},  # opera
            'Mauropous: Orations': {'author': ['Ioannes', 289], 'authority': [tp]},  # opera
            'Michael the Rhetor, Regel': {'author': ['Michael', 17004], 'factoid': 449588, 'authority': [mj]},
            'Michel, Amalfi': {'author': ['Laycus', 101], 'factoid': 445024, 'authority': [jr]},
            'Nea Mone': {'authority': [tp]},
            'Nea Mone,': {'authority': [tp]},
            'Nicolas d\'Andida': {'author': ['Nikolaos', 257], 'factoid': 444805, 'authority': [jr]},
            'Nicole, Chartophylax': {'author': ['Alexios', 1], 'factoid': 444947, 'authority': [jr]},
            'Niketas Choniates, Historia': {'author': ['Niketas', 25001], 'factoid': 435679, 'authority': [mj]},
            'Niketas Stethatos (Darrouzes)': {'author': ['Niketas', 105], 'authority': [jr]},  # opera
            'Niketas Stethatos, On the Holy Spirit': {'author': ['Niketas', 105], 'factoid': 445329, 'authority': [jr]},
            'Nikolaos III to Urban II': {'author': ['Nikolaos', 13], 'factoid': 444667, 'authority': [jr]},
            'Oath of Eudokia': {'author': ['Eudokia', 1], 'factoid': 380209, 'authority': [mj]},
            'Odo of Deuil': {'author': ['Odo', 102], 'factoid': 445727, 'authority': [mj]},
            'Pakourianos': {'author': ['Gregorios', 61], 'factoid': 254714, 'authority': [tp]},
            'Panteleemon': {'authority': [tp]},
            'Pantokrator (Athos)': {'authority': [tp]},
            'Pantokrator Typikon': {'authority': [tp]},
            'Parthenon inscriptions': {'authority': [mj]},
            'Paschal II, Letter to Alexios I': {'author': ['Paschales', 22], 'factoid': 444991, 'authority': [mj]},
            'Patmos: Acts': {'authority': [tp]},
            'Patmos: Codicil': {'authority': [mw]},
            'Patmos: Testament': {'authority': [mw]},
            'Patmos: Typikon': {'authority': [mw]},
            'Peri metatheseon': {'authority': [mj]},
            'Petros of Antioch  ': {'author': ['Petros', 103], 'authority': [tp]},  # multiple
            'Petros of Antioch, ep. 2': {'author': ['Petros', 103], 'factoid': 435035, 'authority': [tp]},
            'Pleiades': {'authority': [cr]},
            'Prodromos, Historische Gedichte': {'author': ['Theodoros', 25001], 'authority': [mj]},  # opera
            'Protaton': {'authority': [tp]},
            'Psellos': {'author': ['Michael', 61], 'authority': [mj, tp]},  # opera
            'Psellos: Chronographia': {'author': ['Michael', 61], 'factoid': 249646, 'authority': [mw]},
            'Ralph of Caen': {'author': ['Radulf', 112], 'authority': [mj]},  # no explicit factoid
            'Sakkos (1166)': {'authority': [mj]},
            'Sakkos (1170)': {'authority': [mj]},
            'Seibt – Zarnitz': {'authority': [ok]},
            'Semeioma on Leon of Chalcedon': {'author': ['Alexios', 1], 'factoid': 444854, 'authority': [jr]},
            'Skylitzes': {'author': ['Ioannes', 110], 'factoid': 223966, 'authority': [tp]},
            'Skylitzes Continuatus': {'author': ['Anonymus', 102], 'authority': [tp]},  # placeholder person from PBW
            'Sode, Berlin': {'authority': [ok]},
            'Speck, Berlin': {'authority': [ok]},
            'Stavrakos': {'authority': [ok]},
            'Synod of 1072': {'authority': [mj]},
            'Synod of 1094': {'authority': [tp]},
            'Synodal edict (1054)': {'authority': [jr]},
            'Synodal protocol (1089)': {'authority': [jr]},
            'Synopsis Chronike': {'authority': []},
            'Thebes: Cadaster': {'authority': [mj]},
            'Thebes: Confraternity': {'authority': [mj]},
            'Theophylact of Ohrid, Speech to Alexios I': {'author': ['Theophylaktos', 105], 'factoid': 444549,
                                                          'authority': [mj]},
            'Theophylaktos of Ohrid, Letters': {'author': ['Theophylaktos', 105], 'authority': [mj]},  # opera
            'Tornikes, Georgios': {'author': ['Georgios', 25002], 'authority': [mj]},  # opera
            'Tzetzes, Exegesis of Homer': {'authority': [mj]},
            'Tzetzes, Historiai': {'author': ['Ioannes', 459], 'factoid': 449306, 'authority': [mj]},
            'Tzetzes, Homerica': {'authority': [mj]},
            'Tzetzes, Letters': {'author': ['Ioannes', 459], 'authority': [mj]},  # opera
            'Tzetzes, Posthomerica': {'authority': [mj]},
            'Usama': {'author': ['Usama', 101], 'authority': [lo, hm]},  # no explicit factoid
            'Vatopedi': {'authority': [tp]},
            'Victor (pope)': {'author': ['Victor', 23], 'factoid': 444676, 'authority': [jr]},
            'Walter the Chancellor': {'author': ['Walter', 101], 'factoid': 441713, 'authority': [mj]},
            'Wassiliou, Hexamilites': {'authority': [ok]},
            'William of Tyre': {'author': ['William', 4001], 'factoid': 450027, 'authority': [mj]},
            'Xenophontos': {'authority': [tp]},
            'Xeropotamou': {'authority': [tp]},
            'Yahya al-Antaki': {'authority': [tp, lo, hm]},
            'Zacos II': {'authority': [ok]},
            'Zetounion': {'author': ['Nikolaos', 13], 'factoid': 445037, 'authority': [jr]},
            'Zonaras': {'author': ['Ioannes', 6007], 'authority': [mw]}  # no explicit factoid
        }

        self.entitylabels = {
            'C1': 'Resource:sdhss__C1',  # Social Quality of an Actor
            'C5': 'Resource:sdhss__C5',  # Membership
            'C7': 'Resource:sdhss__C7',  # Occupation
            'C11': 'Resource:sdhss__C11',  # Gender
            'C12': 'Resource:sdhss__C12',  # Social Role
            'C13': 'Resource:sdhss__C13',  # Social Role Embodiment
            'C21': 'Resource:sdhss__C21',  # Skill
            'C23': 'Resource:sdhss__C23',  # Religious Identity
            'C24': 'Resource:sdhss__C24',  # Religion or Religious Denomination
            'C29': 'Resource:sdhss__C29',  # Know-How
            'E13': 'Resource:crm__E13_Attribute_Assignment',
            'E15': 'Resource:crm__E15_Identifier_Assignment',
            'E17': 'Resource:crm__E17_Type_Assignment',
            'E18': 'Resource:crm__E18_Physical_Thing',
            'E21': 'Resource:crm__E21_Person',
            'E22': 'Resource:`crm__E22_Human-Made_Object`',
            'E31': 'Resource:crm__E31_Document',
            'E34': 'Resource:crm__E34_Inscription',
            'E39': 'Resource:crm__E39_Actor',
            'E41': 'Resource:crm__E41_Appellation',
            'E42': 'Resource:crm__E42_Identifier',
            'E52': 'Resource:`crm__E52_Time-Span`',
            'E55': 'Resource:crm__E55_Type',
            'E56': 'Resource:crm__E56_Language',
            'E62': 'Resource:crm__E62_String',
            'E69': 'Resource:crm__E69_Death',
            'E73': 'Resource:crm__E73_Information_Object',
            'E74': 'Resource:crm__E74_Group',
            'F1': 'Resource:lrmoo__F1',  # Work
            'F2': 'Resource:lrmoo__F2',  # Expression
            'F27': 'Resource:lrmoo__F27',  # Work Creation
            'F28': 'Resource:lrmoo__F28'  # Expression Creation
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
            'R3': 'lrmoo__R3',  # is realised in
            'R5': 'lrmoo__R5',  # has component
            'R16': 'lrmoo__R16',  # created [work]
            'R17': 'lrmoo__R17',  # created [expression]
            'R76': 'lrmoo__R76',  # is derivative of
            'SP13': 'sdhss__P13',  # pertains to [person, social quality]
            'SP14': 'sdhss__P14',  # has social quality
            'SP26': 'sdhss__P26',  # is embodiment by [person, social role]
            'SP33': 'sdhss__P33',  # is embodiment of [social role]
            'SP35': 'sdhss__P35',  # is defined by [person, religious identity]
            'SP36': 'sdhss__P36',  # pertains to [religious identity]
            'SP37': 'sdhss__P37',  # concerns [know-how]
            'SP38': 'sdhss__P38'  # has skill
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
        if sqlsession is None:
            self.factoidTypes = []
        else:
            self.factoidTypes = [x.typeName for x in sqlsession.query(pbw.FactoidType).all()
                                 if x.typeName != '(Unspecified)']
        # Get the classes of info that are directly in the person record
        self.directPersonRecords = ['Gender', 'Disambiguation', 'Identifier']

        # Initialise constants held in the SQL database and get their UUIDs.
        print("Setting up PBW constants...")
        # Make our anonymous agent PBW for the un-sourced information
        pbwcmd = "COMMAND (a:%s {descname:'Prosopography of the Byzantine World', " \
                 "prefix:'https://pbw2016.kdl.kcl.ac.uk/person/', " \
                 "constant:TRUE}) RETURN a" % self.entitylabels.get('E39')
        self.pbw_agent = self._fetch_uuid_from_query(pbwcmd)
        # and our VIAF agent for identifying PBW contributors
        viafcmd = "COMMAND (a:%s {descname:'Virtual Internet Authority File', prefix:'https://viaf.org/viaf/', " \
                  "constant:TRUE}) RETURN a" % self.entitylabels.get('E39')
        self.viaf_agent = self._fetch_uuid_from_query(viafcmd)

        # Some of these factoid types have their own controlled vocabularies.
        # Set up our structure for retaining these; we will define them when we encounter them
        # through the accessor functions.
        self.cv = {
            'Gender': dict(),
            'Ethnicity': dict(),
            'Religion': dict(),
            'Language': dict(),
            'SocietyRole': dict(),
            'Dignity': dict(),
            'Kinship': dict()
        }
        # Special-case 'slave' and ordained/consecrated roles out of 'occupations'
        self.legal_designations = ['Slave', 'Monk', 'Nun', 'Nun (?)', 'Bishops', 'Monk (Latin)', 'Patriarch',
                                   'Servant of Christ', 'Servant of God', 'Hieromonk', 'Servant of the Lord']

        # Make this generic predicate as a one-off. This is a hack to keep from breaking
        # get_predicate later
        with self.graphdriver.session() as session:
            session.run('MERGE (k:Resource:%s {constant:TRUE}) RETURN k' % self.get_label('P107'))
    # END OF __init__
    # Lookup functions

    def source(self, a):
        """Return all the information we have for the given source ID"""
        return self.sourcelist.get(a, None)

    def author(self, a):
        """Return the PBW person identifier for the given source author."""
        srecord = self.sourcelist.get(a, None)
        if srecord is None:
            return None
        return srecord.get('author', None)

    def authorities(self, a):
        """Return the name(s) of the scholar(s) responsible for ingesting the info from the given source
        into the database. Information on the latter is taken from https://pbw2016.kdl.kcl.ac.uk/ref/sources/
        and https://pbw2016.kdl.kcl.ac.uk/ref/seal-editions/"""
        srecord = self.sourcelist.get(a, None)
        if srecord is None:
            return None
        return srecord.get('authority', None)

    def get_label(self, lbl):
        """Return the fully-qualified entity or predicate label given the short name.
        We want this to throw an exception if nothing is found."""
        try:
            return self.entitylabels[lbl]
        except KeyError:
            return self.predicates[lbl]

    def get_predicate(self, p):
        """Return the reified predicate UUID for the given short name. This will throw
        an exception if no predicate with this key is defined."""
        # Deal with the fact that some predicates (currently only for kinship) have specific
        # instances, and here we always want to return the general instance
        specific_properties = {'P107': '`crm__P107.1_kind_of_member`'}
        if p not in self.prednodes:
            fqname = self.predicates[p]
            restriction = ''
            if p in specific_properties:
                restriction = ' WHERE n.%s IS NULL' % specific_properties[p]
            predq = "COMMAND (n:Resource:%s {constant:TRUE})%s RETURN n" % (fqname, restriction)
            npred = self._fetch_uuid_from_query(predq)
            self.prednodes[p] = npred
        return self.prednodes[p]

    # Accessors / creators for our controlled vocabularies
    def _find_or_create_cv_entry(self, category, nodeclass, label, superlabel=None):
        if label in self.cv[category]:
            return self.cv[category][label]
        # We have to create the node, possibly attaching it to a superclass
        nodeq = "(cventry:%s {constant:TRUE, value:\"%s\"})" % (nodeclass, label)
        nq = "COMMAND %s" % nodeq
        if superlabel is not None:
            nq = "MERGE (super:%s {constant:TRUE, value:\"%s\"}) WITH super COMMAND %s-[:%s]->(super) " % (
                self.get_label('E55'), superlabel, nodeq, self.get_label('P2'))
        nq += " RETURN cventry"
        uuid = self._fetch_uuid_from_query(nq)
        if uuid is None:
            raise('UUID for %s label (%s) not found' % (label, category))
        self.cv[category][label] = uuid
        return uuid

    def get_gender(self, gender):
        return self._find_or_create_cv_entry('Gender', self.get_label('C11'), gender)

    def get_religion(self, rel):
        return self._find_or_create_cv_entry('Religion', self.get_label('C24'), rel)

    def get_ethnicity(self, ethlabel):
        return self._find_or_create_cv_entry('Ethnicity', self.get_label('E74'), ethlabel, 'Ethnic Group')

    def get_language(self, lang):
        return self._find_or_create_cv_entry('Language', self.get_label('C29'), lang, 'Language Skill')

    def get_societyrole(self, srlabel):
        if srlabel in self.legal_designations:
            return self._find_or_create_cv_entry('SocietyRole', self.get_label('C12'), srlabel, 'Legal Status')
        else:
            return self._find_or_create_cv_entry('SocietyRole', self.get_label('C7'), srlabel)

    def get_dignity(self, dignity):
        # Dignities in PBW tend to be specific to institutions / areas;
        # make an initial selection by breaking on the 'of'
        diglabel = [dignity]
        if ' of the ' not in dignity:  # Don't split (yet) titles that probably don't refer to places
            diglabel = dignity.split(' of ')
        dig_uuid = self._find_or_create_cv_entry('Dignity', self.get_label('C12'), diglabel[0], 'Legal Status')
        # Make sure that the UUID also appears under the original label
        self.cv['Dignity'][dignity] = dig_uuid
        return dig_uuid

    def get_kinship(self, kinlabel):
        if kinlabel in self.cv['Kinship']:
            return self.cv['Kinship'][kinlabel]
        # Kinship nodes are typed predicates
        cypherq = "COMMAND (kt:Resource:%s {`crm__P107.1_kind_of_member`:\"%s\", constant:TRUE}) RETURN kt" % (
            self.get_label('P107'), kinlabel)
        uuid = self._fetch_uuid_from_query(cypherq)
        self.cv['Kinship'][kinlabel] = uuid
        return uuid

    def inrange(self, floruit):
        """Return true if the given floruit tag falls within RELEVEN's range"""
        return floruit in self.allowed

    def _fetch_uuid_from_query(self, q):
        """Helper function to create one node if it doesn't already exist and return the UUID that gets
        auto-generated upon commit. The query passed in should have COMMAND where the node is match/created,
        and should RETURN the node whose UUID is wanted."""
        matchq = q.replace("COMMAND", "MATCH")
        createq = q.replace("COMMAND", "CREATE")
        with self.graphdriver.session() as session:
            uuid = session.run("%s.uuid AS theid" % matchq).single()
            if uuid is None:
                session.run(createq)
                uuid = session.run("%s.uuid AS theid" % matchq).single()
            return uuid['theid']
