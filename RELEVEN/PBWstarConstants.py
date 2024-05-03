import RELEVEN.PBWSources
from os.path import join, dirname
# This package contains a bunch of information curated from the PBW website about authority, authorship
# and so forth. It is a huge laundry list of data and some initialiser and accessor functions for it; the
# class requires a graph driver in order to do the initialisation.


class PBWstarConstants:
    """A class to deal with all of our constants, where the data is nicely encapsulated"""

    def __init__(self, graphdriver):
        self.graphdriver = graphdriver

        # These are the modern scholars who put the source information into PBW records.
        # We need Michael and Tara on the outside
        self.mj = {'identifier': 'Jeffreys, Michael J.', 'viaf': '73866641'}
        self.ta = {'identifier': 'Andrews, Tara Lee', 'viaf': '316505144'}

        self.sourcelist = RELEVEN.PBWSources.PBWSources(join(dirname(__file__), 'pbw_sources.csv'))

        self.entitylabels = {
            'C1': 'Resource:sdhss__C1',    # Social Quality of an Actor (Embodiment)
            'C3': 'Resource:sdhss__C3',    # Social Relationship
            'C4': 'Resource:sdhss__C4',    # Social Relationship Type
            'C5': 'Resource:sdhss__C5',    # Membership
            'C7': 'Resource:sdhss__C7',    # Occupation
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
            'E22B': 'Resource:spec__Boulloterion',
            'E22S': 'Resource:spec__Lead_Seal',
            'E31': 'Resource:crm__E31_Document',
            'E33': 'Resource:crm__E33_Linguistic_Object:crm__E73_Information_Object',
            'E34': 'Resource:crm__E34_Inscription:crm__E73_Information_Object',
            'E39': 'Resource:crm__E39_Actor',
            'E41': 'Resource:crm__E41_Appellation',
            'E42': 'Resource:crm__E42_Identifier',
            'E52': 'Resource:`crm__E52_Time-Span`',
            'E55': 'Resource:crm__E55_Type',
            'E56': 'Resource:crm__E56_Language',
            'E62': 'Resource:crm__E62_String',
            'E69': 'Resource:crm__E69_Death',
            'E73': 'Resource:crm__E73_Information_Object',
            'E73B': 'Resource:spec__Bibliography',
            'E74': 'Resource:crm__E74_Group',
            'E74A': 'Resource:spec__Author_Group',
            'E74E': 'Resource:spec__Ethnic_Group',
            'E78': 'Resource:crm__E78_Curated_Holding',
            'E87': 'Resource:crm__E87_Curation_Activity',
            'F1': 'Resource:lrmoo__F1',    # Work
            'F2': 'Resource:lrmoo__F2',    # Expression - e.g. a database record
            'F3': 'Resource:lrmoo__F3',    # Publication - e.g. an edition or journal article
            'F11': 'Resource:lrmoo__F11',  # Corporate Body
            'F27': 'Resource:lrmoo__F27',  # Work Creation
            'F28': 'Resource:lrmoo__F28',  # Expression Creation
        }

        self.predicates = {
            'P1': 'crm__P1_is_identified_by',
            'P2': 'crm__P2_has_type',
            'P3': 'crm__P3_has_note',
            'P4': '`crm__P4_has_time-span`',
            'P14': 'crm__P14_carried_out_by',
            'P16': 'crm__P16_used_specific_object',
            'P17': 'crm__P17_was_motivated_by',
            'P37': 'crm__P37_assigned',
            'P41': 'crm__P41_classified',
            'P42': 'crm__P42_assigned',
            'P46': 'crm__P46_is_composed_of',
            'P48': 'crm__P48_has_preferred_identifier',
            'P51': 'crm__P51_has_former_or_current_owner',
            'P70': 'crm__P70_documents',
            'P80': 'crm__P80_end_is_qualified_by',
            'P94': 'crm__P94_has_created',
            'P100': 'crm__P100_was_death_of',
            'P102': 'crm__P102_has_title',
            'P107': 'crm__P107_has_current_or_former_member',
            'P108': 'crm__P108_has_produced',
            'P127': 'crm__P127_has_broader_term',
            'P128': 'crm__P128_carries',
            'P140': 'crm__P140_assigned_attribute_to',
            'P141': 'crm__P141_assigned',
            'P147': 'crm__P147_curated',
            'P148': 'crm__P148_has_component',
            'P165': 'crm__P165_incorporates',
            'P177': 'crm__P177_assigned_property_type',
            'P190': 'crm__P190_has_symbolic_content',
            'R3': 'lrmoo__R3',     # is realised in
            'R5': 'lrmoo__R5',     # has component
            'R15': 'lrmoo__R15',   # has fragment
            'R16': 'lrmoo__R16',   # created [work]
            'R17': 'lrmoo__R17',   # created [expression]
            'R76': 'lrmoo__R76',   # is derivative of
            'SP13': 'sdhss__P13',  # pertains to [person, social quality]
            'SP14': 'sdhss__P14',  # has social quality
            'SP16': 'sdhss__P16',  # has relationship type
            'SP17': 'sdhss__P17',  # has relationship source
            'SP18': 'sdhss__P18',  # has relationship target
            'SP26': 'sdhss__P26',  # is embodiment by [person, social role]
            'SP33': 'sdhss__P33',  # is embodiment of [social role]
            'SP35': 'sdhss__P35',  # is defined by [person, religious identity]
            'SP36': 'sdhss__P36',  # pertains to [religious identity]
            'SP37': 'sdhss__P37',  # concerns [know-how]
            'SP38': 'sdhss__P38'   # has skill
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

        # Initialise our group agents and the data structures we need to start
        print("Setting up PBW constants...")
        # Make our anonymous agent PBW for the un-sourced information
        try:
            pbwcmd = "COMMAND (a:%s {%s:'Prosopography of the Byzantine World', " \
                     "%s:'https://pbw2016.kdl.kcl.ac.uk/'}) RETURN a" % (
                self.entitylabels.get('F11'), self.get_label('P3'), self.get_label('P1'))
            self.pbw_agent = self._fetch_uuid_from_query(pbwcmd)
            # and our VIAF agent for identifying PBW contributors
            viafcmd = "COMMAND (a:%s {%s:'Virtual Internet Authority File', " \
                      "%s:'https://viaf.org/'}) RETURN a" % (
                self.entitylabels.get('F11'), self.get_label('P3'), self.get_label('P1'))
            self.viaf_agent = self._fetch_uuid_from_query(viafcmd)
        except:
            print("WARNING: no connection to graph database! Not fetching graph constants")

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

        # Make a list of boulloterions that are missing their references, with a link to the publication
        # that the seals come from or -1 if we should use the seal catalogues as sources
        self.boulloterion_sources = {
            4779: (45, 'no. 345'),
        }
    # END OF __init__
    # Lookup functions

    def source(self, factoid):
        """Return all the information we have for the given source ID. Special-case the 'seals' source"""
        if factoid.source == 'Seals':
            return factoid.source
        return self.sourcelist.key_for(factoid.source, factoid.sourceRef)

    def sourceinfo(self, a):
        """Return the whole dictionary of info for the given source key."""
        return self.sourcelist.get(a)

    def author(self, a):
        """Return the PBW person identifier for the given source author."""
        srecord = self.sourcelist.get(a)
        if srecord is None:
            return None
        return srecord.get('author', None)

    def editor(self, a):
        srecord = self.sourcelist.get(a)
        if srecord is None:
            return None
        return srecord.get('editor', None)

    def authorities(self, a):
        """Return the name(s) of the scholar(s) responsible for ingesting the info from the given source
        into the database. Information on the latter is taken from https://pbw2016.kdl.kcl.ac.uk/ref/sources/
        and https://pbw2016.kdl.kcl.ac.uk/ref/seal-editions/"""
        srecord = self.sourcelist.get(a)
        if srecord is None:
            return None
        return srecord.get('authority', None)

    def sourceref(self, factoid):
        """Return the source reference, modified to account for our aggregate sources."""
        return self.sourcelist.sourceref(factoid.source, factoid.sourceRef)

    def get_label(self, lbl):
        """Return the fully-qualified entity or predicate label given the short name.
        We want this to throw an exception if nothing is found."""
        try:
            return self.entitylabels[lbl]
        except KeyError:
            return self.predicates[lbl]

    def get_assertion_for_predicate(self, p):
        """Takes a predicate key and returns the qualified assertion class which implies that predicate.
        This will throw an exception if no predicate is defined for the key."""
        fqname = self.predicates[p]
        (nsstr, name) = fqname.replace('`', '').split('__')
        code = name.split('_')[0]
        return f"Resource:star__E13_{nsstr}_{code}"

    # Accessors / creators for our controlled vocabularies
    def _find_or_create_cv_entry(self, category, nodeclass, label):
        if label in self.cv[category]:
            return self.cv[category][label]
        # We have to create the node, possibly attaching it to a superclass
        dataprop = self.get_label('P1')
        nodeq = "(cventry:%s {%s:\"%s\"})" % (nodeclass, dataprop, label)
        nq = "COMMAND %s" % nodeq
        # if superlabel is not None:
        #     nq = "MERGE (super:%s {%s:\"%s\"}) WITH super COMMAND %s-[:%s]->(super) " % (
        #         self.get_label('E55'), dataprop, superlabel, nodeq, self.get_label('P2'))
        nq += " RETURN cventry"
        uuid = self._fetch_uuid_from_query(nq)
        if uuid is None:
            raise Exception('UUID for %s label (%s) not found' % (label, category))
        self.cv[category][label] = uuid
        return uuid

    def get_gender(self, gender):
        return self._find_or_create_cv_entry('Gender', self.get_label('C11'), gender)

    def get_religion(self, rel):
        return self._find_or_create_cv_entry('Religion', self.get_label('C24'), rel)

    def get_ethnicity(self, ethlabel):
        return self._find_or_create_cv_entry('Ethnicity', self.get_label('E74E'), ethlabel)

    def get_language(self, lang):
        return self._find_or_create_cv_entry('Language', self.get_label('C29'), lang)

    def get_kinship(self, kinlabel):
        return self._find_or_create_cv_entry('Kinship', self.get_label('C4'), kinlabel)

    def get_societyrole(self, srlabel):
        if srlabel in self.legal_designations:
            return self._find_or_create_cv_entry('SocietyRole', self.get_label('C12'), srlabel)
        else:
            return self._find_or_create_cv_entry('SocietyRole', self.get_label('C7'), srlabel)

    def get_dignity(self, dignity):
        # Dignities in PBW tend to be specific to institutions / areas;
        # make an initial selection by breaking on the 'of'
        diglabel = [dignity]
        if ' of the ' not in dignity:  # Don't split (yet) titles that probably don't refer to places
            diglabel = dignity.split(' of ')
        dig_uuid = self._find_or_create_cv_entry('Dignity', self.get_label('C12'), diglabel[0])
        # Make sure that the UUID also appears under the original label
        self.cv['Dignity'][dignity] = dig_uuid
        return dig_uuid

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
