from rdflib import Literal, Namespace
import re
import RELEVEN.PBWSources
from os.path import join, dirname
from uuid import uuid4
# This package contains a bunch of information curated from the PBW website about authority, authorship
# and so forth. It is a huge laundry list of data and some initialiser and accessor functions for it; the
# class requires a graph driver in order to do the initialisation.


class PBWstarConstants:
    """A class to deal with all of our constants, where the data is nicely encapsulated"""

    def __init__(self, graph):
        self.graph = graph
        # These are the modern scholars who put the source information into PBW records.
        # We need Michael and Tara on the outside
        self.mj = {'identifier': 'Jeffreys, Michael J.', 'viaf': '73866641'}
        self.ta = {'identifier': 'Andrews, Tara Lee', 'viaf': '316505144'}

        self.sourcelist = RELEVEN.PBWSources.PBWSources(join(dirname(__file__), 'pbw_sources.csv'))

        self.ns = Namespace('https://r11.eu/rdf/resource/')
        self.namespaces = {
            'crm': Namespace('http://www.cidoc-crm.org/cidoc-crm/'),
            'lrmoo': Namespace('http://iflastandards.info/ns/lrm/lrmoo/'),
            'sdhss': Namespace('https://r11.eu/ns/prosopography/'),
            'spec': Namespace('https://r11.eu/ns/spec/')
        }

        self.entitylabels = {
            'C1': 'sdhss:C1',    # Social Quality of an Actor (Embodiment)
            'C3': 'sdhss:C3',    # Social Relationship
            'C4': 'sdhss:C4',    # Social Relationship Type
            'C5': 'sdhss:C5',    # Membership
            'C7': 'sdhss:C7',    # Occupation
            'C11': 'sdhss:C11',  # Gender
            'C12': 'sdhss:C12',  # Social Role
            'C13': 'sdhss:C13',  # Social Role Embodiment
            'C21': 'sdhss:C21',  # Skill
            'C23': 'sdhss:C23',  # Religious Identity
            'C24': 'sdhss:C24',  # Religion or Religious Denomination
            'C29': 'sdhss:C29',  # Know-How
            'E13': 'crm:E13_Attribute_Assignment',
            'E15': 'crm:E15_Identifier_Assignment',
            'E17': 'crm:E17_Type_Assignment',
            'E18': 'crm:E18_Physical_Thing',
            'E21': 'crm:E21_Person',
            'E22': 'crm:E22_Human-Made_Object',
            'E22B': 'spec:Boulloterion',
            'E22S': 'spec:Lead_Seal',
            'E31': 'crm:E31_Document',
            'E33': 'crm:E33_Linguistic_Object',
            'E34': 'crm:E34_Inscription',
            'E39': 'crm:E39_Actor',
            'E41': 'crm:E41_Appellation',
            'E42': 'crm:E42_Identifier',
            'E52': 'crm:E52_Time-Span',
            'E55': 'crm:E55_Type',
            'E56': 'crm:E56_Language',
            'E62': 'crm:E62_String',
            'E69': 'crm:E69_Death',
            'E73': 'crm:E73_Information_Object',
            'E73B': 'spec:Bibliography',
            'E74': 'crm:E74_Group',
            'E74A': 'spec:Author_Group',
            'E74E': 'spec:Ethnic_Group',
            'E78': 'crm:E78_Curated_Holding',
            'E87': 'crm:E87_Curation_Activity',
            'F1': 'lrmoo:F1_Work',    # Work
            'F2': 'lrmoo:F2_Expression',    # Expression - e.g. a database record
            'F3': 'lrmoo:F3_Manifestation',    # Publication - e.g. an edition or journal article
            'F11': 'lrmoo:F11_Corporate_Body',  # Corporate Body
            'F27': 'lrmoo:F27_Work_Creation',  # Work Creation
            'F28': 'lrmoo:F28_Expression_Creation',  # Expression Creation
        }

        self.predicates = {
            'P1': 'crm:P1_is_identified_by',
            'P2': 'crm:P2_has_type',
            'P3': 'crm:P3_has_note',
            'P4': 'crm:P4_has_time-span',
            'P14': 'crm:P14_carried_out_by',
            'P16': 'crm:P16_used_specific_object',
            'P17': 'crm:P17_was_motivated_by',
            'P37': 'crm:P37_assigned',
            'P41': 'crm:P41_classified',
            'P42': 'crm:P42_assigned',
            'P46': 'crm:P46_is_composed_of',
            'P48': 'crm:P48_has_preferred_identifier',
            'P51': 'crm:P51_has_former_or_current_owner',
            'P70': 'crm:P70_documents',
            'P80': 'crm:P80_end_is_qualified_by',
            'P94': 'crm:P94_has_created',
            'P100': 'crm:P100_was_death_of',
            'P102': 'crm:P102_has_title',
            'P107': 'crm:P107_has_current_or_former_member',
            'P108': 'crm:P108_has_produced',
            'P127': 'crm:P127_has_broader_term',
            'P128': 'crm:P128_carries',
            'P140': 'crm:P140_assigned_attribute_to',
            'P141': 'crm:P141_assigned',
            'P147': 'crm:P147_curated',
            'P148': 'crm:P148_has_component',
            'P165': 'crm:P165_incorporates',
            'P177': 'crm:P177_assigned_property_type',
            'P190': 'crm:P190_has_symbolic_content',
            'R3': 'lrmoo:R3_is_realised_in',     # is realised in
            'R5': 'lrmoo:R5_has_component',     # has component
            'R15': 'lrmoo:R15_has_fragment',   # has fragment
            'R16': 'lrmoo:R16_created',   # created [work]
            'R17': 'lrmoo:R17_created',   # created [expression]
            'R76': 'lrmoo:R76_is_derivative_of',   # is derivative of
            'SP13': 'sdhss:P13',  # pertains to [person, social quality]
            'SP14': 'sdhss:P14',  # has social quality
            'SP16': 'sdhss:P16',  # has relationship type
            'SP17': 'sdhss:P17',  # has relationship source
            'SP18': 'sdhss:P18',  # has relationship target
            'SP26': 'sdhss:P26',  # is embodiment by [person, social role]
            'SP33': 'sdhss:P33',  # is embodiment of [social role]
            'SP35': 'sdhss:P35',  # is defined by [person, religious identity]
            'SP36': 'sdhss:P36',  # pertains to [religious identity]
            'SP37': 'sdhss:P37',  # concerns [know-how]
            'SP38': 'sdhss:P38'   # has skill
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
        self.star_based = self.predicates['P17']
        self.star_auth = self.predicates['P14']
        self.star_source = self.predicates['P70']

        # Initialise our group agents and the data structures we need to start
        print("Setting up PBW constants...")
        # Ensure existence of our external authorities
        f11s = [{'key': 'pbw',
                 'title':Literal('Prosopography of the Byzantine World', 'en'),
                 'uri':Literal('https://pbw2016.kdl.kcl.ac.uk/', 'en')},
                {'key': 'viaf',
                 'title':Literal('Virtual Internet Authority File', 'en'),
                 'uri':Literal('https://viaf.org/', 'en')},
                {'key': 'orcid',
                 'title':Literal('OrcID', 'en'),
                 'uri': Literal('https://orcid.org/', 'en')}]
        for ent in f11s:
            f11_query = f"""
            ?a a lrmoo:F11_Corporate_Body ;
                crm:P3_has_note {ent['title'].n3()} ;
                crm:P1_is_identified_by {ent['uri'].n3()} ."""
            res = graph.query("SELECT DISTINCT ?a WHERE {" + f11_query + "}", initNs=self.namespaces)
            f11_uri = None
            if len(res):
                for row in res:  # this should iterate only once
                    f11_uri = row[0]
                    break
            else:
                # Create it.
                new_uris = self.mint_uris_for_query(f11_query)
                graph.update("INSERT DATA {" + f11_query + "}", initNs=self.namespaces, initBindings= new_uris)
                # TODO should we re-query to verify that it got there, or trust in exception handling?
                f11_uri = new_uris['a']
            # Store it in self.[key]_agent, e.g. self.pbw_agent
            self.__setattr__(f"{ent['key']}_agent", f11_uri)


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
        (nsstr, name) = fqname.split(':')
        code = name.split('_')[0]
        return f"star:E13_{nsstr}_{code}"

    # Accessors / creators for our controlled vocabularies
    def _find_or_create_cv_entry(self, category, nodeclass, label):
        # If we haven't made this label yet, do it
        if label not in self.cv[category]:
            # We have to create the node, possibly attaching it to a superclass
            dataprop = self.get_label('P1')
            litlabel = Literal(label, 'en')
            sparql = f"""
            ?cventry a {nodeclass} ;
                {dataprop} {litlabel.n3()} ."""
            res = self.graph.query("SELECT DISTINCT ?cventry WHERE {" + sparql + "}", initNs=self.namespaces)
            if len(res):
                for row in res:
                    self.cv[category][label] = row[0]
            else:
                new_uris = self.mint_uris_for_query(sparql)
                self.graph.update("INSERT DATA {" + sparql + "}", initNs=self.namespaces, initBindings = new_uris)
                self.cv[category][label] = new_uris['cventry']

        # Return the label we have
        return self.cv[category][label]

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

    def mint_uris_for_query(self, q):
        """Generate a URI for every variable in the given query string, and return the bindings."""
        minted = {}
        for m in re.finditer(r'\?(\w+)', q):
            var = m.group(1)
            if var not in minted:
                minted[var] = self.ns[str(uuid4())]
        return minted