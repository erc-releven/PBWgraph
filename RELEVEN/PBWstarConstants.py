import pbw
import re
import RELEVEN.PBWSources
from datetime import datetime
from os.path import join, dirname
from rdflib import Graph, URIRef, Literal, Namespace, OWL, RDF, RDFS, XSD
from rdflib.query import Result
from uuid import uuid4
from warnings import warn


# This package contains a bunch of information curated from the PBW website about authority, authorship
# and so forth. It is a huge laundry list of data and some initialiser and accessor functions for it; the
# class requires an RDFlib graph or remote datastore in order to do the initialisation steps.


class PBWstarConstants:
    """A class to deal with all of our constants, where the data is nicely encapsulated"""

    def __init__(self, graph=None, store=None, execution=None, readonly=False):
        self.sourcelist = RELEVEN.PBWSources.PBWSources(join(dirname(__file__), 'pbw_sources.csv'))

        # These are the modern scholars who put the source information into PBW records.
        # We need Michael, Charlotte, and Tara on the outside
        self.mj = self.sourcelist.authorities['mj']
        self.ta = self.sourcelist.authorities['ta']
        self.cr = self.sourcelist.authorities['cr']

        datauri = 'https://r11.eu/rdf/resource/'
        self.ns = Namespace(datauri)
        self.namespaces = {
            'crm':   Namespace('http://www.cidoc-crm.org/cidoc-crm/'),
            'crmdig': Namespace('http://www.ics.forth.gr/isl/CRMdig/'),
            'crminf': Namespace('http://www.cidoc-crm.org/extensions/crminf/'),
            'lrmoo': Namespace('http://iflastandards.info/ns/lrm/lrmoo/'),
            'pbw':   Namespace('https://pbw2016.kdl.kcl.ac.uk/'),
            'sdhss': Namespace('https://r11.eu/ns/prosopography/'),
            'so': Namespace('https://r11.eu/ns/similarity/'),
            'spec':  Namespace('https://r11.eu/ns/spec/'),
            'star':  Namespace('https://r11.eu/ns/star/'),
            'data':  self.ns
        }

        graph_exists = False
        # If store and graph are defined, connect to the remote store using the given graph
        # If only store is defined, connect to the remote store using the graph named with our base URI
        # If only graph is defined, assume it is a local file and open it
        if store is not None:
            rgraph = graph or datauri
            self.graph = Graph(store, identifier=rgraph)
            graph_exists = True
        elif graph is not None:
            self.graph = graph
            graph_exists = True
        self.readonly = readonly

        if graph_exists:
            # Bind the namespaces in our graph
            for k, v in self.namespaces.items():
                self.graph.bind(k, v, override=True)
        else:
            warn("No graph or remote SPARQL store specified - initialising static constants only")

        # The classes we are using, keyed by their short forms.
        self.entitylabels = {
            'C1': self.namespaces['sdhss']['C1'],    # Social Quality of an Actor (Embodiment)
            'C2': self.namespaces['sdhss']['C2'],    # Actor's Social Quality (Type)
            'C3': self.namespaces['sdhss']['C3'],    # Social Relationship
            'C4': self.namespaces['sdhss']['C4'],    # Social Relationship Type
            'C5': self.namespaces['sdhss']['C5'],    # Membership
            'C7': self.namespaces['sdhss']['C7'],    # Occupation
            'C11': self.namespaces['sdhss']['C11'],  # Gender
            'C12': self.namespaces['sdhss']['C12'],  # Social Role
            'C13': self.namespaces['sdhss']['C13'],  # Social Role Embodiment
            'C21': self.namespaces['sdhss']['C21'],  # Skill
            'C23': self.namespaces['sdhss']['C23'],  # Religious Identity
            'C24': self.namespaces['sdhss']['C24'],  # Religion or Religious Denomination
            'C29': self.namespaces['sdhss']['C29'],  # Know-How
            'D10': self.namespaces['crmdig']['D10_Software_Execution'],
            'D14': self.namespaces['crmdig']['D14_Software'],
            'E5': self.namespaces['crm']['E5_Event'],
            'E13': self.namespaces['crm']['E13_Attribute_Assignment'],
            'E15': self.namespaces['crm']['E15_Identifier_Assignment'],
            'E17': self.namespaces['crm']['E17_Type_Assignment'],
            'E17G': self.namespaces['spec']['Gender_Assignment'],
            'E18': self.namespaces['crm']['E18_Physical_Thing'],
            'E21': self.namespaces['crm']['E21_Person'],
            'E22': self.namespaces['crm']['E22_Human-Made_Object'],
            'E22B': self.namespaces['spec']['Boulloterion'],
            'E22S': self.namespaces['spec']['Lead_Seal'],
            'E27': self.namespaces['crm']['E27_Site'],
            'E31': self.namespaces['crm']['E31_Document'],
            'E33': self.namespaces['crm']['E33_Linguistic_Object'],
            'E33A': self.namespaces['crm']['E33_E41_Linguistic_Appellation'],
            'E34': self.namespaces['crm']['E34_Inscription'],
            'E39': self.namespaces['crm']['E39_Actor'],
            'E41': self.namespaces['crm']['E41_Appellation'],
            'E42': self.namespaces['crm']['E42_Identifier'],
            'E52': self.namespaces['crm']['E52_Time-Span'],
            'E55': self.namespaces['crm']['E55_Type'],
            'E56': self.namespaces['crm']['E56_Language'],
            'E62': self.namespaces['crm']['E62_String'],
            'E69': self.namespaces['crm']['E69_Death'],
            'E73': self.namespaces['crm']['E73_Information_Object'],
            'E73B': self.namespaces['spec']['Bibliography'],
            'E74': self.namespaces['crm']['E74_Group'],
            'E74A': self.namespaces['spec']['Author_Group'],
            'E74E': self.namespaces['spec']['Ethnic_Group'],
            'E78': self.namespaces['crm']['E78_Curated_Holding'],
            'E87': self.namespaces['crm']['E87_Curation_Activity'],
            'F1': self.namespaces['lrmoo']['F1_Work'],    # Work
            'F2': self.namespaces['lrmoo']['F2_Expression'],    # Expression - e.g. a database record
            'F2T': self.namespaces['spec']['Text_Expression'],  # Text Expression - e.g.
            'F3': self.namespaces['lrmoo']['F3_Manifestation'],    # Manifestation
            'F2P': self.namespaces['spec']['Publication'],  # Publication - e.g. an edition or journal article
            'F11': self.namespaces['lrmoo']['F11_Corporate_Body'],  # Corporate Body
            'F27': self.namespaces['lrmoo']['F27_Work_Creation'],  # Work Creation
            'F28': self.namespaces['lrmoo']['F28_Expression_Creation'],  # Expression Creation
            'F30': self.namespaces['lrmoo']['F30_Manifestation_Creation'],
            'I2': self.namespaces['crminf']['I2_Belief'],
            'I3': self.namespaces['crminf']['I3_Inference_Logic'],
            'I4': self.namespaces['crminf']['I4_Proposition_Set'],
            'I5': self.namespaces['crminf']['I5_Inference_Making'],
            'I13': self.namespaces['crminf']['I13_Intended_Meaning_Belief'],
            'I16': self.namespaces['crminf']['I16_Meaning_Comprehension'],
            'S5': self.namespaces['star']['S5_Suggestion']
        }

        # The properties we are using, keyed by their short forms.
        self.predicates = {
            'P1': self.namespaces['crm']['P1_is_identified_by'],
            'P2': self.namespaces['crm']['P2_has_type'],
            'P3': self.namespaces['crm']['P3_has_note'],
            'P4': self.namespaces['crm']['P4_has_time-span'],
            'P7': self.namespaces['crm']['P7_took_place_at'],
            'P11': self.namespaces['crm']['P11_had_participant'],
            'P14': self.namespaces['crm']['P14_carried_out_by'],
            'P16': self.namespaces['crm']['P16_used_specific_object'],
            'P17': self.namespaces['crm']['P17_was_motivated_by'],
            'P37': self.namespaces['crm']['P37_assigned'],
            'P41': self.namespaces['crm']['P41_classified'],
            'P42': self.namespaces['crm']['P42_assigned'],
            'P46': self.namespaces['crm']['P46_is_composed_of'],
            'P48': self.namespaces['crm']['P48_has_preferred_identifier'],
            'P51': self.namespaces['crm']['P51_has_former_or_current_owner'],
            'P67': self.namespaces['crm']['P67_refers_to'],
            'P70': self.namespaces['crm']['P70_documents'],
            'P80': self.namespaces['crm']['P80_end_is_qualified_by'],
            'P82a': self.namespaces['crm']['P82a_begin_of_the_begin'],
            'P82b': self.namespaces['crm']['P82b_end_of_the_end'],
            'P94': self.namespaces['crm']['P94_has_created'],
            'P100': self.namespaces['crm']['P100_was_death_of'],
            'P102': self.namespaces['crm']['P102_has_title'],
            'P107': self.namespaces['crm']['P107_has_current_or_former_member'],
            'P108': self.namespaces['crm']['P108_has_produced'],
            'P127': self.namespaces['crm']['P127_has_broader_term'],
            'P128': self.namespaces['crm']['P128_carries'],
            'P140': self.namespaces['crm']['P140_assigned_attribute_to'],
            'P141': self.namespaces['crm']['P141_assigned'],
            'P147': self.namespaces['crm']['P147_curated'],
            'P148': self.namespaces['crm']['P148_has_component'],
            'P165': self.namespaces['crm']['P165_incorporates'],
            'P177': self.namespaces['crm']['P177_assigned_property_type'],
            'P190': self.namespaces['crm']['P190_has_symbolic_content'],
            'J1': self.namespaces['crminf']['J1_used_as_premise'],
            'J2': self.namespaces['crminf']['J2_concluded_that'],
            'J3': self.namespaces['crminf']['J3_applies'],
            'J4': self.namespaces['crminf']['J4_that'],
            'J5': self.namespaces['crminf']['J5_holds_to_be'],
            'J23': self.namespaces['crminf']['J23_interpreted_meaning_as'],
            'J28': self.namespaces['crminf']['J28_contains_entity_reference'],
            'L1': self.namespaces['spec']['L1_was_used_to_produce'],
            'L11': self.namespaces['crmdig']['L11_had_output'],
            'L11r': self.namespaces['crmdig']['L11r_was_output_of'],
            'L12': self.namespaces['crmdig']['L12_happened_on_device'],
            'L23': self.namespaces['crmdig']['L23_used_software_or_firmware'],
            'R3': self.namespaces['lrmoo']['R3_is_realised_in'],     # is realised in
            'R5': self.namespaces['lrmoo']['R5_has_component'],     # has component
            'R15': self.namespaces['lrmoo']['R15_has_fragment'],   # has fragment
            'R16': self.namespaces['lrmoo']['R16_created'],   # created [work]
            'R17': self.namespaces['lrmoo']['R17_created'],   # created [expression]
            'R24': self.namespaces['lrmoo']['R24_created'],   # created [manifestation]
            'R76': self.namespaces['lrmoo']['R76_is_derivative_of'],   # is derivative of
            'SP13': self.namespaces['sdhss']['P13'],  # pertains to [person, social quality]
            'SP14': self.namespaces['sdhss']['P14'],  # has social quality
            'SP16': self.namespaces['sdhss']['P16'],  # has relationship type
            'SP17': self.namespaces['sdhss']['P17'],  # has relationship source
            'SP18': self.namespaces['sdhss']['P18'],  # has relationship target
            'SP26': self.namespaces['sdhss']['P26'],  # is embodiment by [person, social role]
            'SP33': self.namespaces['sdhss']['P33'],  # is embodiment of [social role]
            'SP35': self.namespaces['sdhss']['P35'],  # is defined by [person, religious identity]
            'SP36': self.namespaces['sdhss']['P36'],  # pertains to [religious identity]
            'SP37': self.namespaces['sdhss']['P37'],  # concerns [know-how]
            'SP38': self.namespaces['sdhss']['P38'],  # has skill
            'ID5': self.namespaces['so']['ID5_related'],    # symmetric
            'ID7': self.namespaces['so']['ID7_matches'],    # symmetric and transitive
            'ID8': self.namespaces['so']['ID8_identical']   # symmetric, reflexive, and transitive
        }

        # The floruit strings for the people we want to include
        self.eleventh_century = {
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


        # Initialise our group agents and the data structures we need to start
        if graph_exists:
            # Define our STAR model predicates
            self.star_subject = self.get_label('P140')
            self.star_object = self.get_label('P141')
            self.star_based = self.get_label('P17')
            self.star_auth = self.get_label('P14')
            self.star_src = self.get_label('P67')
            self.entity_label = RDFS.label
            self.entity_link = OWL.sameAs
            # convenience
            self.label_n3 = self.entity_label.n3(self.graph.namespace_manager)
            self.link_n3 = self.entity_link.n3(self.graph.namespace_manager)
            if not self.readonly:
                try:
                    print("Setting up software execution run...")
                    # Ensure the existence of the software metadata
                    # TODO should this be a string?
                    ourscript = Literal("https://github.com/erc-releven/PBWgraph/RELEVEN/graphimportSTAR.py")
                    md_query = f"""
                    ?thisurl a {self.get_label('E42')} ;
                        {self.get_label('P190')} {ourscript.n3()} .
                    ?this a {self.get_label('D14')} ;
                        {self.get_label('P1')} ?thisurl ."""
                    res = self.ensure_entities_existence(md_query)
                    # Create the software execution for this run, so that we can create the markers at the end
                    if execution is not None:
                        # If we are resuming a run, we use the same software execution entity
                        self.swrun = URIRef(execution) if execution.startswith('http') \
                            else self.ns[execution.lstrip('data:')]
                    else:
                        # If we are not resuming, we have to create the entity with the current timestamp,
                        # assuming we have a writable store.
                        self.swrun = self.namespaces['data'][str(uuid4())]
                        se_query = f"""
                        ?tstamp a {self.get_label('E52')} ;
                            {self.get_label('P82a')} {Literal(datetime.now(), datatype=XSD.dateTimeStamp).n3()} .
                        {self.swrun.n3()} a {self.get_label('D10')} ;
                            {self.get_label('P4')} ?tstamp ;
                            {self.get_label('L23')} {res['this'].n3()} ."""
                        self.ensure_entities_existence(se_query)
                except TypeError:
                    print("Graph is not writable! Continuing in read-only mode")
                    self.readonly = True

            print("Setting up PBW constants...")
            # Ensure existence of our external authorities
            self.pbw_agent = None
            self.viaf_agent = None
            self.orcid_agent = None
            self.r11_agent = None
            f11s = [{'key': 'pbw',
                     'title': Literal('Prosopography of the Byzantine World', 'en'),
                     'uri': URIRef('https://pbw2016.kdl.kcl.ac.uk/')},
                    {'key': 'viaf',
                     'title': Literal('Virtual Internet Authority File', 'en'),
                     'uri': URIRef('https://viaf.org/')},
                    {'key': 'orcid',
                     'title': Literal('ORCID', 'en'),
                     'uri': URIRef('https://orcid.org/')},
                    {'key': 'r11',
                     'title': Literal('RELEVEN project', 'en'),
                     'uri': URIRef('https://r11.eu/')}]
            for ent in f11s:
                f11_query = f"""
                ?a a {self.get_label('F11')} ;
                    {self.label_n3} {ent['title'].n3()} ;
                    {self.link_n3} {ent['uri'].n3()} ."""
                uris = self.ensure_entities_existence(f11_query)
                f11_uri = uris['a']
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
        # Special-case 'slave' and ordained/consecrated roles out of 'occupations'. TODO add Nun, rethink feminine titles
        self.legal_designations = ['Bishops', 'Cantor', 'Captain', 'Chamberlain', 'Hieromonk', 'Imperial courier',
                                   'Judge', 'Missionary priest', 'Monk', 'Monk (Latin)', 'Patriarch',
                                   'Presbyter', 'Priest', 'Slave', 'Tax-collector']

        # Special-case certain "dignities" into generic social roles, cf.
        # https://github.com/erc-releven/.github/issues/16
        self.generic_social_roles = ['Agent', 'Anadochos', 'Asekretissa', 'Asketes', 'Basileus (rebel)', 'Depotatos',
                                     'Despotatos', 'Diasemotatos', 'Doulos of the emperor', 'Doulos of the sebastos',
                                     'Droungaria', 'Ex-droungarios of the fleet', 'Ex-emperor',
                                     'Gambros of the emperor', 'General (?) of the Antiochenes', 'Herald',
                                     'Hyperperilampros', 'Hypersebastos', 'Imperial doctor',
                                     'Interpreter of the English', 'Interpreter of the Bulgarians',
                                     'Interpreter of the Romans', 'Interpreter of the droungarios', 'Kanikline',
                                     'Katepanissa', 'Kouropalatissa', 'Kritaina', 'Lady', 'Lawyer', 'Magistrissa',
                                     'Man', 'Megal(o)epiphanestatos', 'Mystographissa', 'Oikeios of the emperor',
                                     'Oikodespotes', 'Panentimos pantimos', 'Panhyperlampros', 'Panhypersebaste',
                                     'Panhypersebastos', 'Panhypertimos', 'Pansebastohypertatos', 'Pansebastos sebaste',
                                     'Pantimos', 'Patrikia', 'Phrourarchos', 'Phylax', 'Porphyrogennetos', 'Proedrissa',
                                     'Protokouropalatissa', 'Protoproedrissa', 'Protospatharea', 'Protospatharissa',
                                     'Protovestarchissa', 'Protovestiaria', 'Protovestiarissa',
                                     'Relative of the megas domestikos', 'Scholarios', 'Scribe', 'Sebastokratorissa',
                                     'Servant of the patriarch', 'Ship\'s captain', 'Son of the archon', 'Strategissa',
                                     'Strateutes', 'Stratiotes', 'Topoteretissa', 'Vestarchissa', 'Vestena']

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
        """Return the namespaced entity (class) or predicate string given the short name.
        We want this to throw an exception if nothing is found."""
        try:
            return self.entitylabels[lbl].n3(self.graph.namespace_manager)
        except KeyError:
            return self.predicates[lbl].n3(self.graph.namespace_manager)

    def pbw_uri(self, resource):
        if type(resource) == pbw.Factoid:
            return self.namespaces['pbw'][f"factoid/{resource.factoidKey}"]
        elif type(resource) == pbw.Boulloterion:
            return self.namespaces['pbw'][f"boulloterion/{resource.boulloterionKey}"]
        else:
            raise ValueError(f"PBW URI requested for something that is neither factoid nor boulloterion: {resource}")

    def get_assertion_for_predicate(self, p):
        """Takes a predicate key and returns the qualified assertion class string which implies that predicate.
        This will throw an exception if no predicate is defined for the key."""
        nsstr, code = self._split_fqname(p)
        return f"star:E13_{nsstr}_{code}"

    def get_starpreds_for_predicate(self, p):
        """Takes a predicate key and returns the subject and object predicates that go along with its bespoke
        STAR assertion. This will throw an exception if no predicate is defined for the key."""
        nsstr, code = self._split_fqname(p)
        return f"star:P140_{nsstr}_{code}", f"star:P141_{nsstr}_{code}"

    def _split_fqname(self, p):
        fqname = self.predicates[p].n3(self.graph.namespace_manager)
        (nsstr, name) = fqname.split(':')
        code = name.split('_')[0]
        return nsstr, code

    # Accessors / creators for our controlled vocabularies
    def _find_or_create_cv_entry(self, category, nodeclass, label):
        # If we haven't made this label yet, do it
        if label not in self.cv[category]:
            # We have to create the node, possibly attaching it to a superclass
            litlabel = Literal(label, lang='en')
            sparql = f"""
            ?cventry a {nodeclass} ;
                {self.label_n3} {litlabel.n3()} ."""
            res = self.ensure_entities_existence(sparql)
            self.cv[category][label] = res['cventry']

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
        srclass = self.get_label('C2')
        if srlabel in self.legal_designations:
            srclass = self.get_label('C12')
        return self._find_or_create_cv_entry('SocietyRole', srclass, srlabel), srclass

    def get_dignity(self, dignity):
        # Dignities in PBW tend to be specific to institutions / areas;
        # make an initial selection by breaking on the 'of'
        diglabel = dignity
        digclass = self.get_label('C12')
        if ' of the ' not in dignity:  # Don't split (yet) titles that probably don't refer to places
            diglabel = dignity.split(' of ')[0]
        if diglabel in self.generic_social_roles:
            digclass = self.get_label('C2')
        dig_uri = self._find_or_create_cv_entry('Dignity', digclass, diglabel)
        # Make sure that the URI also appears under the original label, if we shortened it
        self.cv['Dignity'][dignity] = dig_uri
        return dig_uri, digclass

    def inrange(self, floruit):
        """Return true if the given floruit tag falls within RELEVEN's range"""
        return floruit in self.eleventh_century

    def mint_uris_for_query(self, q):
        """Generate a URI for every variable in the given query string, and return the bindings."""
        minted = {}
        for m in re.finditer(r'\?(\w+)', q):
            var = m.group(1)
            if var not in minted:
                minted[var] = self.ns[str(uuid4())]
        return minted

    def ensure_entities_existence(self, sparql, force_create=False):
        # print("SPARQL is:" + sparql)
        if force_create and self.readonly:
            raise Exception("Cannot force create triples in readonly mode!")
        try:
            if not force_create:
                res = self.graph.query("SELECT DISTINCT * WHERE {" + sparql + "}")
                if len(res):
                    # Did we actually get a result?
                    if not isinstance(res, Result):
                        raise RuntimeError(f"Got unexpected result on query: {res}\nSPARQL was: {sparql}")
                    # We should hopefully have only one row...
                    if len(res) > 1:
                        warn(f"More than one row returned for SPARQL expression:\n{sparql}")
                    # In any case return the variables from the first row as a dictionary.
                    for row in res:
                        return row.asdict()

            if self.readonly:
                # If we got here we didn't get a result, and we can't add one.
                return dict()

            # Either force_create was specified or res had zero length.
            new_uris = self.mint_uris_for_query(sparql)
            q = sparql
            for k, v in new_uris.items():
                q = q.replace(f'?{k}', v.n3(self.graph.namespace_manager))
            self.graph.update("INSERT DATA {" + q + "}")
            return new_uris
        except Exception as e:
            print(f"EXCEPTION {e}; SPARQL was {sparql}")
            raise e

    def ensure_egroup_existence(self, gclass, glink, members, title=None):
        # Get the URI list
        mvalues = ', '.join([x.n3() for x in members])
        # Get the group label, which is a semicolon-separated list of member labels
        if title is None:
            mnames = []
            for m in members:
                mname = self.graph.value(m, self.entity_label)
                if mname is None:
                    warn(f"Group member {m} has no label?!")
                    mnames.append('XX ANON')
                else:
                    mnames.append(str(mname))
            mlabel = Literal('; '.join(mnames)).n3()
        else:
            mlabel = Literal(title).n3()

        # Look to see if a group with exactly these members exists
        sparql = f"""
SELECT ?egroup WHERE {{
    {{
        # Filter first to egroups that have our particular members
        SELECT DISTINCT ?egroup WHERE {{
            ?egroup a {self.get_label(gclass)} ;
                {self.label_n3} {mlabel} ;
                {self.get_label(glink)} {mvalues} .
        }}
    }}
    #  Now make sure the group doesn't have any further members.
    ?egroup {self.get_label(glink)} ?member .
}}
GROUP BY ?egroup HAVING (COUNT(?member) = {len(members)})
"""
        rows = [x for x in self.graph.query(sparql)]
        if len(rows) == 0:
            # We need to create the group and its members
            mlist = ', '.join([x.n3() for x in members])
            # Construct the query
            sparql = f"""
        ?egroup {self.get_label(glink)} {mlist} ;
            {self.label_n3} {mlabel} ;
            a {self.get_label(gclass)} .
            """
            answer = self.ensure_entities_existence(sparql, force_create=True)
        else:
            # Make sure there is only one egroup that fits this spec
            if len(rows) > 1:
                warn(f"Multiple entity groups found with exactly the given members {mlabel}!")
            answer = rows[0]

        # Either way, return the entity group
        return answer.get('egroup')

    def document(self, pbwpage, *assertions):
        """Make the E31 link between the pbwpage and whatever assertions we just pulled from it, and
        mark these assertions as having been made by this software run. Return the assertions that
        were documented."""
        # Since we don't have to mint any new URIs in this query, we can just add them normally.
        if pbwpage is not None:
            self.graph.add((pbwpage, RDF.type, self.entitylabels['E31']))
        for a in assertions:
            if pbwpage is not None:
                self.graph.add((pbwpage, self.predicates['P70'], a))
            self.graph.add((a, self.predicates['L11r'], self.swrun))
        return assertions
