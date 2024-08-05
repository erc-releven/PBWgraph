from rdflib import Graph, URIRef, Literal, Namespace, RDF, SKOS
from warnings import warn
import re
import RELEVEN.PBWSources
from os.path import join, dirname
from uuid import uuid4

import pbw


# This package contains a bunch of information curated from the PBW website about authority, authorship
# and so forth. It is a huge laundry list of data and some initialiser and accessor functions for it; the
# class requires a graph driver in order to do the initialisation.


class PBWstarConstants:
    """A class to deal with all of our constants, where the data is nicely encapsulated"""

    def __init__(self, graph=None, store=None):
        # These are the modern scholars who put the source information into PBW records.
        # We need Michael and Tara on the outside
        self.mj = {'identifier': 'Jeffreys, Michael J.', 'viaf': '73866641'}
        self.ta = {'identifier': 'Andrews, Tara Lee', 'viaf': '316505144'}

        self.sourcelist = RELEVEN.PBWSources.PBWSources(join(dirname(__file__), 'pbw_sources.csv'))

        datauri = 'https://r11.eu/rdf/resource/'
        self.ns = Namespace(datauri)
        self.namespaces = {
            'crm':   Namespace('http://www.cidoc-crm.org/cidoc-crm/'),
            'crmdig': Namespace('http://www.ics.forth.gr/isl/CRMdig/'),
            'lrmoo': Namespace('http://iflastandards.info/ns/lrm/lrmoo/'),
            'pbw':   Namespace('https://pbw2016.kdl.kcl.ac.uk/'),
            'sdhss': Namespace('https://r11.eu/ns/prosopography/'),
            'spec':  Namespace('https://r11.eu/ns/spec/'),
            'star':  Namespace('https://r11.eu/ns/star/'),
            'data':  self.ns
        }

        graph_exists = False
        if graph is not None:
            self.graph = graph
            graph_exists = True
        elif store is not None:
            self.graph = Graph(store, identifier=datauri)
            graph_exists = True

        if graph_exists:
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
            'E13': self.namespaces['crm']['E13_Attribute_Assignment'],
            'E15': self.namespaces['crm']['E15_Identifier_Assignment'],
            'E17': self.namespaces['crm']['E17_Type_Assignment'],
            'E18': self.namespaces['crm']['E18_Physical_Thing'],
            'E21': self.namespaces['crm']['E21_Person'],
            'E22': self.namespaces['crm']['E22_Human-Made_Object'],
            'E22B': self.namespaces['spec']['Boulloterion'],
            'E22S': self.namespaces['spec']['Lead_Seal'],
            'E31': self.namespaces['crm']['E31_Document'],
            'E33': self.namespaces['crm']['E33_Linguistic_Object'],
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
            'F3P': self.namespaces['spec']['Publication'],  # Publication - e.g. an edition or journal article
            'F11': self.namespaces['lrmoo']['F11_Corporate_Body'],  # Corporate Body
            'F27': self.namespaces['lrmoo']['F27_Work_Creation'],  # Work Creation
            'F28': self.namespaces['lrmoo']['F28_Expression_Creation'],  # Expression Creation
            'F30': self.namespaces['lrmoo']['F30_Manifestation_Creation']
        }

        # The properties we are using, keyed by their short forms.
        self.predicates = {
            'P1': self.namespaces['crm']['P1_is_identified_by'],
            'P2': self.namespaces['crm']['P2_has_type'],
            'P3': self.namespaces['crm']['P3_has_note'],
            'P4': self.namespaces['crm']['P4_has_time-span'],
            'P14': self.namespaces['crm']['P14_carried_out_by'],
            'P16': self.namespaces['crm']['P16_used_specific_object'],
            'P17': self.namespaces['crm']['P17_was_motivated_by'],
            'P37': self.namespaces['crm']['P37_assigned'],
            'P41': self.namespaces['crm']['P41_classified'],
            'P42': self.namespaces['crm']['P42_assigned'],
            'P46': self.namespaces['crm']['P46_is_composed_of'],
            'P48': self.namespaces['crm']['P48_has_preferred_identifier'],
            'P51': self.namespaces['crm']['P51_has_former_or_current_owner'],
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
            'L1': self.namespaces['spec']['L1_was_used_to_produce'],
            'L11': self.namespaces['crmdig']['L11_had_output'],
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
            'SP38': self.namespaces['sdhss']['P38']   # has skill
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

        # Define our STAR model predicates
        self.star_subject = self.get_label('P140')
        self.star_object = self.get_label('P141')
        self.star_based = self.get_label('P17')
        self.star_auth = self.get_label('P14')
        # self.star_src = self.get_label('P70')

        # Initialise our group agents and the data structures we need to start
        if graph_exists:
            print("Setting up PBW constants...")
            # Ensure existence of our external authorities
            self.pbw_agent = None
            self.viaf_agent = None
            self.orcid_agent = None
            f11s = [{'key': 'pbw',
                     'title': Literal('Prosopography of the Byzantine World'),
                     'uri': URIRef('https://pbw2016.kdl.kcl.ac.uk/')},
                    {'key': 'viaf',
                     'title': Literal('Virtual Internet Authority File'),
                     'uri': URIRef('https://viaf.org/')},
                    {'key': 'orcid',
                     'title': Literal('ORCID', 'en'),
                     'uri': URIRef('https://orcid.org/')}]
            for ent in f11s:
                f11_query = f"""
                ?a a {self.get_label('F11')} ;
                    {self.get_label('P1')} {ent['title'].n3()} ;
                    {SKOS.exactMatch.n3()} {ent['uri'].n3()} ."""
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
        # Special-case 'slave' and ordained/consecrated roles out of 'occupations'
        self.legal_designations = ['Slave', 'Monk', 'Bishops', 'Monk (Latin)', 'Patriarch', 'Hieromonk']

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
                                     'Strateutes', 'Stratiotes', 'Topoteretissa', 'Vestarchissa', 'Vestena',
                                     'Xenodochos']

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
        fqname = self.predicates[p].n3(self.graph.namespace_manager)
        (nsstr, name) = fqname.split(':')
        code = name.split('_')[0]
        return f"star:E13_{nsstr}_{code}"

    # Accessors / creators for our controlled vocabularies
    def _find_or_create_cv_entry(self, category, nodeclass, label):
        # If we haven't made this label yet, do it
        if label not in self.cv[category]:
            # We have to create the node, possibly attaching it to a superclass
            dataprop = self.get_label('P1')
            # TODO attach language to these later
            litlabel = Literal(label)
            sparql = f"""
            ?cventry a {nodeclass} ;
                {dataprop} {litlabel.n3()} ."""
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
        if srlabel in self.legal_designations:
            return self._find_or_create_cv_entry('SocietyRole', self.get_label('C12'), srlabel)
        else:
            return self._find_or_create_cv_entry('SocietyRole', self.get_label('C2'), srlabel)

    def get_dignity(self, dignity):
        # Dignities in PBW tend to be specific to institutions / areas;
        # make an initial selection by breaking on the 'of'
        diglabel = dignity
        if ' of the ' not in dignity:  # Don't split (yet) titles that probably don't refer to places
            diglabel = dignity.split(' of ')[0]
        if diglabel in self.generic_social_roles:
            dig_uri = self._find_or_create_cv_entry('Dignity', self.get_label('C2'), diglabel)
        else:
            dig_uri = self._find_or_create_cv_entry('Dignity', self.get_label('C12'), diglabel)
        # Make sure that the URI also appears under the original label, if we shortened it
        self.cv['Dignity'][dignity] = dig_uri
        return dig_uri

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
        try:
            if not force_create:
                res = self.graph.query("SELECT DISTINCT * WHERE {" + sparql + "}")
                if len(res):
                    # We should hopefully have only one row...
                    if len(res) > 1:
                        warn(f"More than one row returned for SPARQL expression:\n{sparql}")
                    # In any case return the variables from the first row as a dictionary.
                    for row in res:
                        return row.asdict()

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

    def ensure_egroup_existence(self, gclass, glink, members):
        # Get the URI list
        mvalues = '\n'.join([f"({x.n3()})" for x in members])
        # Look to see if a group with exactly these members exists
        sparql = f"""
SELECT ?egroup WHERE {{
    VALUES (?member) {{
{mvalues}
    }}
    {{
        SELECT ?egroup WHERE {{
            ?egroup a {self.get_label(gclass)} ;
                {self.get_label(glink)} ?item .
        }}
        GROUP BY ?egroup HAVING (COUNT(?item) = {len(members)})
    }}
    ?egroup {self.get_label(glink)} ?member .
}}
GROUP BY ?egroup
HAVING (COUNT(?member) = {len(members)})
"""
        rows = [x for x in self.graph.query(sparql)]
        if len(rows) == 0:
            # We need to create the group and its members
            mlist = ', '.join([x.n3() for x in members])
            # Get the group label, which is a semicolon-separated list of member labels
            mnames = []
            for m in members:
                mname = self.graph.value(m, self.namespaces['crm'].P3_has_note)
                if mname is None:
                    warn(f"Group member {m} has no label?!")
                    mnames.append('XX ANON')
                else:
                    mnames.append(str(mname))
            mlabel = Literal('; '.join(mnames)).n3()
            # Construct the query
            sparql = f"""
        ?egroup a {self.get_label(gclass)} ;
            {self.get_label('P3')} {mlabel} ;
            {self.get_label(glink)} {mlist} .
            """
            answer = self.ensure_entities_existence(sparql, force_create=True)
        else:
            # Make sure there is only one egroup that fits this spec
            if len(rows) > 1:
                warn("Multiple entity groups found with exactly the given members!")
            answer = rows[0]

        # Either way, return the entity group
        return answer.get('egroup')

    def document(self, pbwpage, *assertions):
        """Make the E31 link between the pbwpage and whatever assertions we just pulled from it."""
        # Since we don't have to mint any new URIs in this query, we can just add them normally.
        self.graph.add((pbwpage, RDF.type, self.entitylabels['E31']))
        for a in assertions:
            self.graph.add((pbwpage, self.predicates['P70'], a))
