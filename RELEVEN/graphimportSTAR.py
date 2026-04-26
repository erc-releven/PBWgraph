import argparse
import sys
import traceback

import pbw
import RELEVEN.PBWstarConstants
import RELEVEN.author_viewpoints
import config
import re
from datetime import datetime
from functools import reduce
from http.client import RemoteDisconnected
from rdflib import Graph, Literal, URIRef
from rdflib.plugins.stores import sparqlstore
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from time import sleep
from urllib.error import URLError, HTTPError
from warnings import warn


def escape_text(t):
    """Escape any single quotes or double quotes in strings that need to go into Neo4J properties"""
    return t.replace("'", "\\'").replace('"', '\\"')


def dedupe(lst):
    """Remove duplicates from a list, keeping the order"""
    r = []
    for i in lst:
        if i not in r:
            r.append(i)
    return r


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
        return 'maka' # "maybe also known as"
    return label


def _matchid(var, val):
    return 'MATCH (%s) WHERE %s.uuid = "%s" ' % (var, var, val)


def _get_single_key(rdfresult, k):
    if len(rdfresult) > 1:
        warn(f"Query result had multiple rows!")
    for row in rdfresult:
        return row[k]
    return None


def _get_source_lang(dbobj):
    """Returns the RDF language tag for any object that has an `oLangKey` property"""
    lkeys = {2: 'grc', 3: 'la', 4: 'ar', 5: 'xcl'}
    try:
        return lkeys.get(int(dbobj.oLangKey))
    except (ValueError, NameError):
        return None


def re_encode(s):
    # To be used for fields where the Unicode encoding is broken in that special MySQL way
    # cf. https://www.digitalbyzantinist.org/2014/06/17/the_mystery_of_the_character_encoding/
    newchrs = bytearray()
    for cc in s:
        if ord(cc) > 256:
            newchrs.append(ord(cc.encode('cp1252')))
        else:
            newchrs.append(ord(cc))
    return newchrs.decode('utf-8')


class graphimportSTAR:
    constants = None
    mysqlsession = None

    def __init__(self, origgraph, testmode=False, execution=None):
        # Set the testing flag
        self.testmode = testmode
        # Record the starting time
        self.starttime = datetime.now()
        # Connect to the SQL DB
        engine = create_engine('mysql+mysqlconnector://' + config.dbstring)
        smaker = sessionmaker(bind=engine)
        self.mysqlsession = smaker()
        # Are we connecting to the remote service?
        if origgraph == config.graphuri:
            # Make the connection and let the constants module instatiate the graph with all the namespaces
            store = sparqlstore.SPARQLUpdateStore(origgraph, origgraph + '/statements', method='POST',
                                                  auth=(config.graphuser, config.graphpw))
            # Make / retrieve the global nodes and self.constants
            self.constants = RELEVEN.PBWstarConstants.PBWstarConstants(store=store, execution=execution)
            self.g = self.constants.graph
            loaded = True
        else:
            # Start an RDF graph, parsing what we started with
            self.g = Graph()
            loaded = False
            try:
                self.g.parse(origgraph)
                loaded = True
            except FileNotFoundError:
                pass
            # Make / retrieve the global nodes and self.constants
            self.constants = RELEVEN.PBWstarConstants.PBWstarConstants(graph=self.g, execution=execution)

        # How many assertions do we have to start with?
        if loaded:
            res = self.g.triples((None, self.constants.predicates['P140'], None))
            ct = reduce(lambda x, y: x + 1, res, 0)
            print(f"Using graph {origgraph} with {ct} existing assertions.")

        # Keep lookup tables of our persistent entities, which hopefully improves performance
        self.resolved_authorities = dict()
        self.resolved_persons = dict()
        self.resolved_locations = dict()
        self.resolved_boulloteria = dict()
        self.resolved_publications = dict()
        self.pbw_composites = dict()

    def _urify(self, label):
        """Utility function to turn STAR predicates into real URIref objects"""
        p, c = label.split(':')
        return self.constants.namespaces[p][c]

    def collect_person_records(self):
        """Get a list of people whose floruit matches our needs"""
        if self.testmode:
            # Debugging / testing: restrict the list of relevant people
            debugnames = ['Anna', 'Apospharios', 'Bagrat', 'Balaleca', 'Gagik', 'Herve', 'Ioannes', 'Konstantinos',
                          'Liparites']
            debugcodes = [62, 64, 68, 101, 102, 110]
            return self.mysqlsession.query(pbw.Person).filter(
                and_(pbw.Person.name.in_(debugnames), pbw.Person.mdbCode.in_(debugcodes))
            ).all()
        relevant = [x for x in self.mysqlsession.query(pbw.Person).all()
                    if self.constants.inrange(x.floruit) and len(x.factoids) > 0]
        # Add the corner cases that we want to include: two emperors and a hegoumenos early in his career
        for name, code in [('Konstantinos', 8), ('Romanos', 3), ('Neophytos', 107)]:
            relevant.append(self.mysqlsession.query(pbw.Person).filter_by(name=name, mdbCode=code).scalar())
        print("Found %d relevant people" % len(relevant))
        return relevant

    def create_assertion_sparql(self, label, ptype, subj, obj, auth, src=None, based=None):
        """Create the SPARQL INSERT statement that corresponds to an assertion with the given parameters.
           Note that the object might be a list of literals."""
        c = self.constants

        # Mint a URI for this assertion based on the label. We hash respectively on label, ptype, subject, object,
        # authority, and source and base if they exist.
        hash_obj = " , ".join(str(o) for o in obj) if isinstance(obj, list) else obj
        hashargs = [label, ptype, subj, hash_obj, auth]
        if src is not None:
            hashargs.append(src)
        if based is not None:
            hashargs.append(based)
        assertion_uri = c.make_uri(*hashargs)

        # We may or may not have a basis for the assertion.
        basisstmt = ""
        if based is not None:
            basisstmt = f"{c.star_based} {based.n3()} ; \n        "

        # The object may be a list. Roleplay accordingly
        if isinstance(obj, list):
            sparqlobj = f"{c.star_object} " + ",".join([o.n3() for o in obj]) + " ;"
        else:
            sparqlobj = f"{c.star_object} {obj.n3()} ;"

        # Given all this, construct the statement
        sparql = f"""    {assertion_uri.n3()} a {c.get_assertion_for_predicate(ptype)} ;
        {c.star_subject} {subj.n3()} ;
        {sparqlobj} ;
        {basisstmt}{c.star_auth} {auth.n3()} .
"""
        # and add the source, if it is given.
        if src is not None:
            sparql += f"    {src.n3()} {c.star_src} {assertion_uri.n3()} . \n"

        # All done.
        return assertion_uri, sparql

    def gender_handler(self, sqlperson, graphperson):
        c = self.constants
        pbwdoc = c.namespaces['pbw'][f"person/{sqlperson.personKey}"]
        pbw_sex = sqlperson.sex
        if pbw_sex == 'Mixed':  # we have already excluded Anonymi
            pbw_sex = 'Unknown'
        elif pbw_sex == 'Eunach':  # correct misspelling in source DB
            pbw_sex = 'Eunuch'
        elif pbw_sex == '(Unspecified)':
            pbw_sex = 'Unknown'
        elif pbw_sex == 'Eunuch (Probable)':
            # All of these are known only from seals; presumably they are "probable eunuchs" based on
            # their titles, but I have no information about who made this determination or why. So we
            # go back to treating them as male, and can make a separate inference if we ever find out.
            pbw_sex = 'Male'
        if pbw_sex != "Unknown":
            # print("...setting gender assignment to %s%s" % (pbw_sex, " (maybe)" if uncertain else ""))
            # Create the SPARQL expression
            person_gender = c.make_uri('gender', graphperson, c.pbw_agent)
            a1, sparql1 = self.create_assertion_sparql('ga1', 'P41', person_gender, graphperson, c.pbw_agent)
            a2, sparql2 = self.create_assertion_sparql('ga2', 'P42', person_gender, c.get_gender(pbw_sex), c.pbw_agent)
            sparql = sparql1 + sparql2 + f"    {person_gender.n3()} a {c.get_label('E17G')} . \n"
            # Create it
            return c.update(sparql, pbwdoc, a1, a2)
        return None

    def identifier_handler(self, sqlperson, graphperson):
        """The identifier in this context is the 'origName' field, thus an identifier assigned by PBW
        not on the basis of any particular source. We turn this into an Appellation assertion"""
        c = self.constants
        pbwdoc = c.namespaces['pbw'][f"person/{sqlperson.personKey}"]
        # Strip any parenthetical from the nameOL field
        withparen = re.search(r'(.*)\s+\(.*\)', sqlperson.nameOL)
        if withparen is not None:
            appellation = withparen.group(1)
        else:
            appellation = sqlperson.nameOL.rstrip()

        # Create the SPARQL expression.
        appellation_uri = c.make_uri('appellation', graphperson, c.pbw_agent)
        a1, sparql = self.create_assertion_sparql('appel', 'P1', graphperson, appellation_uri, c.pbw_agent)
        sparql += f"""{appellation_uri.n3()} a {c.get_label('E33A')} ;
            {c.get_label('P190')} {Literal(appellation, lang=_get_source_lang(sqlperson)).n3()} .
        """
        # Create it
        return c.update(sparql, pbwdoc, a1)

    def get_source_and_agent(self, factoid):
        """Returns a pair of entities that represent the documentary source and the agent for this factoid.
        Creates the network of nodes and
        relationships to describe that source, if necessary. The source will either be an E34 Inscription from
        a physical E22 Human-Made Object (the boulloterion) or an E33 Linguistic Object, i.e. a passage from a
        Publication (the written primary source)."""
        # Is this a 'seals' source without a boulloterion? If so warn and return None
        sourcekey = self.constants.source(factoid)
        if self.constants.authorities(sourcekey) is None:
            if sourcekey != 'Seals' or factoid.boulloterion is None:
                warn(f"No boulloterion found for seal-sourced factoid {factoid.factoidKey}; skipping"
                     if sourcekey == 'Seals'
                     else f"Source {factoid.source} of factoid {factoid.factoidKey} not known; skipping")
                return None, None
        if factoid.boulloterion is not None:
            if len(factoid.boulloterion.publication) == 0:
                warn(f"Boulloterion {factoid.boulloterion.boulloterionKey} has empty publication list; skipping")
                return None, None
            agentnode = self.get_boulloterion_authority(factoid.boulloterion)
            sourcenode = self.get_boulloterion_inscription(factoid.boulloterion, agentnode)
            return sourcenode, agentnode
        else:
            # This factoid is taken from a document.
            agentnode = self.get_text_authority(sourcekey)
            sourcenode = self.get_text_sourceref(factoid)
            return sourcenode, agentnode

    def get_boulloterion_authority(self, boulloterion):
        """Return the PBW editor(s) responsible for the factoids arising from a boulloterion."""
        alist = dict()  # It would be a set if we could put dicts in sets
        for pub in boulloterion.publication:
            # If the publication isn't in the authority list, Michael analysed it
            if pub.bibSource is not None:
                auths = self.constants.authorities(pub.bibSource.shortName) or [self.constants.mj]
                for a in auths:
                    alist[a['identifier']] = a
        return self.get_viaf_agent_node(list(alist.values()))

    def get_boulloterion(self, boulloterion, pbweditor):
        """Helper function to find a boulloterion with a given ID. Creates its seals and sources
        if it is a new boulloterion. Returns the boulloterion and its inscription."""
        c = self.constants
        pbwdoc = c.pbw_uri(boulloterion)
        # boulloterion is a subclass of E22 Human-Made Object, with an identifier assigned by PBW
        keystr = str(boulloterion.boulloterionKey)
        btitle = f"Boulloterion of {boulloterion.title}"
        boul_node = self.find_or_create_boulloterion(keystr, btitle)
        # Create the boulloterion with its inscription, its seals, and its source list
        # Get the sources that PBW used for this boulloterion, if any
        source_node = self.get_boulloterion_sourcelist(boulloterion)

        # Make the assertion(s) concerning its inscription. Mark the inscription explicitly as E33 as well as
        # E34, since we can't always rely on having inferencing
        inscription_uri = c.make_uri(str(boul_node), boulloterion.origLText)
        sparql_stmts = [f"""    {inscription_uri.n3()} {c.get_label('P190')} {Literal(boulloterion.origLText, 
                                                                          lang=_get_source_lang(boulloterion)).n3()} ;
        a {c.get_label('E34')}, {c.get_label('E33')} . \n"""]
        a1, asp = self.create_assertion_sparql('boulloterion inscription', 'P128', boul_node,
                                               inscription_uri, pbweditor, based=source_node)
        sparql_stmts.append(asp)
        assertions = [a1]
        # Create the seals that belong to this boulloterion; assert that they
        # belong to their collection and that they came from this boulloterion.
        for i, seal in enumerate(boulloterion.seals):
            coll = self.find_or_create_seal_collection(seal.collection.collectionName)
            # Make an ID unique for our purposes
            seal_id = "%d-%d-%d" % (seal.collectionKey, seal.collectionRef, seal.sealKey)
            # Use a hash-based URI derived from the full PBW seal ID, per URI policy
            seal_uri = c.make_uri(c.entitylabels['E22S'], seal_id, c.r11_agent)
            sparql_stmts.append(f"""    {seal_uri.n3()} {c.label_n3} {Literal(seal_id).n3()} ;
        a {c.get_label('E22S')} . 
""")
            sa1, sq1 = self.create_assertion_sparql(f"seal collection", 'P46',
                                                    coll, seal_uri, pbweditor, src=source_node)
            sa2, sq2 = self.create_assertion_sparql(f"seal boulloterion", 'L1',
                                                    boul_node, seal_uri, pbweditor, based=source_node)
            assertions.extend([sa1, sa2])
            sparql_stmts.extend([sq1, sq2])

        # Update the graph and document its assertions
        c.update('\n'.join(sparql_stmts), pbwdoc, *assertions)

        # Return the boulloterion and inscription
        return boul_node, inscription_uri

    def get_boulloterion_inscription(self, boulloterion, pbweditor):
        # This factoid is taken from one or more seal inscriptions. Let's pull that out into CRM objects.
        # If the boulloterion has no associated publications, we shouldn't use it.
        if len(boulloterion.publication) == 0 and \
                boulloterion.boulloterionKey not in self.constants.boulloterion_sources:
            warn("No published source found for boulloterion %d; skipping this factoid" % boulloterion.boulloterionKey)
            return None
        # Get (create if necessary) the boulloterion node. This will also create the inscription.
        boul_node, inscription = self.get_boulloterion(boulloterion, pbweditor)
        return inscription

    def get_boulloterion_sourcelist(self, boulloterion):
        """A helper function to create the list of publications where the seals allegedly produced by a
        given boulloterion were published. Returns either a single Publication (if there was a single
        publication) or a Bibliography that represents a collection of Expressions. We do not
        try to isolate individual references here; anyone interested in that can follow the link back
        to the original boulloterion description."""
        c = self.constants
        # Extract the bibliography and page / object ref for each publication
        pubs = dedupe([x.bibSource for x in boulloterion.publication])
        if len(pubs) == 0:
            extrapub, ref = c.boulloterion_sources.get(boulloterion.boulloterionKey, (-1, None))
            if extrapub < 0:
                # We only have the seal catalogues as sources, and those attach to the seals.
                return None
            else:
                pubs = [self.mysqlsession.query(pbw.Bibliography).filter_by(bibKey=extrapub).scalar()]

        # Get some labels
        source_nodes = []
        for source in pubs:
            # Fix the encoding for the entries we didn't add
            short_name = source.shortName if source.bibKey == 816 else re_encode(source.shortName)
            latin_bib = source.latinBib if source.bibKey == 816 else re_encode(source.latinBib)
            # Write the entry for the publication. The short name is an identifier assigned by PBW,
            # so we will record it as such.
            src_uri = c.make_uri("boulloterion source", source.bibKey, c.pbw_agent)
            src_pbw_identifier = c.make_uri(c.entitylabels['E42'], short_name, c.pbw_agent)
            src_pbw_idassignment = c.make_uri(c.entitylabels['E15'], short_name, c.pbw_agent)
            src_sparql = f"""    {src_uri.n3()} {c.label_n3} {Literal(latin_bib).n3()} ;
        a {c.get_label('F2P')} .
    {src_pbw_identifier.n3()} {c.get_label('P190')} {Literal(short_name).n3()} ;
        a {c.get_label('E42')} .
    {src_pbw_idassignment.n3()} {c.star_auth} {c.pbw_agent.n3()};
        {c.star_subject} {src_uri.n3()} ;
        {c.get_label('P37')} {src_pbw_identifier.n3()} ;
        a {c.get_label('E15')}  .
"""
            c.update(src_sparql, None, src_pbw_idassignment)
            source_nodes.append(src_uri)
        if len(source_nodes) > 1:
            # Find or create a matching bibliography/publication list with only these publication nodes.
            return c.ensure_egroup_existence('E73B', 'P165', source_nodes,
                                             f"Bibliography for boulloterion {boulloterion.boulloterionKey}")
        else:
            # There was only a single source. We just return it.
            return source_nodes[0]

    def get_text_authority(self, fsource):
        """Return the authority (either a text author or someone else, e.g. the editor of the print edition) for the
        source behind this factoid."""
        # Do we have a known author for this text?
        author = self.get_author_node(self.constants.author(fsource))
        # If not, we use the editor(s) as the authority.
        editor = self.get_viaf_agent_node(self.constants.editor(fsource))
        # And if that doesn't exist, we use the PBW editor of the text.
        agent = self.get_viaf_agent_node(self.constants.authorities(fsource))
        # If there is no PBW scholar known for this source, we use the generic PBW agent.
        if agent is None:
            agent = self.constants.pbw_agent
        if author:
            return author
        print("No author given for source %s; using edition editor" % fsource)
        if editor:
            return editor
        print("...no editor either! Using PBW authority")
        return agent

    def get_text_sourceref(self, factoid):
        """Return an E33 Linguistic Object of the source reference for this factoid, ensuring that the correct
        assertions for the expression of the whole source work and its authorship."""
        # Get (possibly creating) the expression of the entire source
        c = self.constants
        pbwdoc = c.pbw_uri(factoid)
        wholesource = self.get_source_work_expression(factoid)
        if wholesource is None:
            return None
        # In this context, the agent is the PBW editor for this source.
        sourcekey = self.constants.source(factoid)  # e.g. 'Psellos Sathas 12'
        agent = self.get_viaf_agent_node(self.constants.authorities(sourcekey))
        reference_label = c.sourceref(factoid)      # e.g. '37.3-38.2'
        sourceref_uri = (c.make_uri(str(wholesource), reference_label, factoid.origLDesc)
                         if factoid.origLDesc
                         else c.make_uri(str(wholesource), reference_label))
        sparql = f"""    {sourceref_uri.n3()} {c.get_label('P190')} {Literal(factoid.origLDesc).n3()} ;
            {c.label_n3} {Literal(reference_label).n3()} ;
            a {c.get_label('E33')} .\n"""
        a, sparql_a = self.create_assertion_sparql('a', 'R15', wholesource, sourceref_uri, agent)
        sparql += sparql_a
        c.update(sparql, pbwdoc, a)
        return sourceref_uri

    def get_source_work_expression(self, factoid):
        # Ensure the existence of the work and, if it has a declared author, link the author to it via
        # a CREATION event, asserted by the author.
        c = self.constants
        orig_sourcekey = factoid.source
        sourcekey = c.source(factoid)
        workinfo = c.sourceinfo(sourcekey)
        pbw_authority = self.get_viaf_agent_node(c.authorities(sourcekey))
        editors = self.get_viaf_agent_node(workinfo.get('editor'))
        # NOTE I mislabelled these in the data hash. The 'work' is actually a spec:Text_Expression
        # and the 'expression' is actually a spec:Publication, both of which are F2s
        # The primary source identifier is the 'work' key, or else the PBW source ID string.
        text_id = workinfo.get('work')
        # The edition identifier is the 'expression' key (a citation to the edition).
        edition_id = workinfo.get('expression')  # The expression is really a spec:Publication

        # Check that we have the information on this source
        if editors is None or edition_id is None:
            print("Cannot ingest factoid with source %s until work/edition info is specified" % sourcekey)
            return None

        # Have we already retrieved / created this publication?
        if edition_id in self.resolved_publications:
            return self.resolved_publications[edition_id]

        afact_src = None

        # Deterministic URIs for the publication and its identifier assignment
        publ_uri = c.make_uri(c.entitylabels['F2P'], edition_id, 'https://r11.eu/')
        if orig_sourcekey == sourcekey:
            # If we haven't changed the sourcekey, the identifier for this edition comes from PBW.
            pub_agent = c.pbw_agent
        else:
            # If we did change the sourcekey, the identifier for this edition comes from us *and*
            # we are going to need to add the PBW source that this is a part of.
            pub_agent = c.r11_agent
            self._track_pbw_orig_source(orig_sourcekey, publ_uri)

        # Whatever the case, make the identifier for this particular source.
        pub_e42 = c.make_uri(c.entitylabels['E42'], sourcekey, pub_agent)
        pub_e15 = c.make_uri(c.entitylabels['E15'], sourcekey, pub_agent)
        sparql = f"""    {pub_e42.n3()} {c.get_label('P190')} {Literal(sourcekey).n3()} ;
        a {c.get_label('E42')} .
    {publ_uri.n3()} {c.label_n3} {Literal(edition_id).n3()} ;
        a {c.get_label('F2P')} .
    {pub_e15.n3()} {c.star_subject} {publ_uri.n3()} ;
        {c.get_label('P37')} {pub_e42.n3()} ;
        {c.star_auth} {pub_agent.n3()} ;
        a {c.get_label('E15')} .\n"""
        real_assertions = [pub_e15]

        if text_id is None:
            # We are dealing with a secondary source. Assert a publication creation instead of a
            # text (expression) creation, with the editors; we will have to go back later and say that this
            # depended on another work (the primary source).
            ec_uri = c.make_uri(c.entitylabels['F28'], edition_id, str(editors))
            a1, sparql_a1 = self.create_assertion_sparql('a1', 'R17', ec_uri, publ_uri, editors, publ_uri)
            a2, sparql_a2 = self.create_assertion_sparql('a2', 'P14', ec_uri, editors, editors, publ_uri)
            sparql += f"    {ec_uri.n3()} a {c.get_label('F28')} .\n"
            sparql += sparql_a1 + sparql_a2
            real_assertions.extend([a1, a2])
        else:
            # We are dealing with a primary source, so we need to make a bunch of assertions.
            # First, the editors assert that the edition (that is, the publication) belongs to
            # the work; the source for this is, well, the edition.
            work_uri = c.make_uri(c.entitylabels['F2T'], text_id, 'https://r11.eu/')
            sparql += f"""    {work_uri.n3()} {c.label_n3} {Literal(text_id).n3()} ; a {c.get_label('F2T')} .\n"""
            a1, sparql_a1 = self.create_assertion_sparql('a1', 'R76', publ_uri, work_uri, editors, publ_uri)
            sparql += sparql_a1
            real_assertions.append(a1)

            # Now we need to see if authorship has to be asserted.
            author = self.get_author_node(self.constants.author(sourcekey))
            if author is not None:
                # Make the assertions that the author authored the work. If we have a factoid,
                # then the authority for this assertion is the factoid's primary referent.
                # Otherwise, the authority (for now) is the editor.
                # If we don't have a specific reference for the claim, we just use the edition (again).
                aship_authority = editors
                aship_source_uri = publ_uri
                has_srcref = False

                if 'factoid' in workinfo:
                    # Pull in the authorship factoid that describes the authorship of this work
                    afact = self.mysqlsession.query(pbw.Factoid).filter_by(factoidKey=workinfo['factoid']).scalar()
                    afact_src = c.pbw_uri(afact)
                    asourcekey = c.source(afact)
                    if asourcekey != sourcekey:
                        print("CHECK: Using %s authorship factoid from different work %s" % (
                            sourcekey, asourcekey))
                    # Find the primary person for the authorship factoid
                    fact_person = afact.main_person()
                    if len(fact_person) > 1:
                        print("CHECK: More than one main person in a factoid?")
                    # Make sure that the primary factoid person is indeed our author
                    fp0 = fact_person[0]
                    if author != self.find_or_create_pbwperson(fp0):
                        print("CHECK: Is %s multiply authored, and is %s %d among the authors?" % (
                            sourcekey, fp0.name, fp0.mdbCode))
                    # If the factoid is an authorship factoid, then the author is claiming to have written; if
                    # it is a narrative factoid, then the PBW editor is making the claim
                    # Either way, the PBW editor will be who says this passage belongs to the edition
                    aship_authority = author if afact.factoidType == 'Authorship' else pbw_authority
                    # We have to make a sourceref node, connected to this text, for the factoid source.
                    srcref_uri = (c.make_uri(str(publ_uri), afact.sourceRef, afact.origLDesc)
                                  if afact.origLDesc
                                  else c.make_uri(str(publ_uri), afact.sourceRef))
                    sparql += f"""    {srcref_uri.n3()} {c.get_label('P190')} {Literal(afact.origLDesc, lang=_get_source_lang(afact)).n3()} ;
        {c.label_n3} {Literal(afact.sourceRef).n3()} ;
        a {c.get_label('E33')} .\n"""
                    aship_source_uri = srcref_uri
                    has_srcref = True
                elif 'provenance' in workinfo:
                    # We have a page number. This makes our authorship authority the editor(s), with the source
                    # being the passage in this very edition.
                    srcref_uri = c.make_uri(str(publ_uri), workinfo['provenance'])
                    sparql += f"""    {srcref_uri.n3()} {c.label_n3} {Literal(workinfo['provenance']).n3()} ;
        a {c.get_label('E33')} .\n"""
                    aship_source_uri = srcref_uri
                    has_srcref = True

                if has_srcref:
                    # It is the PBW editor who says that a particular passage exists and belongs to the
                    # respective edition. We have no further source or evidence of this, apart from the DB
                    # record that will get documented via an E31 link.
                    # n.b. We will need to fix/change this manually for non-factoid provenance!
                    a2, sparql_a2 = self.create_assertion_sparql('a2', 'R15', publ_uri, aship_source_uri,
                                                                  pbw_authority)
                    sparql += sparql_a2
                    real_assertions.append(a2)

                # We have now dealt with extracting information from some relevant authorship factoid, if it exists.
                # Move on to the assertion that the author authored the work
                wc_uri = c.make_uri(c.entitylabels['F28'], text_id, str(aship_authority))
                a3, sparql_a3 = self.create_assertion_sparql('a3', 'R17', wc_uri, work_uri,
                                                              aship_authority, aship_source_uri)
                a4, sparql_a4 = self.create_assertion_sparql('a4', 'P14', wc_uri, author,
                                                              aship_authority, aship_source_uri)
                sparql += f"    {wc_uri.n3()} a {c.get_label('F28')} .\n"
                sparql += sparql_a3 + sparql_a4
                real_assertions.extend([a3, a4])

        # Whatever we just made, insert it and document the assertions.
        if afact_src:
            c.update(sparql, afact_src, *real_assertions)
        else:
            c.update(sparql, None, *real_assertions)
        self.resolved_publications[edition_id] = publ_uri
        return publ_uri

    def _track_pbw_orig_source(self, pbw_id_string, part_uri):
        """Initialise or update a PBW source that we split up. Exclude the PBW 'sources' that don't actually
        denote a single publication."""
        c = self.constants
        # Don't bother with these
        if pbw_id_string in ['Alexios Stoudites', 'Eustathios Romaios', 'Nea Mone', 'Psellos']:
            return
        # Warn if we don't get a bibliography string for the composite
        composite_bibstring = c.composite_source(pbw_id_string)
        if composite_bibstring is None:
            warn(f"Unable to find composite bibliography string for PBW source {pbw_id_string}")
        # Make a note of it
        if pbw_id_string not in self.pbw_composites:
            self.pbw_composites[pbw_id_string] = {'parts': [part_uri], 'citation': composite_bibstring}
        else:
            self.pbw_composites[pbw_id_string]['parts'].append(part_uri)

    def _find_or_create_identified_entity(self, etype, agent, identifier, dname):
        """Return an identified entity URIRef. This can be a Boulloterion (E22 subclass) or an E21 Person
        depending on context. It is labeled with the identifier via an E15 Identifier Assignment
        carried out by the given agent, with dname becoming our preferred human-readable identifier."""
        c = self.constants
        hash_id = identifier
        if etype == c.get_label('E22B'):
            # Identifier is a number, thus possibly not unique
            url = URIRef(f'https://pbw2016.kdl.kcl.ac.uk/boulloterion/{identifier}/')
            # ...so for hashing, we want to prepend 'boulloterion'.
            hash_id = f'{c.entitylabels['E22B']} / {identifier}'
        elif etype == c.get_label('E27'):
            # Identifier is also a number, thus possibly not unique
            url = URIRef(f'https://pbw2016.kcl.ac.uk/location/{identifier}/')
            # ...so for hashing, we want to use the display name, which is in this case unique.
            hash_id = dname
        elif agent == c.pbw_agent:
            # Identifier is something like 'Alexios 10102' or 'Alp Arslan 51'.
            # The URL changes it to 'Alexios/10102' or 'Alp+Arslan/51'
            # Need to deal with identifiers like Alp Arslan 51, Gostri[...] 101, Nizam al-Mulk 101
            idparts = identifier.split()  # This deals with trailing whitespace on the name
            code = idparts.pop()          # This leaves behind the whitespace-separated name parts
            # This replaces spaces with + and removes [] characters
            id_urified = '+'.join(idparts).replace('[', '').replace(']', '')
            # Now we have Alp+Arslan/51, Gostri.../101, and Nizam+al-Mulk/101 respectively.
            # Even if none of these URLs actually work in PBW.
            url = URIRef(f'https://pbw2016.kdl.kcl.ac.uk/person/{id_urified}/{code}/')
        else:
            # Identifier is again a number. We can leave this bare, since VIAF doesn't reuse numbers.
            url = URIRef(f'https://viaf.org/viaf/{identifier}/')

        # Create the entity URI based on its name and service
        entity_uri = c.make_uri(hash_id, str(agent))
        # The entity should have its display name as its label, without a language designation.
        #
        entitystr = f"{entity_uri.n3()} a {etype} "
        if dname is not None:
            entitystr += f";\n            {c.label_n3} {Literal(dname).n3()} "
        entitystr += '.'

        # Construct the identifier assignment that should exist.
        the_e42 = c.make_uri(c.entitylabels['E42'], hash_id, str(agent))
        the_e15 = c.make_uri(c.entitylabels['E15'], hash_id, str(agent))
        sparql = f"""
        {entitystr}
        {the_e42.n3()} {c.get_label('P190')} {Literal(str(identifier)).n3()} ;
            {c.link_n3} {url.n3()} ;
            a {c.get_label('E42')} .
        {the_e15.n3()} {c.get_label('P37')} {the_e42.n3()} ;
            {c.star_subject} {entity_uri.n3()} ;
            {c.star_auth} {agent.n3()} ;
            a {c.get_label('E15')} .
        """

        # Ensure its existence and return the entity in question
        c.update(sparql, None, the_e15)
        return entity_uri

    def find_or_create_pbwperson(self, sqlperson):
        # Cache these
        textkey = f"{sqlperson.name} {sqlperson.mdbCode}"
        if textkey not in self.resolved_persons:
            self.resolved_persons[textkey] = self._find_or_create_identified_entity(
                self.constants.get_label('E21'), self.constants.pbw_agent, textkey, sqlperson.descName)
        return self.resolved_persons[textkey]

    def find_or_create_viafperson(self, name, viafid):
        if viafid not in self.resolved_persons:
            self.resolved_persons[viafid] = self._find_or_create_identified_entity(
                self.constants.get_label('E21'), self.constants.viaf_agent, viafid, name)
        return self.resolved_persons[viafid]

    def find_or_create_boulloterion(self, keystr, btitle):
        if keystr not in self.resolved_boulloteria:
            self.resolved_boulloteria[keystr] = self._find_or_create_identified_entity(
                self.constants.get_label('E22B'), self.constants.pbw_agent, keystr, btitle)
        return self.resolved_boulloteria[keystr]

    def find_or_create_location(self, sqlloc):
        c = self.constants
        k = sqlloc.locationKey
        if k not in self.resolved_locations:
            # If we haven't seen it yet, make it
            loc_ent = self._find_or_create_identified_entity(
                self.constants.get_label('E27'), self.constants.pbw_agent,
                k, sqlloc.locName)
            self.resolved_locations[k] = loc_ent
            # ...and add the gazetteer links that Charlotte made
            geoagent = self.get_viaf_agent_node([c.cr])
            loc_sparql = ''
            to_doc = []
            if sqlloc.pleiades_id:
                pleiades_uri = URIRef(f'https://pleiades.stoa.org/places/{sqlloc.pleiades_id}')
                ap, ap_sparql = self.create_assertion_sparql('ap', 'ID8', loc_ent, pleiades_uri, geoagent)
                loc_sparql += ap_sparql
                to_doc.append(ap)
            if sqlloc.geonames_id:
                geonames_uri = URIRef(f'https://www.geonames.org/{sqlloc.geonames_id}')
                ag, ag_sparql = self.create_assertion_sparql('ag', 'ID8', loc_ent, geonames_uri, geoagent)
                loc_sparql += ag_sparql
                to_doc.append(ag)
            if loc_sparql:
                c.update(loc_sparql, None, *to_doc)
        return self.resolved_locations[k]

    # This one doesn't use an E15 assertion, it is just a thing with a name
    def find_or_create_seal_collection(self, collname):
        c = self.constants
        coll_uri = c.make_uri(collname, c.pbw_agent)
        sparql = f"""    {coll_uri.n3()} a {c.get_label('E78')} ;
            {c.label_n3} {Literal(collname).n3()} .
"""
        c.update(sparql)
        return coll_uri

    def get_author_node(self, authorlist):
        """Return the E21 Person node for the author of a text, or a group of authors if authorship was composite"""
        if authorlist is None or len(authorlist) == 0:
            return None
        authors = []
        for i in range(0, len(authorlist), 2):
            pname = authorlist[i]
            pcode = authorlist[i + 1]
            # Hackish, but after all this is a script... If the code is longer than five digits, we are
            # dealing with a VIAF person; otherwise we are dealing with a PBW person.
            if len(str(pcode)) > 5:
                authors.append((self.find_or_create_viafperson(pname, pcode), pname))
            else:
                # We need to get the SQL record for the author since we might still need to create them in the graphdb
                sqlperson = self.mysqlsession.query(pbw.Person).filter_by(name=pname, mdbCode=pcode).scalar()
                authors.append((self.find_or_create_pbwperson(sqlperson), sqlperson.descName))
        if len(authors) > 1:
            # It is our multi-authored text. Make a group because both authors share authority.
            return self._find_or_create_authority_group(authors)
        else:
            return authors[0][0]

    def get_viaf_agent_node(self, authoritylist):
        """Create or find the node for the given authority in our (modern) authority list."""
        if authoritylist is None or len(authoritylist) == 0:
            return None
        if len(authoritylist) == 1:
            authority = authoritylist[0]
            # We have a cache of these things
            return self._lookup_authority_node(authority)
        # If we get here, we have more than one authority for this source.
        # Ensure the existence of the people, and then ensure the existence of their group
        scholars = []
        for p in authoritylist:
            scholars.append((self._lookup_authority_node(p), p['identifier']))
        return self._find_or_create_authority_group(scholars)

    def _lookup_authority_node(self, authority):
        viaf_id = authority['viaf']
        if viaf_id not in self.resolved_authorities:
            self.resolved_authorities[viaf_id] = self.find_or_create_viafperson(authority['identifier'], viaf_id)
        return self.resolved_authorities[viaf_id]

    def _find_or_create_authority_group(self, members_with_names):
        if len(members_with_names) < 2:
            warn(f"Tried to create authority group with {len(members_with_names)} member(s)!")
            return None

        c = self.constants
        title = '; '.join(sorted(name for _, name in members_with_names))
        members = [uri for uri, _ in members_with_names]
        return c.ensure_egroup_existence('E74A', 'P107', members, title)

    def maka_handler(self, factoid, graphperson):
        """Associate the person with another person, provisionally. This is the one use of the
        uncertainty typing on assertions that we use here."""
        c = self.constants
        pbwdoc = c.pbw_uri(factoid)
        # Avoid 'uncertain' identity factoids that are about group membership
        exclude_strings = ['the two collectively are',
            'included in',
            'was one of'
        ]
        for xs in exclude_strings:
            if xs in factoid.engDesc:
                print(f"Skipping evident group membership factoid {factoid.factoidKey}: {factoid.replace_referents()}")
                return

        # Fish out the other person(s) with whom identity is being asserted
        sparql = ''
        assertions = []
        for i, otherperson in enumerate(factoid.referents()):
            if otherperson.name in ['Anonymi', 'Anonymae']:
                print(f"Skipping group membership for uncertain identity in factoid {factoid.factoidKey}: {factoid.replace_referents()}")
                return
            graphother = self.find_or_create_pbwperson(otherperson)
            a, sparql_part = self.create_assertion_sparql(f"a{i}", 'ID8', graphperson, graphother, c.pbw_agent)
            sparql += sparql_part
            # Mark it as a suggestion rather than a full-on assertion
            sparql += f"    {a.n3()} a {c.get_label('S5')} .\n"
            assertions.append(a)

        if len(assertions):
            # Add them to the graph
            c.update(sparql, pbwdoc, *assertions)

    def appellation_handler(self, sourcenode, agent, factoid, graphperson):
        """This handler deals with Second Name factoids and also Alternative Name factoids.
        The Second Names might be in all sorts of languages in the factoid itself, but refer
        to a canonical version of the name in the FamilyName table, which is probably usually
        Greek. The Alternative Name factoids should exclusively use the information in the
        base factoid."""
        c = self.constants
        pbwdoc = c.pbw_uri(factoid)

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
                olang = _get_source_lang(factoid.secondName) or 'grc'
            else:
                name_en = factoid.engDesc
                name_ol = factoid.origLDesc
                olang = _get_source_lang(factoid) or 'grc'
            print("Adding second name %s (%s '%s')" % (name_en, olang, name_ol))

        appel_uri = c.make_uri(c.entitylabels['E33A'], name_ol, name_en)
        sparql = f"""    {appel_uri.n3()} {c.get_label('P190')} {Literal(name_ol, olang).n3()} ;
            {c.get_label('P190')} {Literal(name_en, 'en').n3()} ;
            a {c.get_label('E33A')} .\n"""
        a1, sparql_a1 = self.create_assertion_sparql('a1', 'P1', graphperson, appel_uri, agent, sourcenode)
        sparql += sparql_a1
        return c.update(sparql, pbwdoc, a1)

    def description_handler(self, sourcenode, agent, factoid, graphperson):
        """Record the descriptions given in the sources as P3 data-property assertions."""
        c = self.constants
        pbwdoc = c.pbw_uri(factoid)

        olang = _get_source_lang(factoid) or 'grc'
        descriptions = [Literal(factoid.replace_referents(), 'en'), Literal(factoid.origLDesc, olang)]
        a1, sparql = self.create_assertion_sparql('a1', 'P3', graphperson, descriptions, agent, sourcenode)
        return c.update(sparql, pbwdoc, a1)

    def death_handler(self, sourcenode, agent, factoid, graphperson):
        # Each factoid is its own set of assertions pertaining to the single death of a person.
        # When there are multiple sources, we will have to review them for consistency and make
        # proxies for the death event as necessary.
        c = self.constants
        pbwdoc = c.pbw_uri(factoid)

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
            return None

        # Set up the death event with a deterministic URI. We take for granted that every 11th-c.
        # person has exactly one death.
        # For ease of understanding we also give the death event a label with the person's PBW identifier.
        death_event_uri = c.make_uri(str(graphperson), 'death')
        pbwperson = factoid.main_person()[0]
        death_event_label = f"Death of {pbwperson.name} {pbwperson.mdbCode}, {pbwperson.descName}"
        deathof_uri = c.make_uri(c.get_assertion_for_predicate('P100'), death_event_uri, graphperson)
        # Since there is no authority, we don't use create_assertion_sparql
        sparql_event = f"""    {death_event_uri.n3()} a {c.get_label('E69')} ;
        {c.label_n3} {Literal(death_event_label).n3()} .
    {deathof_uri.n3()} {c.star_subject} {death_event_uri.n3()} ;
        {c.star_object} {graphperson.n3()} ;
        a {c.get_assertion_for_predicate('P100')}.\n"""
        assertions_created = [deathof_uri]
        deathevent = death_event_uri

        # Get the description of the death in English and the original language, if applicable
        olang = _get_source_lang(factoid) or 'grc'
        descriptions = [Literal(factoid.replace_referents(), 'en')]
        if len(factoid.origLDesc):
            descriptions.append(Literal(factoid.origLDesc, olang))
        desc_lines = ' ; '.join([f"{c.get_label('P190')} {x.n3()}" for x in descriptions])

        # Assert the description(s) that we found
        desc_e33_uri = c.make_uri(sourcenode, factoid.replace_referents(), agent)
        a1, sparql_desc = self.create_assertion_sparql('a1', 'P67', desc_e33_uri, deathevent, agent, sourcenode)
        assertions_created.append(a1)
        # Add the description content
        sparql_desc += f"    {desc_e33_uri.n3()} a {c.get_label('E33')} ; {desc_lines} .\n"

        # See if the death is dated and, if so, add the date.
        deathdate = factoid.deathRecord.sourceDate
        sparql_date = ""
        if deathdate:
            deathdate_uri = c.make_uri(c.entitylabels['E52'], deathdate, str(deathevent))
            # The date is actually asserted by the PBW editor for the factoid, based on the source but documented
            # in the factoid record!
            pbw_ed = self.get_viaf_agent_node(c.authorities(c.source(factoid)))
            a2, sparql_date = self.create_assertion_sparql('a2', 'P4', deathevent, deathdate_uri,
                                                            pbw_ed, pbwdoc, sourcenode)
            assertions_created.append(a2)

            # Add the date content.
            # The "deathdate" is a string description of when the death happened.
            # If it says "after" something, then it qualifies the beginning (P79);
            # if it says "before" something, then it qualifies the end (P80).
            # If it doesn't say either, then we assume it qualifies the beginning and the end.
            date_preds = ['P79', 'P80']
            if 'after' in deathdate:
                date_preds.remove('P80')
            elif 'before' in deathdate:
                date_preds.remove('P79')
            sparql_parts = [f"    {deathdate_uri.n3()} a {c.get_label('E52')}"]
            for dp in date_preds:
                sparql_parts.append(f"        {c.get_label(dp)} {Literal(deathdate).n3()}")
            sparql_date += " ;\n".join(sparql_parts) + " .\n"

        c.update(sparql_event + sparql_desc + sparql_date, pbwdoc, *assertions_created)
        return assertions_created

    def ethnicity_handler(self, sourcenode, agent, factoid, graphperson):
        """Assign a group membership for the given ethnicity to the person"""
        c = self.constants
        pbwdoc = c.pbw_uri(factoid)
        if factoid.ethnicityInfo is None or factoid.ethnicityInfo.ethnicity is None:
            # We can't assign any ethnicity without the ethnicity info
            warn("Empty ethnicity factoid found: id %s" % factoid.factoidKey)
            return None
        elabel = factoid.ethnicityInfo.ethnicity.ethName
        groupid = c.get_ethnicity(elabel)
        a1, sparql = self.create_assertion_sparql('a1', 'P107', groupid, graphperson, agent, sourcenode)
        return c.update(sparql, pbwdoc, a1)

    # Helper to create the assertions for our various social designation groups
    def _find_or_create_social_designation(self, sourcenode, agent, factoid, graphperson, des, eclass, whopred,
                                           whichpred):
        # (grouping:label) [:whopred] person
        # (grouping) [:whichpred] rnode
        c = self.constants
        pbwdoc = c.pbw_uri(factoid)
        # We will sometimes have duplicate assertions originating from different geographic
        # scopes, which we have chopped off in the title name. For now we keep these duplicate.
        # TODO we should add the geographic scopes to these when we can...
        des_uri = c.make_uri(eclass, str(graphperson), str(des), str(pbwdoc))
        a1, sparql_a1 = self.create_assertion_sparql('a1', whopred, des_uri, graphperson, agent, sourcenode)
        a2, sparql_a2 = self.create_assertion_sparql('a2', whichpred, des_uri, des, agent, sourcenode)
        sparql = f"    {des_uri.n3()} a {eclass.n3()} .\n"
        sparql += sparql_a1 + sparql_a2

        # Document it in either case as coming from this factoid
        return c.update(sparql, pbwdoc, a1, a2)

    def religion_handler(self, sourcenode, agent, factoid, graphperson):
        """Assign a group membership for the given religious confession to the person"""
        if factoid.religion is None:
            warn("Empty religion factoid found: id %d" % factoid.factoidKey)
            return None
        rlabel = factoid.religion
        # Special case, database had an error
        if factoid.religion == '':
            rlabel = 'Heretic'
        relid = self.constants.get_religion(rlabel)
        # (r:C23 Religious identity) [rwho:P36 pertains to] person
        # (r:C23 Religious identity) [rwhich:P35 is defined by] rnode
        return self._find_or_create_social_designation(sourcenode, agent, factoid, graphperson, relid,
                                                self.constants.entitylabels['C23'], 'SP36', 'SP35')

    def societyrole_handler(self, sourcenode, agent, factoid, graphperson):
        if factoid.occupation is None:
            return None
        roleid, roleclass = self.constants.get_societyrole(factoid.occupation)
        roletype = self.constants.entitylabels['C1']
        whopred = 'SP13'
        whichpred = 'SP14'
        if roleclass == self.constants.get_label('C12'):
            # We need to treat it as a legal role instead of a social role / occupation
            roletype = self.constants.entitylabels['C13']
            whopred = 'SP26'
            whichpred = 'SP33'
        # (r:C1 Social Quality of an Actor) [rwho:P13 pertains to] person
        # (r:C1) [rwhich:P14 is defined by] rnode
        return self._find_or_create_social_designation(sourcenode, agent, factoid, graphperson, roleid, roletype,
                                                whopred, whichpred)

    def dignity_handler(self, sourcenode, agent, factoid, graphperson):
        if factoid.dignityOffice is None:
            return None
        dignity_id, dignity_class = self.constants.get_dignity(factoid.dignityOffice.stdName)
        roletype = self.constants.entitylabels['C13']
        whopred = 'SP26'
        whichpred = 'SP33'
        if dignity_class == self.constants.get_label('C2'):
            # We need to treat it as a social instead of a legal role
            roletype = self.constants.entitylabels['C1']
            whopred = 'SP13'
            whichpred = 'SP14'
        # We treat (most) dignities as legal roles
        # (r:C13 Social Role Embodiment) [dwho:P26 is embodied by] person
        # (r:C13) [dwhich:P33 is embodiment of] dignity
        return self._find_or_create_social_designation(sourcenode, agent, factoid, graphperson, dignity_id, roletype,
                                                whopred, whichpred)

    def languageskill_handler(self, sourcenode, agent, factoid, graphperson):
        """Assign a language skill to the person"""
        c = self.constants
        pbwdoc = c.pbw_uri(factoid)
        if factoid.languageSkill is None:
            return None
        # Language know-how ID
        lkhid = self.constants.get_language(factoid.languageSkill)
        # This doesn't chain quite the same way as the others do
        # person [rwho:P38 has skill] (r:C21 Skill)
        # (r:C21 Skill) [rwhich:P37 concerns] (l:C29 Know-How)
        lskill_uri = c.make_uri(c.entitylabels['C21'], str(graphperson), str(lkhid), str(pbwdoc))
        a1, sparql_a1 = self.create_assertion_sparql('a1', 'SP38', graphperson, lskill_uri, agent, sourcenode)
        a2, sparql_a2 = self.create_assertion_sparql('a2', 'SP37', lskill_uri, lkhid, agent, sourcenode)
        sparql = f"    {lskill_uri.n3()} a {c.get_label('C21')} .\n"
        sparql += sparql_a1 + sparql_a2
        return c.update(sparql, pbwdoc, a1, a2)

    def location_handler(self, sourcenode, agent, factoid, graphperson):
        """Associate the person with a location. These are honestly very inexact factoids, so we
        represent this with a generic event taking place at the location that involved the person.
        This will need to be refined with spreadsheet data."""
        c = self.constants
        pbwdoc = c.pbw_uri(factoid)
        if factoid.locationInfo is None or factoid.locationInfo.location is None:
            # We can't assign any location without the location info
            warn("Empty location factoid found: id %s" % factoid.factoidKey)
            return None
        # Get the location in question
        loc_ent = self.find_or_create_location(factoid.locationInfo.location)
        # Now connect the person to the location via the extremely generic 'event', as that is all PBW gives us
        # Label the event with the factoid key for easier resolution from the spreadsheets later
        locevent_uri = c.make_uri(c.entitylabels['E5'], str(graphperson), str(loc_ent), str(agent), str(sourcenode))
        lfactlabel = Literal(f"Location event for factoid {factoid.factoidKey}")
        sparql = f"    {locevent_uri.n3()} {c.label_n3} {lfactlabel.n3()} ;\n        a {c.get_label('E5')} .\n"
        a1, sparql_a1 = self.create_assertion_sparql('a1', 'P7', locevent_uri, loc_ent, agent, sourcenode)
        a2, sparql_a2 = self.create_assertion_sparql('a2', 'P11', locevent_uri, graphperson, agent, sourcenode)
        sparql += sparql_a1 + sparql_a2
        return c.update(sparql, pbwdoc, a1, a2)

    def kinship_handler(self, sourcenode, agent, factoid, graphperson):
        # These are social relationships as opposed to social roles, so they need a partner.
        # (rel:C3 Social Relationship) [pt:P16 has type] (kt:C4 Kinship type)
        # (rel) [src:P17 has source] (p:E21 person)
        # (rel) [trg:P18 has target] (p:E21 kin)
        c = self.constants
        pbwdoc = c.pbw_uri(factoid)
        if factoid.kinshipType is None:
            warn("Empty kinship factoid found: id %d" % factoid.factoidKey)
            return None
        ktype = c.get_kinship(factoid.kinshipType.gspecRelat)

        for kin in factoid.referents():
            if kin.name == 'Anonymi' or kin.name == 'Anonymae':
                # We skip kin who are anonymous groups
                continue
            graphkin = self.find_or_create_pbwperson(kin)
            if graphkin == graphperson:
                # We skip self-referential kinship factoids
                warn("Person %s listed as related to self" % kin)
                continue
            # Mint a deterministic URI for the kinship state from the C3 class URI and the two persons
            kstate_uri = c.make_uri(str(c.namespaces['sdhss']['C3']), str(graphperson), str(graphkin))
            # Now set up the three kinship assertions
            a1, sparql_a1 = self.create_assertion_sparql('a1', 'SP16', kstate_uri, ktype, agent, sourcenode)
            a2, sparql_a2 = self.create_assertion_sparql('a2', 'SP17', kstate_uri, graphperson, agent, sourcenode)
            a3, sparql_a3 = self.create_assertion_sparql('a3', 'SP18', kstate_uri, graphkin, agent, sourcenode)
            sparql = f"    {kstate_uri.n3()} a {c.get_label('C3')} .\n"
            sparql += sparql_a1 + sparql_a2 + sparql_a3
            return c.update(sparql, pbwdoc, a1, a2, a3)
        return None

    def possession_handler(self, sourcenode, agent, factoid, graphperson):
        """Ensure the existence of an E18 Physical Thing (we don't have any more category info about
        the possessions). For now, we assume that a possession with an identical description is, in fact,
        the same possession."""
        c = self.constants
        pbwdoc = c.pbw_uri(factoid)

        # Give the possession its description, which comes out of the factoid's engDesc
        possession_uri = c.make_uri(c.entitylabels['E18'], str(graphperson), str(agent), str(sourcenode))
        sparql = f"""    {possession_uri.n3()} {c.label_n3} {Literal(factoid.replace_referents()).n3()} ;
            a {c.get_label('E18')} .\n"""
        # Assert ownership of the possession
        a1, sparql_a1 = self.create_assertion_sparql('a1', 'P51', possession_uri, graphperson, agent, sourcenode)
        sparql += sparql_a1
        if factoid.possession is not None and factoid.possession != '':
            # Give the assertion a note, if such note exists in the poorly-named PossessionFactoid.possessionName field
            sparql += f"    {a1.n3()} {c.get_label('P3')} {Literal(factoid.possession).n3()} .\n"
        return c.update(sparql, pbwdoc, a1)

    def record_pbw_composite_sources(self):
        """To be run after most other things are done. Creates the composite source for each PBW source
        that we split up, and adds the E15/E42 identifier to that source as well as the links to its parts."""
        c = self.constants
        for key, info in self.pbw_composites.items():
            print(f"Creating composite PBW source for {key}")
            # Make the items
            composite_uri = c.make_uri(c.entitylabels['F2P'], info['citation'], c.pbw_agent)
            pbw_e15 = c.make_uri(c.entitylabels['E15'], key, c.pbw_agent)
            pbw_e42 = c.make_uri(c.entitylabels['E42'], key, c.pbw_agent)
            asserted = [pbw_e15]
            # Make the assertions
            sparql = f"""    {composite_uri.n3()} a {c.get_label('F2P')} ;
        {c.label_n3} {Literal(info['citation']).n3()} .
    {pbw_e42.n3()} a {c.get_label('E42')} ; 
        {c.get_label('P190')} {Literal(key).n3()} .
    {pbw_e15.n3()} a {c.get_label('E15')} ;
        {c.star_subject} {composite_uri.n3()} ;
        {c.get_label('P37')} {pbw_e42.n3()} ;
        {c.star_auth} {c.pbw_agent.n3()} .
"""
            print(f"...adding {len(info['parts'])} publication parts to {key}")
            for i, pubpart in enumerate(info['parts']):
                # Assert that we say the part is part of the whole
                a, sparql_a = self.create_assertion_sparql(f'a{i}', 'R5', composite_uri, pubpart, c.r11_agent, composite_uri)
                asserted.append(a)
                sparql += sparql_a
            c.update(sparql, URIRef('https://pbw2016.kdl.kcl.ac.uk/ref/sources/'), *asserted)

    def record_assertion_factoids(self):
        """To be run after everything else is done. Creates the assertion record for all assertions created here,
        tying each to the factoid or person record that originated it and tying all the assertion records to the
        database creation event."""

        # Add the responsible person. TODO this should have more options than just tla
        tla = self.get_viaf_agent_node([self.constants.ta])
        return self.constants.record_script_run(tla)

    def _person_process_loop(self, person, direct_person_records, factoid_types, used_sources, boulloteria):
        c = self.constants
        # Skip the anonymous groups for now
        if person.name == 'Anonymi':
            return None
        # Create or find the person node
        print(f"*** {datetime.now().strftime('%d %H:%M:%S')} Making/finding node for person "
              f"{person.name} {person.mdbCode} ***")
        graph_person = self.find_or_create_pbwperson(person)

        # Get the 'factoids' that are directly in the person record
        for ftype in direct_person_records:
            ourftype = _smooth_labels(ftype)
            try:
                method = getattr(self, "%s_handler" % ourftype.lower())
                method(person, graph_person)
            except AttributeError:
                warn(f"No handler for {ourftype} record info; skipping.")


        # Now get the factoids that are really factoids
        for ftype in factoid_types:
            ourftype = _smooth_labels(ftype)
            try:
                method = getattr(self, "%s_handler" % ourftype.lower())
            except AttributeError:
                continue
            fprocessed = 0
            for f in person.main_factoids(ftype):
                if ftype in ['Uncertain Ident']:
                    # There is no source, so no individual agent. Just run the factoid processing method.
                    method(f, graph_person)
                    fprocessed = fprocessed+1
                    continue

                # Find out what sources we are actually using and make note of them
                source_key = c.source(f)
                if source_key is None and ourftype != 'UncertainIdent':
                    print(f"Skipping factoid {f.factoidKey} with unlisted source {f.source}")
                    continue
                elif source_key == 'OUT_OF_SCOPE':
                    print(f"Skipping factoid {f.factoidKey} with a source {f.source} out of our temporal scope")
                    continue
                else:
                    used_sources.add(source_key)
                # Note if we use a boulloterion
                if f.boulloterion is not None:
                    boulloteria.add(f.boulloterion.boulloterionKey)
                # Get the source, either a text passage or a seal inscription, and the authority
                # for the factoid. Authority will either be the author of the text, or the PBW
                # colleague who read the text and ingested the information.
                (source_node, authority_node) = self.get_source_and_agent(f)
                # If the factoid has no source then we skip it
                if source_node is None:
                    print(f"HELP: Factoid {f.factoidKey} had no parseable source for some reason")
                    continue
                # If the factoid has no authority then we assign it to the generic PBW agent
                if authority_node is None:
                    authority_node = c.pbw_agent
                # Call the handler for this factoid type
                assertions_created = method(source_node, authority_node, f, graph_person)
                # Keep track of how many factoids we have converted
                fprocessed += 1

                # Now see if we can assign a reading interpretation event.
                if assertions_created:
                    # Who read this source to create this factoid?
                    if f.boulloterion is not None:
                        # The authority is already the PBW reader.
                        source_reader = authority_node
                    elif c.sourceinfo(source_key):
                        # We should have the record of who read this source.
                        source_reader = self.get_viaf_agent_node(c.sourceinfo(source_key).get('authority'))
                    else:
                        # If we don't, we need to make a warning for the time being, and move on
                        warn(f"No PBW reader/editor found for source {source_key} on factoid {f.factoidKey}")
                        continue

                    # First get the timestamp on the factoid. Deterministic URI so the same date reuses the same node.
                    factoid_ts = c.make_uri(c.entitylabels['E52'], str(f.creationDate))
                    ts_sparql = f"    {factoid_ts.n3()} a {c.get_label('E52')} ; {c.get_label('P82b')} {Literal(f.creationDate).n3()} .\n"
                    c.update(ts_sparql)

                    # An I16 Meaning Comprehension was P14 carried out by the authority on the given date,
                    # which P16 used specific object the source,
                    # created an I13 Intended Meaning Belief and J5 holds (it) to be true,
                    # J4 that an I4 proposition set which J28 contains entity reference the assertion.
                    # We will give these deterministic names, to aid performance.
                    pset = c.ns[f'proposition_set/pbw{f.factoidKey}'].n3()
                    mbelief = c.ns[f'meaning/pbw{f.factoidKey}'].n3()
                    reading = c.ns[f'reading/pbw{f.factoidKey}'].n3()
                    sparql = f"""
    INSERT DATA {{
            {pset} a {c.get_label('I4')} ;
                {c.get_label('J28')} {', '.join(x.n3() for x in assertions_created)} .
            {mbelief} a {c.get_label('I13')} ;
                {c.get_label('J4')} {pset} ;
                {c.get_label('J5')} {Literal(True).n3()} .
            {reading} a {c.get_label('I16')} ;
                {c.get_label('L11r')} {c.swrun.n3()} ;
                {c.get_label('P4')} {factoid_ts.n3()} ;
                {c.get_label('P14')} {source_reader.n3()} ;
                {c.get_label('P16')} {source_node.n3()} ;
                {c.get_label('J23')} {mbelief} .
    }}"""
                    c.graph.update(sparql)

            if fprocessed > 0:
                print(f"Ingested {fprocessed} {ftype} factoid(s)")
        return True

    def process_persons(self, facttype=None, skipuntil=None, processed=0):
        """Go through the relevant person records and process them for factoids"""
        used_sources = set()
        boulloteria = set()

        # Get the classes of info that are directly in the person record
        direct_person_records = ['Gender', 'Identifier']
        # Get the list of factoid types in the PBW DB
        if facttype is not None:
            factoid_types = [facttype]
            direct_person_records = []
        else:
            factoid_types = [x.typeName for x in self.mysqlsession.query(pbw.FactoidType).all() if
                             x.typeName != '(Unspecified)']
        # Are we skipping?
        started = skipuntil is None
        for person in self.collect_person_records():
            # Get the person's string name
            person_pbwstr = f"{person.name} {person.mdbCode}"
            if not started:
                if skipuntil == f"{person_pbwstr}":
                    print(f"Reached {skipuntil}, resuming")
                    started = True
                else:
                    # print(f"Skipping past {person_pbwstr}")
                    continue

            for attempt in range(5):
                try:
                    result = self._person_process_loop(person, direct_person_records, factoid_types,
                                                       used_sources, boulloteria)
                    if result:
                        processed += 1
                    break
                except (URLError, RemoteDisconnected, ConnectionResetError) as e:
                    if attempt == 4:
                        # RemoteDisconnected has no 'reason' attribute
                        if isinstance(e, URLError):
                            print(f"Persistent URLerror {e.reason}.")
                        else:
                            print(f"Persistent connection error {e}.")
                        self._print_restart_line(person_pbwstr)
                        exit(1)
                    else:
                        if isinstance(e, HTTPError):
                            if 399 < e.code < 500:
                                traceback.print_exc()
                                print(f"Obtained 4xx error; check your SPARQL!", file=sys.stderr)
                                self._print_restart_line(person_pbwstr, sys.stderr)
                                exit(1)
                                print(f"Process started at {self.starttime} and ending at {datetime.now()}.",
                                      file=sys.stderr)
                                print(f'Restart with the arguments: -r "{person_pbwstr}" -x "{self.constants.swrun}"',
                                      file=sys.stderr)
                                exit(1)
                            print(f"Obtained URLerror {e.reason}; will retry")
                        else:
                            print(f"Persistent connection error {e}; will retry")
                        sleep(attempt * 30)
                except Exception as e:
                    self._print_restart_line(person_pbwstr)
                    raise e

        # Make a pass through the authored sources and add viewpoints for all of them
        RELEVEN.author_viewpoints.add_viewpoint_structures(self.constants)

        try:
            self.record_pbw_composite_sources()
            self.record_assertion_factoids()
        except Exception as e:
            self._print_restart_line(file=sys.stderr)
            raise e
        print(f"Processed {processed} person records.")
        print(f"Used the following sources: {sorted(used_sources)}")
        print(f"Used the following boulloterion IDs: {sorted(boulloteria)}")

    def _print_restart_line(self, person_pbwstr=None, file=sys.stdout):
        restart_args = f'-x "{self.constants.swrun}"'
        if person_pbwstr is not None:
            restart_args += f' -r "{person_pbwstr}"'
        print(f'Process started at {self.starttime} and ending at {datetime.now()}.', file=file)
        print(f'Restart with the arguments: {restart_args}"', file=file)



# If we are running as main, execute the script
if __name__ == '__main__':
    # Get the options
    parser = argparse.ArgumentParser(
        prog="graphimportSTAR",
        description="Convert PBW factoids to STAR assertions"
    )
    parser.add_argument('-t', '--testing', action='store_true',
                        help="Run in testing mode with limited data")
    parser.add_argument('-g', '--graph',
                        default=config.graphuri,
                        help="Graph containing existing STAR assertions, if any")
    parser.add_argument('-f', '--factoid-type',
                        default=None,
                        help="Process factoids of the single given type")
    parser.add_argument('-r', '--resume-from',
                        default=None,
                        help="Resume from the named PBW person")
    parser.add_argument('-x', '--execution',
                        default=None,
                        help="Software execution URI for run being resumed")
    args = parser.parse_args()
    # Check that we have an execution if we are resuming
    if args.resume_from is not None and args.execution is None:
        print("Please specify the earlier execution URI to resume the run.")
        exit(1)

    # Process the person records
    gimport = graphimportSTAR(origgraph=args.graph, testmode=args.testing, execution=args.execution)
    print(f"Ingestion run started at {gimport.starttime}")
    gimport.process_persons(facttype=args.factoid_type, skipuntil=args.resume_from)
    # Where are we writing the graph to? Default is the location in config.py
    filename = args.graph
    if args.graph != config.graphuri:
        gimport.g.serialize(args.graph)
    duration = datetime.now() - gimport.starttime
    print("Done! Ran in %s" % str(duration))
