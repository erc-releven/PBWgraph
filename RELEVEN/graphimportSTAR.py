import argparse
import pbw
import RELEVEN.PBWstarConstants
import config
import re
from datetime import datetime
from functools import reduce
from http.client import RemoteDisconnected
from rdflib import Graph, Literal, XSD
from rdflib.plugins.stores import sparqlstore
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from time import sleep
from urllib.error import URLError
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
        return 'UncertainIdent'
    return label


def _matchid(var, val):
    return 'MATCH (%s) WHERE %s.uuid = "%s" ' % (var, var, val)


def _get_single_key(rdfresult, k):
    if len(rdfresult) == 0:
        return None
    if len(rdfresult) > 1:
        warn(f"Query result had multiple rows!")
    for row in rdfresult:
        return row[k]


def _get_source_lang(dbobj):
    """Returns the RDF language tag for any object that has an `oLangKey` property"""
    lkeys = {2: 'grc', 3: 'la', 4: 'ar', 5: 'xcl'}
    try:
        return lkeys.get(dbobj.oLangKey)
    except NameError:
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
        """Create the SPARQL query that corresponds to an assertion with the given parameters.
           Note that the object might be a list of literals."""
        # Get the predicates for the STAR assertion in question

        # Try to optimize this by putting the subject or object first, if it is a literal
        c = self.constants
        subject = subj
        objectfirst = None
        if hasattr(subj, 'n3'):
            subject = subj.n3()
            objectfirst = False
        sparqlsubj = f"{c.star_subject} {subject} ;"

        # Deal with the object
        sparqlobj = ""
        if isinstance(obj, list):
            # Ensure that the list are all literals
            disallowed = [x for x in obj if not isinstance(x, Literal)]
            if len(disallowed):
                warn("Object can only be a list if it is a list of literals")
                return None
            objectfirst = True
            sparqlobj = f"{c.star_object} " + ",".join([o.n3() for o in obj]) + " ;"
        else:
            objec_t = obj
            if hasattr(obj, 'n3'):
                objec_t = obj.n3()
                objectfirst = True
            sparqlobj += f"{c.star_object} {objec_t} ;"

        # Construct the SPARQL
        first, second = (sparqlobj, sparqlsubj) if objectfirst else (sparqlsubj, sparqlobj)
        sparql = f"""
        ?{label} {first}
            {second}
            a {c.get_assertion_for_predicate(ptype)} ;
"""

        # The assertion might or might not have a source
        if src:
            provenance = src
            if hasattr(src, 'n3'):
                provenance = src.n3()
            sparql += f"            ^{c.star_src} {provenance} ; \n"
        # ...and it might or might not be based on something
        if based:
            evidence = based
            if hasattr(based, 'n3'):
                evidence = based.n3()
            sparql += f"            {c.star_based} {evidence} ; \n"
        # ...but it should always have an authority.
        authority = auth
        if hasattr(auth, 'n3'):
            authority = auth.n3()
        sparql += f"""            {c.star_auth} {authority} .
        """
        return sparql

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
            sparql = self.create_assertion_sparql('a1', 'P41', '?gass', graphperson, c.pbw_agent)
            sparql += self.create_assertion_sparql('a2', 'P42', '?gass', c.get_gender(pbw_sex), c.pbw_agent)
            sparql += f"?gass a {c.get_label('E17')} . "
            # Check and create it if necessary
            res = c.ensure_entities_existence(sparql)
            c.document(pbwdoc, res['a1'], res['a2'])

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
        sparql = self.create_assertion_sparql('a1', 'P1', graphperson, '?appellation', c.pbw_agent)
        sparql += f"""?appellation a {c.get_label('E41')} ;
            {c.get_label('P190')} {Literal(appellation, lang=_get_source_lang(sqlperson)).n3()} .
        """
        # Check and create it if necessary
        res = c.ensure_entities_existence(sparql)
        c.document(pbwdoc, res['a1'])

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
        return self.get_authority_node(list(alist.values()))

    def get_boulloterion(self, boulloterion, pbweditor):
        """Helper function to find a boulloterion with a given ID. Creates its seals and sources
        if it is a new boulloterion. Returns the boulloterion and its inscription."""
        c = self.constants
        pbwdoc = c.pbw_uri(boulloterion)
        # boulloterion is a subclass of E22 Human-Made Object, with an identifier assigned by PBW
        keystr = str(boulloterion.boulloterionKey)
        btitle = f"Boulloterion of {boulloterion.title}"
        boul_node = self.find_or_create_boulloterion(keystr, btitle)
        # See if the boulloterion already exists with an inscription. Be sure to document it if so
        sparql_check = self.create_assertion_sparql('a1', 'P128', boul_node, '?inscription', pbweditor)
        res = self.g.query("SELECT ?a1 ?inscription WHERE { " + sparql_check + "}")
        if len(res):
            c.document(pbwdoc, _get_single_key(res, 'a1'))
            return boul_node, _get_single_key(res, 'inscription')

        # If the boulloterion does not yet have an assertion that it carries any inscription, it
        # will need to be created with its inscription, its seals, and its source list
        # Get the sources that PBW used for this boulloterion, if any
        source_node = self.get_boulloterion_sourcelist(boulloterion)

        # Make the assertion(s) concerning its inscription. Mark the inscription explicitly as E33 as well as
        # E34, since we can't always rely on having inferencing
        sparql = f"""
        ?inscription {c.get_label('P190')} {Literal(boulloterion.origLText, lang=_get_source_lang(boulloterion)).n3()} ;
            a {c.get_label('E34')}, {c.get_label('E33')} . """
        sparql += self.create_assertion_sparql('a', 'P128', boul_node, '?inscription', pbweditor, based=source_node)

        # Create the seals that belong to this boulloterion; assert that they
        # belong to their collection and that they came from this boulloterion.
        for i, seal in enumerate(boulloterion.seals):
            coll = self.find_or_create_seal_collection(seal.collection.collectionName)
            # Is it one of the happy items with a collection URL that we can construct?
            seal_link = ''
            if seal.collection.baseURL is not None:
                seal_uri = seal.collection.baseURL + seal.collectionKey
                if 'doaks' in seal_uri:  # only DOaks URIs are recoverable directly from the database
                    seal_link = f"\n            {c.link_n3} <{seal_uri}> ;"
            # Make an ID unique for our purposes
            seal_id = "%d-%d-%d" % (seal.collectionKey, seal.collectionRef, seal.sealKey)
            sparql += f"""
        ?seal{i} {c.label_n3} {Literal(seal_id).n3()} ;{seal_link}
            a {c.get_label('E22S')} . """
            sparql += self.create_assertion_sparql(f"a{i}c", 'P46', coll, f'?seal{i}', pbweditor, src=source_node)
            sparql += self.create_assertion_sparql(f"a{i}b", 'L1', boul_node, f'?seal{i}', pbweditor, based=source_node)

        # Possible optimization: We have already established that this boulloterion (and therefore the
        # inscription and seals) don't exist yet, so just run the statement as an update
        # For now, do it the slow way
        res = c.ensure_entities_existence(sparql)
        # Get the documentation
        assertionkeys = ['a']
        assertionkeys.extend([f'a{n}c' for n in range(len(boulloterion.seals))])
        assertionkeys.extend([f'a{n}b' for n in range(len(boulloterion.seals))])
        c.document(pbwdoc, *[res[x] for x in assertionkeys])
        # Return the boulloterion and inscription
        return boul_node, res['inscription']

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
            # short_name = source.shortName if source.bibKey == 816 else re_encode(source.shortName)
            latin_bib = source.latinBib if source.bibKey == 816 else re_encode(source.latinBib)
            # TODO work in the shortName?
            sn = f"""
        ?src {c.label_n3} {Literal(latin_bib).n3()} ;
            a {c.get_label('F2P')} .
        """
            res = c.ensure_entities_existence(sn)
            source_nodes.append(res['src'])
        if len(source_nodes) > 1:
            # Find or create a matching bibliography/publication list with only these publication nodes.
            return c.ensure_egroup_existence('E73B', 'P165', source_nodes)

        else:
            # There was only a single source. We just return it.
            return source_nodes[0]

    def get_text_authority(self, fsource):
        """Return the authority (either a text author or someone else, e.g. the editor of the print edition) for the
        source behind this factoid."""
        # Do we have a known author for this text?
        author = self.get_author_node(self.constants.author(fsource))
        # If not, we use the editor(s) as the authority.
        editor = self.get_authority_node(self.constants.editor(fsource))
        # And if that doesn't exist, we use the PBW authority.
        agent = self.get_authority_node(self.constants.authorities(fsource))
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
        sourcekey = self.constants.source(factoid)
        agent = self.get_authority_node(self.constants.authorities(sourcekey))
        sparql = f"""
        ?sourceref {c.get_label('P190')} {Literal(factoid.origLDesc).n3()} ;
            {c.label_n3} {Literal(c.sourceref(factoid)).n3()} ;
            a {c.get_label('E33')} . """
        sparql += self.create_assertion_sparql('a', 'R15', wholesource, '?sourceref', agent)
        res = c.ensure_entities_existence(sparql)
        c.document(pbwdoc, res['a'])
        return res['sourceref']

    def get_source_work_expression(self, factoid):
        # Ensure the existence of the work and, if it has a declared author, link the author to it via
        # a CREATION event, asserted by the author.
        c = self.constants
        sourcekey = c.source(factoid)
        workinfo = c.sourceinfo(sourcekey)
        pbw_authority = self.get_authority_node(c.authorities(sourcekey))
        editors = self.get_authority_node(workinfo.get('editor'))
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

        # Keep track of the assertions we may have created
        assertions_set = []
        afact_src = None
        # Express the edition/publication
        sparql = f"""
        ?publ {c.label_n3} {Literal(edition_id).n3()} ;
            a {c.get_label('F2P')} . """

        if text_id is None:
            # We are dealing with a secondary source. Assert a publication creation instead of a
            # text (expression) creation, with the editors; we will have to go back later and say that this
            # depended on another work (the primary source).
            sparql += self.create_assertion_sparql('a1', 'R17', '?ec', '?publ', editors, '?publ')
            sparql += self.create_assertion_sparql('a2', 'P14', '?ec', editors, editors, '?publ')
            sparql += f"""
        ?ec a {c.get_label('F28')} . """
            assertions_set.extend(['a1', 'a2'])
        else:
            # We are dealing with a primary source, so we need to make a bunch of assertions.
            # First, the editors assert that the edition (that is, the publication) belongs to
            # the work; the source for this is, well, the edition.
            sparql += f"""
        ?work {c.label_n3} {Literal(text_id).n3()} ;
            a {c.get_label('F2T')} . """
            sparql += self.create_assertion_sparql('a1', 'R76', '?publ', '?work', editors, '?publ')
            assertions_set.append('a1')
            # Now we need to see if authorship has to be asserted.
            author = self.get_author_node(self.constants.author(sourcekey))
            if author is not None:
                # Make the assertions that the author authored the work. If we have a factoid,
                # then the authority for this assertion is the factoid's primary referent.
                # Otherwise, the authority (for now) is the editor.
                # If we don't have a specific reference for the claim, we just use the edition (again).
                aship_authority = editors
                aship_source = '?publ'
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
                    # We have to make a sourceref node, connected to this text, for
                    # the factoid source.
                    sparql += f"""
        ?srcref {c.get_label('P190')} {Literal(afact.origLDesc, lang=_get_source_lang(afact)).n3()} ;
            {c.label_n3} {Literal(afact.sourceRef).n3()} ;
            a {c.get_label('E33')} . """
                    aship_source = '?srcref'
                elif 'provenance' in workinfo:
                    # We have a page number. This makes our authorship authority the editor(s), with the source
                    # being the passage in this very edition.
                    sparql += f"""
        ?srcref {c.label_n3} {Literal(workinfo['provenance']).n3()} ;
            a {c.get_label('E33')} . """
                    aship_source = '?srcref'

                if aship_source == '?srcref':
                    # It is the PBW editor who says that a particular passage exists and belongs to the respective
                    # edition. We have no further source or evidence of this, apart from the DB record that will get
                    # documented via an E31 link.
                    # n.b. We will need to fix/change this manually for non-factoid provenance!
                    sparql += self.create_assertion_sparql('a2', 'R15', '?publ', aship_source, pbw_authority)
                    assertions_set.append('a2')

                # We have now dealt with extracting information from some relevant authorship factoid, if it exists.
                # Move on to the assertion that the author authored the work
                sparql += self.create_assertion_sparql('a3', 'R17', '?wc', '?work', aship_authority, aship_source)
                sparql += self.create_assertion_sparql('a4', 'P14', '?wc', author, aship_authority, aship_source)
                sparql += f"""
        ?wc a {c.get_label('F28')} . """
                assertions_set.extend(['a3', 'a4'])

        # Whatever we just made, return the edition/publication, which is what we are after.
        res = c.ensure_entities_existence(sparql)

        # Do a backwards-compatible post-hoc addition of the work's internal identifier for reconciliation
        if 'work' in res:
            c.graph.add((res['work'], c.entity_label, Literal(sourcekey)))
        # Document the assertions we just made
        if afact_src:
            # It comes from an authorship factoid
            c.document(afact_src, *[res[x] for x in assertions_set])
        else:
            # It comes from our reading of the database structure
            c.document(None, *[res[x] for x in assertions_set])
        return res['publ']

    def _find_or_create_identified_entity(self, etype, agent, identifier, dname):
        """Return an identified entity URIRef. This can be a Boulloterion (E22 subclass) or an E21 Person
        depending on context. It is labeled with the identifier via an E15 Identifier Assignment
        carried out by the given agent, with dname becoming our preferred human-readable identifier."""
        c = self.constants
        if etype == c.get_label('E22B'):
            # Identifier is a number
            url = f'https://pbw2016.kdl.kcl.ac.uk/boulloterion/{identifier}/'
        elif agent == c.pbw_agent:
            # Identifier is something like 'Alexios 10102' or 'Alp Arslan 51'.
            # The URL changes it to 'Alexios/10102' or 'Alp%20Arslan/51'
            uf = identifier.split(' ')
            f"{'%20'.join(uf[:-1])}/{uf[-1]}"
            url = f'https://pbw2016.kdl.kcl.ac.uk/person/{' '.join(uf[:-1])}/{uf[-1]}/'
        else:
            # Identifier is a number
            url = f'https://viaf.org/viaf/{identifier}/'

        # The entity should have its display name as its label, without a language designation.
        #
        entitystr = f"?entity a {etype} "
        if dname is not None:
            entitystr += f";\n            {c.label_n3} {Literal(dname).n3()} "
        entitystr += '.'

        # Construct the identifier assignment that should exist.
        # This identifier is marked sameAs the identifier we are copying from.
        sparql = f"""
        ?ident {c.get_label('P190')} {Literal(identifier).n3()} ;
            {c.link_n3} <{url}> ;
            a {c.get_label('E42')} .
        ?idass {c.get_label('P37')} ?ident ;
            {c.star_subject} ?entity ;
            {c.star_auth} {agent.n3()} ;
            a {c.get_label('E15')} .
        {entitystr}
        """

        # Ensure its existence and return the entity in question
        res = c.ensure_entities_existence(sparql)
        c.document(None, res['idass'])
        return res['entity']

    def find_or_create_pbwperson(self, sqlperson):
        return self._find_or_create_identified_entity(
            self.constants.get_label('E21'), self.constants.pbw_agent,
            "%s %d" % (sqlperson.name, sqlperson.mdbCode), sqlperson.descName)

    def find_or_create_viafperson(self, name, viafid):
        return self._find_or_create_identified_entity(
            self.constants.get_label('E21'), self.constants.viaf_agent, viafid, name)

    def find_or_create_boulloterion(self, keystr, btitle):
        return self._find_or_create_identified_entity(
            self.constants.get_label('E22B'), self.constants.pbw_agent, keystr, btitle)

    # This one doesn't use an E15 assertion, it is just a thing with a name
    def find_or_create_seal_collection(self, collname):
        c = self.constants
        sparql = f"""
        ?collection a {c.get_label('E78')} ;
            {c.label_n3} {Literal(collname).n3()} .
        """
        res = c.ensure_entities_existence(sparql)
        return res['collection']

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
                authors.append(self.find_or_create_viafperson(pname, pcode))
            else:
                # We need to get the SQL record for the author in case they aren't in the DB yet
                sqlperson = self.mysqlsession.query(pbw.Person).filter_by(name=pname, mdbCode=pcode).scalar()
                authors.append(self.find_or_create_pbwperson(sqlperson))
        if len(authors) > 1:
            # It is our multi-authored text. Make a group because both authors share authority.
            return self._find_or_create_authority_group(authors)
        else:
            return authors[0]

    def get_authority_node(self, authoritylist):
        """Create or find the node for the given authority in our (modern) authority list."""
        if authoritylist is None or len(authoritylist) == 0:
            return None
        if len(authoritylist) == 1:
            authority = authoritylist[0]
            return self.find_or_create_viafperson(authority['identifier'], authority['viaf'])
        # If we get here, we have more than one authority for this source.
        # Ensure the existence of the people, and then ensure the existence of their group
        scholars = []
        for p in authoritylist:
            scholars.append(self.find_or_create_viafperson(p['identifier'], p['viaf']))
        return self._find_or_create_authority_group(scholars)

    def _find_or_create_authority_group(self, members):
        if len(members) < 2:
            warn(f"Tried to create authority group with {len(members)} member(s)!")
            return None

        c = self.constants
        return c.ensure_egroup_existence('E74A', 'P107', members)

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

        sparql = f"""
        ?appel {c.get_label('P190')} {Literal(name_ol, olang).n3()} ;
            {c.get_label('P190')} {Literal(name_en, 'en').n3()} ;
            a {c.get_label('E41')} . """
        sparql += self.create_assertion_sparql('a1', 'P1', graphperson, '?appel', agent, sourcenode)
        res = c.ensure_entities_existence(sparql)
        c.document(pbwdoc, res['a1'])

    def description_handler(self, sourcenode, agent, factoid, graphperson):
        """Record the descriptions given in the sources as P3 data-property assertions."""
        c = self.constants
        pbwdoc = c.pbw_uri(factoid)

        olang = _get_source_lang(factoid) or 'grc'
        descriptions = [Literal(factoid.replace_referents(), 'en'), Literal(factoid.origLDesc, olang)]
        sparql = self.create_assertion_sparql('a1', 'P3', graphperson, descriptions, agent, sourcenode)
        res = c.ensure_entities_existence(sparql)
        c.document(pbwdoc, res['a1'])

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
            return

        # Set up the death event. We take for granted that every 11th-c. person has exactly one death,
        # so we set this event separately and we don't bother with source or authority, as those will be
        # attached to the date/place/etc. of the event.
        de_query = f"""
        ?a0 {c.star_object} {graphperson.n3()} ;
            a {c.get_assertion_for_predicate('P100')} ;
            {c.star_subject} ?de .
        ?de a {c.get_label('E69')} .
        """
        res = c.ensure_entities_existence(de_query)
        c.document(pbwdoc, res['a0'])
        deathevent = res['de']
        # For ease of understanding we should give the death event a label with the person's PBW identifier.
        # Add the label if it doesn't already exist, in a backwards-compatible way
        pbwperson = factoid.main_person()[0]
        person_pbwid = f"{pbwperson.name} {pbwperson.mdbCode}"
        if c.graph.value(deathevent, c.predicates['P3']) is None:
            c.graph.add((deathevent, c.predicates['P3'], Literal("Death event for " + person_pbwid)))

        # Get the description of the death in English and the original language
        olang = _get_source_lang(factoid) or 'grc'
        descriptions = [Literal(factoid.replace_referents(), 'en'), Literal(factoid.origLDesc, olang)]
        sparql = self.create_assertion_sparql('a1', 'P3', deathevent, descriptions, agent, sourcenode)

        # See if the death is dated and, if so, add the date
        deathdate = factoid.deathRecord.sourceDate
        if deathdate:
            sparql += f"""
        ?deathdate a {c.get_label('E52')} ;
            {c.get_label('P80')} {Literal(deathdate).n3()} . """
            sparql += self.create_assertion_sparql('a2', 'P4', deathevent, '?deathdate', agent, sourcenode)

        res = c.ensure_entities_existence(sparql)
        c.document(pbwdoc, res['a1'])
        if deathdate:
            c.document(pbwdoc, res['a2'])

    def ethnicity_handler(self, sourcenode, agent, factoid, graphperson):
        """Assign a group membership for the given ethnicity to the person"""
        c = self.constants
        pbwdoc = c.pbw_uri(factoid)
        if factoid.ethnicityInfo is None or factoid.ethnicityInfo.ethnicity is None:
            # We can't assign any ethnicity without the ethnicity info
            warn("Empty ethnicity factoid found: id %s" % factoid.factoidKey)
            return
        elabel = factoid.ethnicityInfo.ethnicity.ethName
        groupid = c.get_ethnicity(elabel)
        sparql = self.create_assertion_sparql('a1', 'P107', groupid, graphperson, agent, sourcenode)
        res = c.ensure_entities_existence(sparql)
        c.document(pbwdoc, res['a1'])

    # Helper to create the assertions for our various social designation groups
    def _find_or_create_social_designation(self, sourcenode, agent, factoid, graphperson, des, label, whopred,
                                           whichpred):
        # (grouping:label) [:whopred] person
        # (grouping) [:whichpred] rnode
        c = self.constants
        pbwdoc = c.pbw_uri(factoid)
        # We will sometimes have duplicate assertions originating from different geographic
        # scopes, which we have chopped off in the title name. For now we keep these duplicate.
        # TODO we should add the geographic scopes to these when we can...
        sparql = self.create_assertion_sparql('a1', whopred, '?designation', graphperson, agent, sourcenode)
        sparql += self.create_assertion_sparql('a2', whichpred, '?designation', des, agent, sourcenode)
        sparql += f"?designation a {label} . \n"
        res = c.ensure_entities_existence(sparql)

        # Document it in either case as coming from this factoid
        c.document(pbwdoc, res['a1'], res['a2'])

    def religion_handler(self, sourcenode, agent, factoid, graphperson):
        """Assign a group membership for the given religious confession to the person"""
        if factoid.religion is None:
            warn("Empty religion factoid found: id %d" % factoid.factoidKey)
            return
        rlabel = factoid.religion
        # Special case, database had an error
        if factoid.religion == '':
            rlabel = 'Heretic'
        relid = self.constants.get_religion(rlabel)
        # (r:C23 Religious identity) [rwho:P36 pertains to] person
        # (r:C23 Religious identity) [rwhich:P35 is defined by] rnode
        self._find_or_create_social_designation(sourcenode, agent, factoid, graphperson, relid,
                                                self.constants.get_label('C23'), 'SP36', 'SP35')

    def societyrole_handler(self, sourcenode, agent, factoid, graphperson):
        if factoid.occupation is None:
            return
        roleid, roleclass = self.constants.get_societyrole(factoid.occupation)
        roletype = self.constants.get_label('C1')
        whopred = 'SP13'
        whichpred = 'SP14'
        if roleclass == self.constants.get_label('C12'):
            # We need to treat it as a legal role instead of a social role / occupation
            roletype = self.constants.get_label('C13')
            whopred = 'SP26'
            whichpred = 'SP33'
        # (r:C1 Social Quality of an Actor) [rwho:P13 pertains to] person
        # (r:C1) [rwhich:P14 is defined by] rnode
        self._find_or_create_social_designation(sourcenode, agent, factoid, graphperson, roleid, roletype,
                                                whopred, whichpred)

    def dignity_handler(self, sourcenode, agent, factoid, graphperson):
        if factoid.dignityOffice is None:
            return
        dignity_id, dignity_class = self.constants.get_dignity(factoid.dignityOffice.stdName)
        roletype = self.constants.get_label('C13')
        whopred = 'SP26'
        whichpred = 'SP33'
        if dignity_class == self.constants.get_label('C2'):
            # We need to treat it as a social instead of a legal role
            roletype = self.constants.get_label('C1')
            whopred = 'SP13'
            whichpred = 'SP14'
        # We treat (most) dignities as legal roles
        # (r:C13 Social Role Embodiment) [dwho:P26 is embodied by] person
        # (r:C13) [dwhich:P33 is embodiment of] dignity
        self._find_or_create_social_designation(sourcenode, agent, factoid, graphperson, dignity_id, roletype,
                                                whopred, whichpred)

    def languageskill_handler(self, sourcenode, agent, factoid, graphperson):
        """Assign a language skill to the person"""
        c = self.constants
        pbwdoc = c.pbw_uri(factoid)
        if factoid.languageSkill is None:
            return
        # Language know-how ID
        lkhid = self.constants.get_language(factoid.languageSkill)
        # This doesn't chain quite the same way as the others do
        # person [rwho:P38 has skill] (r:C21 Skill)
        # (r:C21 Skill) [rwhich:P37 concerns] (l:C29 Know-How)
        sparql = self.create_assertion_sparql('a1', 'SP38', graphperson, '?lskill', agent, sourcenode)
        sparql += self.create_assertion_sparql('a2', 'SP37', '?lskill', lkhid, agent, sourcenode)
        sparql += f"""
        ?lskill a {c.get_label('C21')} ."""
        res = c.ensure_entities_existence(sparql)
        c.document(pbwdoc, res['a1'], res['a2'])

    def _find_or_create_kinship(self, graphperson, graphkin):
        # See if there is an existing kinship group of any sort with the person as source and their
        # kin as target. If not, return a new (not yet connected) C3 Social Relationship node.
        c = self.constants
        # The pattern to match. TODO maybe we want to add the kinship type to this search pattern?
        sparql_check = f"""select distinct ?kstate where {{
        ?a1 {c.star_object} {graphperson.n3()} ;
            a {c.get_assertion_for_predicate('SP17')} ;
            {c.star_subject} ?kstate .
        ?a2 {c.star_object} {graphkin.n3()} ;
            {c.star_subject} ?kstate ;
            a {c.get_assertion_for_predicate('SP18')} .
        ?kstate a {c.get_label('C3')} .}}
"""
        res = self.g.query(sparql_check)
        if len(res):
            # We found a kinship between these two people. Return it
            return _get_single_key(res, 'kstate')
        else:
            return None

    def kinship_handler(self, sourcenode, agent, factoid, graphperson):
        # These are social relationships as opposed to social roles, so they need a partner.
        # (rel:C3 Social Relationship) [pt:P16 has type] (kt:C4 Kinship type)
        # (rel) [src:P17 has source] (p:E21 person)
        # (rel) [trg:P18 has target] (p:E21 kin)
        c = self.constants
        pbwdoc = c.pbw_uri(factoid)
        if factoid.kinshipType is None:
            warn("Empty kinship factoid found: id %d" % factoid.factoidKey)
            return
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
            # Look for any existing kinship state between these two persons
            kstate = self._find_or_create_kinship(graphperson, graphkin)
            if kstate is None:
                # We will then have to refer to it by variable name instead of URIref.
                kstate = '?kstate'
            # Now set up the three kinship assertions
            sparql = self.create_assertion_sparql('a1', 'SP16', kstate, ktype, agent, sourcenode)
            sparql += self.create_assertion_sparql('a2', 'SP17', kstate, graphperson, agent, sourcenode)
            sparql += self.create_assertion_sparql('a3', 'SP18', kstate, graphkin, agent, sourcenode)
            if kstate is None:
                # Make one if it doesn't exist.
                sparql += f"        ?kstate a {c.get_label('C3')} ."
            res = c.ensure_entities_existence(sparql)
            c.document(pbwdoc, res['a1'], res['a2'], res['a3'])

    def possession_handler(self, sourcenode, agent, factoid, graphperson):
        """Ensure the existence of an E18 Physical Thing (we don't have any more category info about
        the possessions). For now, we assume that a possession with an identical description is, in fact,
        the same possession."""
        c = self.constants
        pbwdoc = c.pbw_uri(factoid)

        # Give the possession its description, which comes out of the factoid's engDesc
        sparql = f"""
        ?possession {c.label_n3} {Literal(factoid.replace_referents()).n3()} ;
            a {c.get_label('E18')} ."""
        # Assert ownership of the possession
        sparql += self.create_assertion_sparql('a1', 'P51', '?possession', graphperson, agent, sourcenode)
        if factoid.possession is not None and factoid.possession != '':
            # Give the assertion a note, if such note exists in the poorly-named PossessionFactoid.possessionName field
            sparql += f"""
        ?a1 {c.get_label('P3')} {Literal(factoid.possession).n3()}"""
        res = c.ensure_entities_existence(sparql)
        c.document(pbwdoc, res['a1'])

    def record_assertion_factoids(self):
        """To be run after everything else is done. Creates the assertion record for all assertions created here,
        tying each to the factoid or person record that originated it and tying all the assertion records to the
        database creation event."""
        c = self.constants

        # Find all assertions that have been marked as coming from this software run. We will add the
        # forward property to the ones that don't yet have a forward property. We can keep the reverse property
        # as a 'touched by' indicator, or we can delete it.
        sparql_criteria = f"""
            ?a {c.get_label('L11r')} {c.swrun.n3()} .
        MINUS {{
            ?l {c.get_label('L11')} ?a .
        }}
        """
        res = self.g.query(f"SELECT (COUNT(?a) AS ?act) WHERE {{ {sparql_criteria} }}")
        num_new = 0
        for row in res:  # there is only one row
            num_new = row['act'].toPython()
        if num_new > 0:
            print(f"Recording {num_new} new assertions in the graph.")
            # Add the ending timestamp to the execution we have
            tstamp = self.g.value(c.swrun, c.predicates['P4'])
            timenow = datetime.now()
            self.g.add((tstamp, c.predicates['P82b'], Literal(timenow, datatype=XSD.dateTimeStamp)))

            # Add the responsible person. TODO this should have more options than just tla
            tla = self.get_authority_node([self.constants.ta])
            self.g.add((c.swrun, c.predicates['P14'], tla))

            # Put in the forward predicate. LATER delete the reverse predicate if we decide it's a good idea
            sparql_update = f"""
        INSERT {{
            {c.swrun.n3()} {c.get_label('L11')} ?a .
        }} WHERE {{
            {sparql_criteria}
        }}
            """
            c.graph.update(sparql_update)
        else:
            print("No new assertions created on this run.")

    def _person_process_loop(self, person, direct_person_records, factoid_types, used_sources, boulloteria):
        # Skip the anonymous groups for now
        if person.name == 'Anonymi':
            return
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
                # Find out what sources we are actually using and make note of them
                source_key = self.constants.source(f)
                if source_key is None:
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
                    authority_node = self.constants.pbw_agent
                # Call the handler for this factoid type
                method(source_node, authority_node, f, graph_person)
                fprocessed += 1
            if fprocessed > 0:
                print(f"Ingested {fprocessed} {ftype} factoid(s)")
        return True

    def process_persons(self, skipuntil=None, processed=0):
        """Go through the relevant person records and process them for factoids"""
        used_sources = set()
        boulloteria = set()

        # Get the classes of info that are directly in the person record
        direct_person_records = ['Gender', 'Identifier']
        # Get the list of factoid types in the PBW DB
        factoid_types = [x.typeName for x in self.mysqlsession.query(pbw.FactoidType).all() if
                         x.typeName != '(Unspecified)']
        # Are we skipping?
        started = skipuntil is None
        for person in self.collect_person_records():
            # Get the person's string name
            person_pbwstr = f"{person.name} {person.mdbCode}"
            if not started:
                if skipuntil == f"{person_pbwstr}":
                    started = True
                else:
                    print(f"Skipping past {person_pbwstr}")
                    continue

            for attempt in range(5):
                try:
                    result = self._person_process_loop(person, direct_person_records, factoid_types,
                                                       used_sources, boulloteria)
                    if result:
                        processed += 1
                    break
                except (URLError, RemoteDisconnected) as e:
                    if attempt == 4:
                        # RemoteDisconnected has no 'reason' attribute
                        if isinstance(e, URLError):
                            print(f"Persistent URLerror {e.reason}.")
                        else:
                            print(f"Persistent connection error {e}.")
                        print(f"Process started at {self.starttime} and ending at {datetime.now()}.")
                        print(f"Restart with the arguments: -r '{person_pbwstr}' -x '{self.constants.swrun}'")
                        exit(1)
                    else:
                        print(f"Obtained URLerror {e.reason}; will retry")
                        sleep(attempt * 30)
                except Exception as e:
                    print(f"Process started at {self.starttime} and ending at {datetime.now()}.")
                    print(f"Restart with the arguments: -r '{person_pbwstr}' -x '{self.constants.swrun}'")
                    raise e

        self.record_assertion_factoids()
        print(f"Processed {processed} person records.")
        print(f"Used the following sources: {sorted(used_sources)}")
        print(f"Used the following boulloterion IDs: {sorted(boulloteria)}")


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
    gimport.process_persons(skipuntil=args.resume_from)
    # Where are we writing the graph to? Default is the location in config.py
    filename = args.graph
    if args.graph != config.graphuri:
        gimport.g.serialize(args.graph)
    duration = datetime.now() - gimport.starttime
    print("Done! Ran in %s" % str(duration))
