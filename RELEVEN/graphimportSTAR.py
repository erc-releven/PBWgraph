import argparse
import pbw
import RELEVEN.PBWstarConstants
import config
import re
from datetime import datetime
from rdflib import Graph, Literal, XSD
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
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


def _get_source_lang(factoid):
    lkeys = {2: 'grc', 3: 'la', 4: 'ar', 5: 'xcl'}
    try:
        return lkeys.get(factoid.oLangKey)
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

    def __init__(self, origgraph=None, testmode=False):
        # Set the testing flag
        self.testmode = testmode
        # Record the starting time
        self.starttime = datetime.now()
        # Connect to the SQL DB
        engine = create_engine('mysql+mysqlconnector://' + config.dbstring)
        smaker = sessionmaker(bind=engine)
        self.mysqlsession = smaker()
        # Start an RDF graph, parsing what we started with
        self.g = Graph()
        if origgraph is not None:
            self.g.parse(origgraph)
        # Make / retrieve the global nodes and self.constants
        self.constants = RELEVEN.PBWstarConstants.PBWstarConstants(self.g)

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

    def create_assertion_sparql(self, label, ptype, subj, obj, auth, src=None):
        """Create the SPARQL query that corresponds to an assertion with the given parameters.
           Note that the object might be a list of literals."""
        c = self.constants
        subject = subj
        if hasattr(subj, 'n3'):
            subject = subj.n3()
        sparql = f"""
        ?{label} a {c.get_assertion_for_predicate(ptype)} ;
            {c.star_subj_l} {subject} ;
        """
        # Deal with the object
        if type(obj) == list:
            # Ensure that the list are all literals
            disallowed = [x for x in obj if not isinstance(x, Literal)]
            if len(disallowed):
                warn("Object can only be a list if it is a list of literals")
                return None
            for o in obj:
                sparql += f"    {c.star_obj_l} {o.n3()} ;\n"
        else:
            objec_t = obj
            if hasattr(obj, 'n3'):
                objec_t = obj.n3()
            sparql += f"    {c.star_obj_l} {objec_t} ;\n"
        # The assertion might or might not have a source
        if src:
            basis = src
            if hasattr(src, 'n3'):
                basis = src.n3()
            sparql += f"            {c.star_based_l} {basis} ; \n"
        # ...but it should always have an authority.
        authority = auth
        if hasattr(auth, 'n3'):
            authority = auth.n3()
        sparql += f"""            {c.star_auth_l} {authority} .
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
            sparql = f"""
            ?gass a {c.get_label('E17')} . """
            sparql += self.create_assertion_sparql('a1', 'P41', '?gass', graphperson, c.pbw_agent)
            sparql += self.create_assertion_sparql('a2', 'P37', '?gass', c.get_gender(pbw_sex), c.pbw_agent)
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

        # Create the SPARQL expression
        sparql = f"""
        ?appellation a crm:E41 ;
            crm:P190 {Literal(appellation, lang='en').n3()} .
        """
        sparql += self.create_assertion_sparql('a1', 'P1', graphperson, '?appellation', c.pbw_agent)
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
        # See if the boulloterion already exists with an inscription
        sparql_check = self.create_assertion_sparql('a1', 'P128', boul_node, '?inscription', pbweditor)
        res = self.g.query("SELECT ?inscription WHERE { " + sparql_check + "}")
        if len(res):
            return boul_node, res['inscription']

        # If the boulloterion does not yet have an assertion that it carries any inscription, it
        # will need to be created with its inscription, its seals, and its source list
        # Get the sources that PBW used for this boulloterion, if any
        source_node = self.get_boulloterion_sourcelist(boulloterion)

        # Make the assertion(s) concerning its inscription. TODO for the cleanup change E73 to E33
        sparql = f"""
        ?inscription a {c.get_label('E34')}, {c.get_label('E73')} ;
            {c.get_label('P190')} {Literal(boulloterion.origLText, _get_source_lang(boulloterion.origLang)).n3()} . """
        sparql += self.create_assertion_sparql('a', 'P128', boul_node, '?inscription', pbweditor, source_node)

        # Create the seals that belong to this boulloterion; assert that they
        # belong to their collection and that they came from this boulloterion.
        for i, seal in enumerate(boulloterion.seals):
            coll = self.find_or_create_seal_collection(seal.collection.collectionName)
            seal_id = "%d.%d.%d" % (seal.collectionKey, seal.boulloterionKey, seal.collectionRef)
            sparql += f"""
        ?seal{i} a {c.get_label('E22S')} ;
            {c.get_label('P3')} "{seal_id}" . """
            sparql += self.create_assertion_sparql(f"?a{i}c", 'P46', coll, f'?seal{i}', pbweditor)
            sparql += self.create_assertion_sparql(f"?a{i}b", 'L1', boul_node, f'?seal{i}', pbweditor)

        # Possible optimization: We have already established that this boulloterion (and therefore the
        # inscription and seals) don't exist yet, so just run the statement as an update
        # For now, do it the slow way
        res = c.ensure_entities_existence(sparql)
        # Get the documentation
        assertionkeys = ['a']
        assertionkeys.extend([f'a{n}c'] for n in range(len(boulloterion.seals)))
        assertionkeys.extend([f'a{n}b'] for n in range(len(boulloterion.seals)))
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
            short_name = source.shortName if source.bibKey == 816 else re_encode(source.shortName)
            latin_bib = source.latinBib if source.bibKey == 816 else re_encode(source.latinBib)
            sn = f"""
            ?src a {c.get_label('F3')} ;
                {c.get_label('P1')} {Literal(short_name).n3()} ;
                {c.get_label('P3')} {Literal(latin_bib).n3()} .
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
        ?sourceref a {c.get_label('E33')} ;
            {c.get_label('P3')} '{escape_text(c.sourceref(factoid))}' ;
            {c.get_label('P190')} '{escape_text(factoid.origLDesc)}' . """
        sparql += self.create_assertion_sparql('a', 'R15', wholesource, '?sourceref', agent)
        res = c.ensure_entities_existence(sparql)
        c.document(pbwdoc, res['a'])
        return res['sourceref']

    def get_source_work_expression(self, factoid):
        # Ensure the existence of the work and, if it has a declared author, link the author to it via
        # a CREATION event, asserted by the author.
        c = self.constants
        sourcekey = c.source(factoid)
        workinfo = c.sourceinfo(sourcekey)  # The work is really a spec:TextExpression
        pbw_authority = self.get_authority_node(c.authorities(sourcekey))
        editors = self.get_authority_node(workinfo.get('editor'))
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
        # Express the expression
        sparql = f"""
        ?expr a {c.get_label('F3P')};
            {c.get_label('P3')} {Literal(edition_id).n3()} . """

        if text_id is None:
            # We are dealing with a secondary source. Assert an expression creation instead of a
            # work creation, with the editors; we will have to go back later and say that this
            # depended on another work (the primary source).
            # TODO remove the 'based' leg after reconciliation
            sparql += f"""
        ?ec a {c.get_label('F28')} . """
            sparql += self.create_assertion_sparql('a1', 'R17', '?ec', '?expr', editors, '?expr')
            sparql += self.create_assertion_sparql('a2', 'P14', '?ec', editors, editors, '?expr')
            assertions_set.extend(['a1', 'a2'])
        else:
            # We are dealing with a primary source, so we need to make a bunch of assertions.
            # First, the editors assert that the edition (that is, the expression) belongs to
            # the work; this is based on, well, the edition.
            sparql += f"""
        ?work a {c.get_label('F2T')} ;
            {c.get_label('P3')} "{text_id}" . """
            sparql += self.create_assertion_sparql('a1', 'R3', '?work', '?expr', editors, '?expr')
            assertions_set.append('a1')
            # Now we need to see if authorship has to be asserted.
            author = self.get_author_node(self.constants.author(sourcekey))
            if author is not None:
                # Make the assertions that the author authored the work. If we have a factoid,
                # then the authority for this assertion is the factoid's primary referent.
                # Otherwise, the authority (for now) is the editor.
                # If we don't have a specific reference for the claim, we just use the edition (again).
                aship_authority = editors
                aship_source = '?expr'
                if 'factoid' in workinfo:
                    # Pull in the authorship factoid that describes the authorship of this work
                    afact = self.mysqlsession.query(pbw.Factoid).filter_by(factoidKey=workinfo['factoid']).scalar()
                    afact_src = c.pbw_uri(afact)
                    asourcekey = c.source(afact)
                    if asourcekey != sourcekey:
                        print("HELP: Not dealing with %s authorship factoid from different work %s" % (
                            sourcekey, asourcekey))
                    else:
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
                        # We have to make a sourceref expression node, connected to this expression, for
                        # the factoid source
                        sparql += f"""
        ?srcref a {c.get_label('E33')} ;
            {c.get_label('P3')} {Literal(afact.sourceRef).n3()} ;
            {c.get_label('P190')} {Literal(afact.origLDesc, _get_source_lang(afact)).n3()} . """
                    aship_source = '?srcref'
                elif 'provenance' in workinfo:
                    # We have a page number. This makes our authorship authority the editor(s), with the source
                    # being the passage in this very edition.
                    sparql += f"""
        ?srcref a {c.get_label('E33')} ;
            {c.get_label('P3')} {Literal(workinfo['provenance']).n3()} . """
                    aship_source = '?srcref'

                #
                if aship_source == '?srcref':
                    # It is the PBW editor who says that a particular passage belongs to the respective edition.
                    # n.b. We will need to fix/change this manually for non-factoid provenance!
                    sparql += self.create_assertion_sparql('a2', 'R15', '?expr', aship_source, pbw_authority)
                    assertions_set.append('a2')

                # We have now dealt with extracting information from some relevant authorship factoid, if it exists.
                # Move on to the assertion that the author authored the work
                sparql += f"""
        ?wc a {c.get_label('F27')} . """
                sparql += self.create_assertion_sparql('a3', 'R16', '?wc', '?work', aship_authority, aship_source)
                sparql += self.create_assertion_sparql('a4', 'P14', '?wc', author, aship_authority, aship_source)
                assertions_set.extend(['a3', 'a4'])

        # Whatever we just made, return the expression, which is what we are after.
        res = c.ensure_entities_existence(sparql)
        if afact_src:
            c.document(afact_src, *[res[x] for x in assertions_set])
        return res['expr']

    def _find_or_create_identified_entity(self, etype, agent, identifier, dname):
        """Return an identified entity URIRef. This can be a Boulloterion (E22 subclass), an E21 Person, an E39 Agent,
        or an E74 Group depending on context. It is labeled with the identifier via an E15 Identifier Assignment
        carried out by the given agent, with dname becoming our preferred human-readable identifier."""
        c = self.constants
        if etype == c.get_label('E22B'):
            url = f'https://pbw2016.kdl.kcl.ac.uk/boulloterion/{identifier}/'
        elif agent == c.pbw_agent:
            url = f'https://pbw2016.kdl.kcl.ac.uk/person/{identifier.replace(" ", "/")}/'
        else:
            url = f'https://viaf.org/viaf/{identifier}/'

        # The entity should have its display name as a crm:P3 note
        entitystr = f"?entity a {etype} "
        if dname is not None:
            entitystr += f";\n            {c.get_label('P3')} {Literal(dname, lang='en').n3()} "
        entitystr += '.'

        # Construct the identifier assignment that should exist
        sparql = f"""
        ?ident a {c.get_label('E42')} ;
            {c.get_label('P190')} {Literal(identifier).n3()} ;
            {c.get_label('P3')} {Literal(url).n3()} .
        {entitystr}
        ?idass a {c.get_label('E15')} ;
            {c.star_subj_l} ?entity ;
            {c.get_label('P37')} ?ident ;
            {c.star_auth_l} {agent.n3()} .
        """

        # Ensure its existence and return the entity in question
        res = c.ensure_entities_existence(sparql)
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

    def find_or_create_seal_collection(self, collname):
        c = self.constants
        sparql = f"""
        ?collection a {c.get_label('E78')} ;
            {c.get_label('P1')} "{collname}" .
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
        ?appel a {c.get_label('E41')} ;
            {c.get_label('P190')} {Literal(name_en, 'en').n3()} ;
            {c.get_label('P190')} {Literal(name_ol, olang).n3()} . """
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

        # Set up the death event. We take for granted that every 11th-c. person has one.
        sparql = f"""
        ?de a {c.get_label('E69')} .
        ?a0 a {c.get_assertion_for_predicate('P100')} ;
            {c.star_subj_l} ?de ;
            {c.star_obj_l} {graphperson.n3()} .
        """

        # Get the description of the death in English and the original language
        olang = _get_source_lang(factoid) or 'grc'
        descriptions = [Literal(factoid.replace_referents(), 'en'), Literal(factoid.origLDesc, olang)]
        sparql += self.create_assertion_sparql('a1', 'P3', '?de', descriptions, agent, sourcenode)

        # See if the death is dated and, if so, add the date
        deathdate = factoid.deathRecord.sourceDate
        if deathdate:
            sparql += f"""
        ?deathdate a {c.get_label('E52')} ;
            {c.get_label('P80')} {Literal(deathdate).n3()} . """
            sparql += self.create_assertion_sparql('a2', 'P4', '?de', '?deathdate', agent, sourcenode)

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
        sparql = f"""
        ?designation a {label} . """
        sparql += self.create_assertion_sparql('a1', whopred, '?designation', graphperson, agent, sourcenode)
        sparql += self.create_assertion_sparql('a2', whichpred, '?designation', des, agent, sourcenode)
        res = c.ensure_entities_existence(sparql)
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
        roleid = self.constants.get_societyrole(factoid.occupation)
        roletype = self.constants.get_label('C1')
        whopred = 'SP13'
        whichpred = 'SP14'
        if factoid.occupation in self.constants.legal_designations:
            # We need to treat it as a legal role instead of an occupation
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
        dignity_id = self.constants.get_dignity(factoid.dignityOffice.stdName)
        # We treat dignities as legal roles
        # (r:C13 Social Role Embodiment) [dwho:P26 is embodied by] person
        # (r:C13) [dwhich:P33 is embodiment of] dignity
        self._find_or_create_social_designation(sourcenode, agent, factoid, graphperson, dignity_id,
                                                self.constants.get_label('C13'), 'SP26', 'SP33')

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
        sparql = f"""
        ?lskill a {c.get_label('C21')} ."""
        sparql += self.create_assertion_sparql('a1', 'SP38', graphperson, '?lskill', agent, sourcenode)
        sparql += self.create_assertion_sparql('a2', 'SP37', '?lskill', lkhid, agent, sourcenode)
        res = c.ensure_entities_existence(sparql)
        c.document(pbwdoc, res['a1'], res['a2'])

    def _find_or_create_kinship(self, graphperson, graphkin):
        # See if there is an existing kinship group of any sort with the person as source and their
        # kin as target. If not, return a new (not yet connected) C3 Social Relationship node.
        c = self.constants
        # The pattern to match
        sparql_check = f"""select distinct ?kstate where {{
        ?kstate a {c.get_label('C3')} .
        ?a1 a {c.get_assertion_for_predicate('SP17')} ;
            {c.star_subj_l} ?kstate ;
            {c.star_obj_l} {graphperson.n3()} .
        ?a2 a star:E13_sdhss_P18 ;
            {c.star_subj_l} ?kstate ;
            {c.star_obj_l} {graphkin.n3()} . }}"""
        res = self.g.query(sparql_check)
        if len(res):
            # We found a kinship between these two people. Return it
            return res.get('kstate')
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
            sparql = ''
            if kstate is None:
                # Make one if it doesn't exist. We will then have to refer to it by
                # variable name instead of URIref.
                sparql = f"        ?kstate a {c.get_label('C3')} ."
                kstate = '?kstate'
            # Now set up the three kinship assertions
            sparql += self.create_assertion_sparql('a1', 'SP16', kstate, ktype, agent, sourcenode)
            sparql += self.create_assertion_sparql('a2', 'SP17', kstate, graphperson, agent, sourcenode)
            sparql += self.create_assertion_sparql('a3', 'SP18', kstate, graphkin, agent, sourcenode)
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
        ?possession a {c.get_label('E18')} ;
            {c.get_label('P1')} {Literal(factoid.replace_referents()).n3()} ."""
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
        tla = self.get_authority_node([self.constants.ta])
        # Create the database record
        timenow = datetime.now()
        dbr_q = f"""
        ?thisurl a {c.get_label('E42')} ;
            {c.get_label('P190')} {Literal("https://github.com/erc-releven/PBWgraph/RELEVEN/graphimportSTAR.py").n3()} .
        ?this a {c.get_label('D14')} ;
            {c.get_label('P1')} ?thisurl .
        ?tstamp a {c.get_label('E52')} ;
            {c.get_label('P82a')} {Literal(self.starttime, datatype=XSD.dateTimeStamp).n3()} ;
            {c.get_label('P82b')} {Literal(timenow, datatype=XSD.dateTimeStamp).n3()} .
        ?dbr a {c.get_label('F28')} ;
            {c.get_label('P14')} {tla.n3()} ;
            {c.get_label('P4')} ?tstamp .
        """
        res = c.ensure_entities_existence(dbr_q, force_create=True)
        dbr = res['dbr']

        # Find all assertions with real UUIDs that haven't been created by a different software run.
        # WissKI-created assertions don't have UUIDs.
        uuid_regex = r"/\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12}$"
        sparql_check = f"""
        select distinct ?a where {{
            ?a a ?type .
            ?type rdfs:subClassOf {c.get_label('E13')} .
            MINUS {{
                ?d a {c.get_label('E31')} ;
                    {c.get_label('P70')} ?a .
            }}
            FILTER(regex(STR(?a), "{uuid_regex}"))
        }}
        """
        res = self.g.query(sparql_check)
        # These assertions must therefore have been produced by this run.
        for row in res:
            self.g.add((dbr, c.get_label('L11'), row['a']))

    def process_persons(self):
        """Go through the relevant person records and process them for factoids"""
        processed = 0
        used_sources = set()
        boulloteria = set()
        seals = 0

        # Get the classes of info that are directly in the person record
        direct_person_records = ['Gender', 'Identifier']
        # Get the list of factoid types in the PBW DB
        factoid_types = [x.typeName for x in self.mysqlsession.query(pbw.FactoidType).all() if
                         x.typeName != '(Unspecified)']
        for person in self.collect_person_records():
            # Skip the anonymous groups for now
            if person.name == 'Anonymi':
                continue
            processed += 1
            # Create or find the person node
            print("*** Making/finding node for person %s %d ***" % (person.name, person.mdbCode))
            graph_person = self.find_or_create_pbwperson(person)

            # Get the 'factoids' that are directly in the person record
            for ftype in direct_person_records:
                ourftype = _smooth_labels(ftype)
                try:
                    method = getattr(self, "%s_handler" % ourftype.lower())
                    method(person, graph_person)
                except AttributeError:
                    warn("No handler for %s record info; skipping." % ourftype)

            # Now get the factoids that are really factoids
            for ftype in factoid_types:
                ourftype = _smooth_labels(ftype)
                try:
                    method = getattr(self, "%s_handler" % ourftype.lower())
                    fprocessed = 0
                    for f in person.main_factoids(ftype):
                        # Find out what sources we are actually using and make note of them
                        source_key = self.constants.source(f)
                        if source_key is None:
                            print("Skipping factoid %d with unlisted source %s" % (f.factoidKey, f.source))
                            continue
                        elif source_key == 'OUT_OF_SCOPE':
                            print("Skipping factoid %d with a source %s out of our temporal scope"
                                  % (f.factoidKey, f.source))
                            continue
                        else:
                            used_sources.add(source_key)
                        # Note if we use a boulloterion, and if so how many seals it has
                        if f.boulloterion is not None:
                            if f.boulloterion.boulloterionKey not in boulloteria:
                                seals += len(f.boulloterion.seals)
                            boulloteria.add(f.boulloterion.boulloterionKey)
                        # Get the source, either a text passage or a seal inscription, and the authority
                        # for the factoid. Authority will either be the author of the text, or the PBW
                        # colleague who read the text and ingested the information.
                        (source_node, authority_node) = self.get_source_and_agent(f)
                        # If the factoid has no source then we skip it
                        if source_node is None:
                            print("HELP: Factoid %d had no parseable source for some reason" % f.factoidKey)
                            continue
                        # If the factoid has no authority then we assign it to the generic PBW agent
                        if authority_node is None:
                            authority_node = self.constants.pbw_agent
                        # Call the handler for this factoid type
                        method(source_node, authority_node, f, graph_person)
                        fprocessed += 1
                    if fprocessed > 0:
                        print("Ingested %d %s factoid(s)" % (fprocessed, ftype))

                except AttributeError:
                    pass
        self.record_assertion_factoids()
        print("Processed %d person records." % processed)
        print("Used the following sources: %s" % sorted(used_sources))
        print("Used the following boulloterion IDs with a total of %d seals: %s" % (seals, sorted(boulloteria)))


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
                        help="Graph containing existing STAR assertions, if any")
    args = parser.parse_args()
    # Process the person records
    gimport = graphimportSTAR(origgraph=args.graph, testmode=args.testing)
    gimport.process_persons()
    # Write the output to a timestamped file
    gimport.g.serialize(f'statements_{gimport.starttime.isoformat().split(".")[0].replace(":", "-")}.ttl')
    duration = datetime.now() - gimport.starttime
    print("Done! Ran in %s" % str(duration))
