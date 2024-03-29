import pbw
import RELEVEN.PBWstarConstants
import config
import re
from datetime import datetime, timezone
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from neo4j import GraphDatabase
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

    def __init__(self):
        # Connect to the SQL DB
        engine = create_engine('mysql+pymysql://' + config.dbstring)
        smaker = sessionmaker(bind=engine)
        self.mysqlsession = smaker()
        # Connect to the graph DB
        driver = GraphDatabase.driver(config.graphuri, auth=(config.graphuser, config.graphpw))
        # Make / retrieve the global nodes and self.constants
        self.constants = RELEVEN.PBWstarConstants.PBWstarConstants(driver)

    def collect_person_records(self):
        """Get a list of people whose floruit matches our needs"""
        relevant = [x for x in self.mysqlsession.query(pbw.Person).all()
                    if self.constants.inrange(x.floruit) and len(x.factoids) > 0]
        # Add the corner cases that we want to include: two emperors and a hegoumenos early in his career
        for name, code in [('Konstantinos', 8), ('Romanos', 3), ('Neophytos', 107)]:
            relevant.append(self.mysqlsession.query(pbw.Person).filter_by(name=name, mdbCode=code).scalar())
        print("Found %d relevant people" % len(relevant))
        return relevant
        # # Debugging / testing: restrict the list of relevant people
        # debugnames = ['Anna', 'Apospharios', 'Bagrat', 'Balaleca', 'Gagik', 'Herve', 'Ioannes', 'Konstantinos',
        #               'Liparites']
        # debugcodes = [62, 64, 68, 101, 102, 110]
        # return self.mysqlsession.query(pbw.Person).filter(
        #     and_(pbw.Person.name.in_(debugnames), pbw.Person.mdbCode.in_(debugcodes))
        # ).all()

    def create_assertion_query(self, factoid, subj, pred, obj, auth, src, var="a"):
        """Create the query pattern for an assertion with the given connections. Use 'var' to control
        the variable name for the assertion. Attempts to build the query with specific information first,
        assuming that plain node variable names indicate an already known node."""
        apreds = {'subj': '[:%s]' % self.constants.star_subject,
                  'obj': '[:%s]' % self.constants.star_object,
                  'auth': '[:%s]' % self.constants.star_auth,
                  'src': '[:%s]' % self.constants.star_source}
        dataprops = ['P3']
        # Do the subject and object first, then source, authority and predicate
        # as search area probably increases for each in that order
        anodes = [('auth', auth)]
        if src is not None:
            anodes.insert(0, ('src', src))
        if pred not in dataprops:  # otherwise it is a data property, see below
            if re.match(r'^\w+$', obj):
                anodes.insert(0, ('obj', obj))
            else:
                anodes.append(('obj', obj))
        if re.match(r'^\w+$', subj):
            anodes.insert(0, ('subj', subj))
        else:
            anodes.append(('subj', subj))

        # Now build the query using the order in anodes
        if factoid is None:
            factoid = 'NONE'
        afromfact = 'https://pbw2016.kdl.kcl.ac.uk/rdf/' + factoid
        aprops = ['origsource: "%s"' % afromfact]
        # If the predicate is P3, we need a data property instead of an object property
        if pred in dataprops:
            aprops.append('%s: %s' % (self.constants.star_object, obj))
        ameta = ':%s {%s}' % (self.constants.get_assertion_for_predicate(pred), ', '.join(aprops))
        aclassed = False
        aqparts = []
        for nt in anodes:
            aqparts.append("(%s%s)-%s->(%s)" % (var, ameta if not aclassed else '', apreds[nt[0]], nt[1]))
            aclassed = True
        return "COMMAND %s " % ", ".join(aqparts)

    def gender_handler(self, sqlperson, graphperson):
        orig = 'person/%d' % sqlperson.personKey
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
            # Make the event tied to this person
            genderassertion = _matchid('p', graphperson)
            genderassertion += _matchid('s', self.constants.get_gender(pbw_sex))
            genderassertion += _matchid('pbw', self.constants.pbw_agent)
            genderassertion += "WITH p, s, pbw "
            genderassertion += self.create_assertion_query(orig, 'ga:%s' % self.constants.get_label('E17'),
                                                            'P41', 'p', 'pbw', None, 'a1')
            genderassertion += self.create_assertion_query(orig, 'ga', 'P42', 's', 'pbw', None, 'a2')
            genderassertion += "RETURN a1, a2"
            # print(genderassertion)
            with self.constants.graphdriver.session() as session:
                result = session.run(genderassertion.replace('COMMAND', 'MATCH')).single()
                if result is None:
                    session.run(genderassertion.replace('COMMAND', 'CREATE'))

    def identifier_handler(self, sqlperson, graphperson):
        """The identifier in this context is the 'origName' field, thus an identifier assigned by PBW
        not on the basis of any particular source. We turn this into an Appellation assertion"""
        orig = 'person/%d' % sqlperson.personKey
        # Strip any parenthetical from the nameOL field
        withparen = re.search(r'(.*)\s+\(.*\)', sqlperson.nameOL)
        if withparen is not None:
            appellation = withparen.group(1)
        else:
            appellation = sqlperson.nameOL.rstrip()
        idassertion = _matchid('p', graphperson)
        idassertion += _matchid('pbw', self.constants.pbw_agent)
        idassertion += "MERGE (app:%s {%s: \"%s\"}) " % (
            self.constants.get_label('E41'), self.constants.get_label('P190'), appellation)
        idassertion += "WITH p, pbw, app "
        idassertion += self.create_assertion_query(orig, 'p', 'P1', 'app', 'pbw', None)
        idassertion += "RETURN a"
        with self.constants.graphdriver.session() as session:
            result = session.run(idassertion.replace('COMMAND', 'MATCH')).single()
            if result is None:
                session.run(idassertion.replace('COMMAND', 'CREATE'))

    def find_or_create_event(self, person, eventclass, predicate):
        """Helper function to find the relevant event for event-based factoids"""
        query = _matchid('pers', person)
        query += "MATCH (a:%s)-[:%s]->(event:%s) " % (self.constants.get_assertion_for_predicate(predicate),
                                                      self.constants.star_subject, eventclass)
        query += "MATCH (a)-[:%s]->(pers) " % self.constants.star_object
        query += "RETURN DISTINCT event"  # There may well be multiple assertions about this event
        with self.constants.graphdriver.session() as session:
            result = session.run(query).single()
            if result is None:
                # If we don't have an event of this class tied to this person yet, create a new one,
                # look it up again for its UUID, and return it for use in the assertion being made about it.
                session.run("CREATE (event:%s {justcreated:true}) RETURN event" % eventclass)
                result = session.run("MATCH (event:%s {justcreated:true}) REMOVE event.justcreated RETURN event"
                                     % eventclass).single()
            return result['event'].get('uuid')

    def get_source_and_agent(self, factoid):
        """Returns a node that represents the source for this factoid. Creates the network of nodes and
        relationships to describe that source, if necessary. The source will either be an E34 Inscription from
        a physical E22 Human-Made Object (the boulloterion) or an E33 Linguistic Object, i.e. a passage from a
        Publication (the written primary source)."""
        # Is this a 'seals' source without a boulloterion? If so warn and return None
        sourcekey = self.constants.source(factoid)
        if self.constants.authorities(sourcekey) is None:
            if sourcekey != 'Seals' or factoid.boulloterion is None:
                warn("No boulloterion found for seal-sourced factoid %d; skipping" % factoid.factoidKey
                     if sourcekey == 'Seals'
                     else "Source %s of factoid %d not known; skipping" % (factoid.source, factoid.factoidKey))
                return None, None
        if factoid.boulloterion is not None:
            if len(factoid.boulloterion.publication) == 0:
                warn("Boulloterion %d has empty publication list; skipping" % factoid.boulloterion.boulloterionKey)
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
        if it is a new boulloterion."""
        orig = 'boulloterion/%d' % boulloterion.boulloterionKey
        # boulloterion is an E22 Human-Made Object, with an identifier assigned by PBW
        keystr = "%d" % boulloterion.boulloterionKey
        btitle = "Boulloterion of %s" % boulloterion.title
        boul_node = self._find_or_create_identified_entity(
            self.constants.get_label('E22B'), self.constants.pbw_agent, keystr, btitle)
        # Does it have any assertions yet concerning its inscription?
        testq = "MATCH (boul)<-[:%s]-(a:%s) WHERE boul.uuid = '%s' RETURN a" % (
            self.constants.star_subject, self.constants.get_assertion_for_predicate('P128'), boul_node)
        with self.constants.graphdriver.session() as session:
            exists = session.run(testq).values()
        if len(exists) == 0:
            # It does not. We have some creating to do.
            q = _matchid('pbweditor', pbweditor)
            q += _matchid('boul', boul_node)
            # Get/create the list of sources, if we have one
            source_node = self.get_boulloterion_sourcelist(boulloterion)
            if source_node is not None:
                q += _matchid('src', source_node)
            # Create the inscription node
            q += "MERGE (inscription:%s {%s:\"%s\"}) " % (
                self.constants.get_label('E34'), self.constants.get_label('P190'), boulloterion.origLText)
            # Create the seal(s) that belong to it; these are E22s that belong to
            # E78 Curated Holdings (the collections).
            for i, seal in enumerate(boulloterion.seals):
                # n.b. I had to correct the collectionKey of seal 4990
                coll = seal.collection.collectionName
                # The curated holding
                q += "MERGE (coll%d:%s {%s:\"%s\"}) " % (
                    i, self.constants.get_label('E78'), self.constants.get_label('P1'), coll)
                # The seal. Make a unique ID from collectionKey.boulloterionKey
                seal_id = "%d.%d.%d" % (seal.collectionKey, seal.boulloterionKey, seal.collectionRef)
                q += "MERGE (seal%d:%s {%s:\"%s\"}) " % (
                    i, self.constants.get_label('E22S'), self.constants.get_label('P3'), seal_id)
            # Now assert that the seal belongs to the collection and that the boulloterion produced
            # the seals. These both depend on the PBW editor, and we can't name a specific source for
            # either assertion based on the PBW data.
            for i in range(len(boulloterion.seals)):
                q += self.create_assertion_query(
                    orig, "coll%d" % i, 'P46', "seal%d" % i, 'pbweditor', None, 'cs%d' % i)
                q += self.create_assertion_query(orig, 'boul', 'P108', 'seal%d' % i, 'pbweditor', None, 'bs%d' % i)
            # Finally, assert based on the sources that the boulloterion carries the inscription
            q += self.create_assertion_query(orig, 'boul', 'P128', 'inscription', 'pbweditor',
                                              'src' if source_node else None)
            q += "RETURN boul.uuid"
            with self.constants.graphdriver.session() as session:
                result = session.run(q.replace('COMMAND', 'CREATE')).single()
                # Sanity check
                if result['boul.uuid'] != boul_node:
                    raise Exception("Boulloterion metadata not created!")
        return boul_node

    def get_boulloterion_inscription(self, boulloterion, pbweditor):
        # This factoid is taken from one or more seal inscription. Let's pull that out into CRM objects.
        # If the boulloterion has no associated publications, we shouldn't use it.
        orig = 'boulloterion/%d' % boulloterion.boulloterionKey
        if len(boulloterion.publication) == 0 and \
                boulloterion.boulloterionKey not in self.constants.boulloterion_sources:
            warn("No published source found for boulloterion %d; skipping this factoid" % boulloterion.boulloterionKey)
            return None
        # Get (create if necessary) the boulloterion node. This will also create the inscription.
        boul_node = self.get_boulloterion(boulloterion, pbweditor)
        # Now find the assertion in question: that it P128 carried an E34 Inscription
        qm = _matchid('boul', boul_node)
        # We haven't pre-defined 'agent' because we don't need to set or query it for the match.
        qm += self.create_assertion_query(orig, 'boul', 'P128', 'inscription', 'agent', None)
        qm += " RETURN inscription.uuid as iid"
        qm = qm.replace('COMMAND', 'MATCH')
        with self.constants.graphdriver.session() as session:
            result = session.run(qm).single()
            return result['iid']

    def get_boulloterion_sourcelist(self, boulloterion):
        """A helper function to create the list of publications where the seals allegedly produced by a
        given boulloterion were published. Returns either a single Publication (if there was a single
        publication) or a Bibliography that represents a collection of Expressions. We do not
        try to isolate individual references here; anyone interested in that can follow the link back
        to the original boulloterion description."""
        # Extract the bibliography and page / object ref for each publication
        pubs = dedupe([x.bibSource for x in boulloterion.publication])
        if len(pubs) == 0:
            extrapub, ref = self.constants.boulloterion_sources.get(boulloterion.boulloterionKey, (-1, None))
            if extrapub < 0:
                # We only have the seal catalogues as sources, and those attach to the seals.
                return None
            else:
                pubs = [self.mysqlsession.query(pbw.Bibliography).filter_by(bibKey=extrapub).scalar()]

        # Get some labels
        f3 = self.constants.get_label('F3')
        e73b = self.constants.get_label('E73B')
        p165 = self.constants.get_label('P165')
        pubct = 0
        mquery = ""
        source_nodes = []
        for source in pubs:
            # Make sure with 'merge' that each bibliography node exists
            # Fix the encoding for the entries we didn't add
            shortName = source.shortName if source.bibKey == 816 else re_encode(source.shortName)
            latinBib = source.latinBib if source.bibKey == 816 else re_encode(source.latinBib)
            sn = "(src%d:%s {%s:'%s', %s:'%s'})" % (
                pubct, f3, self.constants.get_label('P1'), shortName,
                self.constants.get_label('P102'), escape_text(latinBib))
            source_nodes.append(sn)
            mquery += "MERGE %s " % sn
            pubct += 1
        if pubct > 1:
            # Check to see whether we have a matching group with only these publication nodes.
            parts = []
            retvar = "srcgrp"
            mquery += "WITH %s " % ", ".join(["src%d" % x for x in range(pubct)])
            # This size syntax taken blindly from
            # https://stackoverflow.com/questions/68785613/neo4j-4-3-deprecation-of-size-function-and-pattern-comprehension
            mquery += "MATCH (srcgrp:%s) WHERE size([(srcgrp)-[:%s]->(:%s) | srcgrp]) = %d " % (e73b, p165, f3, pubct)
            for n in range(pubct):
                parts.append("(srcgrp)-[:%s]->(src%d)" % (p165, n))
            mquery += "MATCH " + ", ".join(parts) + " "
        else:
            # We simply return the one node we created
            retvar = "src0"
        mquery += "RETURN %s" % retvar
        with self.constants.graphdriver.session() as session:
            ret = session.run(mquery).single()
            if ret is None:
                # The plural sources (now) exist, but the source group doesn't. Create it
                createparts = ["(srcgrp:%s)" % e73b]
                for j in range(pubct):
                    createparts.append("(srcgrp)-[:%s]->(src%d)" % (p165, j))
                cquery = "MATCH " + ", ".join(source_nodes) + " "
                cquery += "CREATE " + ", ".join(createparts) + " "
                cquery += "RETURN srcgrp"
                ret = session.run(cquery).single()
            if 'uuid' not in ret[retvar]:
                # We have to go fishing for the thing again; we reuse the MATCH / MERGE statements.
                ret = session.run(mquery).single()
            return ret[retvar].get('uuid')

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
        wholesource = self.get_source_work_expression(factoid)
        if wholesource is None:
            return None
        # In this context, the agent is the PBW editor for this source.
        sourcekey = self.constants.source(factoid)
        agent = self.get_authority_node(self.constants.authorities(sourcekey))
        # First see whether this source reference already exists
        srcref_node = "(sourceref:%s {%s:'%s', %s:'%s'})" % (
            self.constants.get_label('E33'), self.constants.get_label('P3'),
            escape_text(self.constants.sourceref(factoid)),
            self.constants.get_label('P190'), escape_text(factoid.origLDesc))
        qm = _matchid('expr', wholesource)
        qm += "MATCH %s " % srcref_node
        qm += self.create_assertion_query(None, 'expr', 'R15', 'sourceref', 'agent', None)
        qm += " RETURN sourceref.uuid as sid"
        qm = qm.replace('COMMAND', 'MATCH')
        with self.constants.graphdriver.session() as session:
            result = session.run(qm).single()
            if result is not None:
                return result['sid']

        # In this case, we have to create the assertion.
        qc = _matchid('expr', wholesource)
        qc += _matchid('agent', agent)
        qc += "COMMAND %s " % srcref_node
        qc += "WITH expr, sourceref, agent "
        # The agent (whoever worked on the source) asserts that the reference is from the given source.
        qc += self.create_assertion_query(None, "expr", "R15", "sourceref", "agent", None)
        qc += "RETURN sourceref"
        # To run it right, run it twice
        with self.constants.graphdriver.session() as session:
            session.run(qc.replace('COMMAND', 'CREATE'))
            result = session.run(qm).single()
            return result['sid']

    def get_source_work_expression(self, factoid):
        # Ensure the existence of the work and, if it has a declared author, link the author to it via
        # a CREATION event, asserted by the author.

        sourcekey = self.constants.source(factoid)
        workinfo = self.constants.sourceinfo(sourcekey)
        pbw_authority = self.get_authority_node(self.constants.authorities(sourcekey))
        editors = self.get_authority_node(workinfo.get('editor'))
        # The work identifier is the 'work' key, or else the PBW source ID string.
        workid = workinfo.get('work')
        # The expression identifier is the 'expression' key (a citation to the edition).
        exprid = workinfo.get('expression')

        # Check that we have the information on this source
        if editors is None or exprid is None:
            print("Cannot ingest factoid with source %s until work/edition info is specified" % sourcekey)
            return None

        # First see if the expression already exists. We cheat here by setting a 'pbwid' on the
        # expression for easy lookup.
        expression = "(expr:%s {%s: \"%s\", pbwid: \"%s\"})" % (
            self.constants.get_label('F3'), self.constants.get_label('P3'), escape_text(exprid), sourcekey)
        mquery = "MATCH %s RETURN expr.uuid" % expression
        with self.constants.graphdriver.session() as session:
            result = session.run(mquery).single()
            if result is not None:
                return result['expr.uuid']

        if workid is None:
            # We are dealing with a secondary source. Assert an expression creation instead of a
            # work creation, with the editors; we will have to go back later and say that this
            # depended on another work (the primary source).
            q = _matchid('editors', editors)
            q += "CREATE %s " % expression
            q += self.create_assertion_query(None, 'ec:%s' % self.constants.get_label('F28'), 'R17', 'expr', 'editors',
                                              'expr', 'e1')
            q += self.create_assertion_query(None, 'ec', 'P14', 'editors', 'editors', 'expr', 'e2')
        else:
            # We are dealing with a primary source, so we need to make a bunch of assertions.
            # First, the editors assert that the edition (that is, the expression) belongs to
            # the work; this is based on, well, the edition.
            q = _matchid('editors', editors)
            q += "MERGE (work:%s {%s: \"%s\"}) " % (
                self.constants.get_label('F1'), self.constants.get_label('P3'), workid)
            q += "CREATE %s " % expression
            q += "WITH editors, work, expr "
            q += self.create_assertion_query(None, 'work', 'R3', 'expr', 'editors', 'expr')

            # Now we need to see if authorship has to be asserted.
            author = self.get_author_node(self.constants.author(sourcekey))
            if author is not None:
                # Make the assertions that the author authored the work. If we have a factoid,
                # then the authority for this assertion is the factoid's primary referent.
                # Otherwise, the authority (for now) is the editor.
                # If we don't have a specific reference for the claim, we just use the edition (again).
                aship_authority = editors
                aship_source = 'expr'
                aship_srefnode = None
                if 'factoid' in workinfo:
                    # Pull in the authorship factoid that describes the authorship of this work
                    afact = self.mysqlsession.query(pbw.Factoid).filter_by(factoidKey=workinfo['factoid']).scalar()
                    asourcekey = self.constants.source(afact)
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
                        aship_srefnode = "(srcref:%s {%s:'%s', %s:'%s'})" % (
                            self.constants.get_label('E33'), self.constants.get_label('P3'),
                            escape_text(afact.sourceRef),
                            self.constants.get_label('P190'), escape_text(afact.origLDesc))
                        aship_source = 'srcref'
                elif 'provenance' in workinfo:
                    # We have a page number. This makes our authorship authority the editor(s), with the source
                    # being the passage in this very edition.
                    aship_srefnode = "(srcref:%s {%s:'%s'})" % (
                        self.constants.get_label('E33'), self.constants.get_label('P3'), workinfo['provenance'])
                    aship_source = 'srcref'
                # On to the assertion that the author authored the work
                if aship_authority == editors:
                    q += "WITH editors AS reporter, work, expr "
                else:
                    # If the authorship authority is the author or the PBW editor, we have to fish them out.
                    q += "WITH work, expr "
                    q += _matchid('reporter', aship_authority)
                q += _matchid('author', author)
                if aship_srefnode:
                    # It is the PBW editor who says that a particular passage belongs to the respective edition.
                    # n.b. We will need to fix/change this manually for non-factoid provenance!
                    q += _matchid('pbweditor', pbw_authority)
                    q += "MERGE %s " % aship_srefnode
                    q += self.create_assertion_query(None, 'expr', 'R15', aship_source, 'pbweditor', None, 'wc0')
                q += self.create_assertion_query(None, 'wc:%s' % self.constants.get_label('F27'),
                                                  'R16', 'work', 'reporter', aship_source, 'wc1')
                q += self.create_assertion_query(None, 'wc', 'P14', 'author', 'reporter', aship_source, 'wc2')
        # Whatever we just made, return the expression, which is what we are after.
        q += "RETURN expr"
        with self.constants.graphdriver.session() as session:
            session.run(q.replace('COMMAND', 'CREATE'))
            result = session.run(mquery).single()
            # This will barf if the above didn't create anything
            return result['expr.uuid']

    def _find_or_create_identified_entity(self, etype, agent, identifier, dname):
        """Return an identified entity. This can be a Boulloterion (E22 subclass), an E21 Person, an E39 Agent,
        or an E74 Group depending on context. It is labeled with the identifier via an E15 Identifier Assignment
        carried out by the given agent, with dname becoming our preferred human-readable identifier."""
        if etype == self.constants.get_label('E22B'):
            url = 'https://pbw2016.kdl.kcl.ac.uk/boulloterion/%s/' % identifier
        elif agent == self.constants.pbw_agent:
            url = 'https://pbw2016.kdl.kcl.ac.uk/person/%s/' % identifier.replace(' ', '/')
        else:
            url = 'https://viaf.org/viaf/%s/' % identifier
        # We can't merge with comma statements, so we have to do it with successive one-liners.
        # Start the merge from the specific information we have, which is the agent and the identifier itself.
        # E15:idass -[P14 carried out by]-> coll
        # E15:idass -[P37 assigned]-> E42:idlabel {uri:"url"}
        # E15:idass -[P140 assigned to]-> etype:p -[P3 has note]-> "dname"
        nodelookup = _matchid('coll', agent)
        # Add the secret 'pbwid' attribute to ensure uniqueness of the thing; we have some boulloterions
        # with identical descriptions
        nodelookup += "MERGE (ident:%s {%s:'%s',%s:'%s'}) " \
                      "MERGE (p:%s {%s:'%s', pbwid:'%s'}) " \
                      "MERGE (coll)<-[:%s]-(idass:%s)-[:%s]->(ident)  " \
                      "MERGE (idass)-[:%s]->(p) RETURN p" % \
                      (self.constants.get_label('E42'), self.constants.get_label('P190'), identifier,
                       self.constants.get_label('P3'), url,
                       etype, self.constants.get_label('P3'), escape_text(dname), identifier,
                       self.constants.get_label('P14'), self.constants.get_label('E15'),
                       self.constants.get_label('P37'),
                       self.constants.get_label('P140') )
        with self.constants.graphdriver.session() as session:
            graph_entity = session.run(nodelookup).single()['p']
            if 'uuid' not in graph_entity:
                # do it again to get the UUID that was set
                graph_entity = session.run(nodelookup).single()['p']
        return graph_entity.get('uuid')

    def find_or_create_pbwperson(self, sqlperson):
        return self._find_or_create_identified_entity(
            self.constants.get_label('E21'), self.constants.pbw_agent,
            "%s %d" % (sqlperson.name, sqlperson.mdbCode), sqlperson.descName)

    def find_or_create_viafperson(self, name, viafid):
        return self._find_or_create_identified_entity(
            self.constants.get_label('E21'), self.constants.viaf_agent, viafid, name)

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
        gc = []  # group connection parts
        ll = []  # label parts
        i = 1
        q = ''  # main query
        for m in members:
            lvar = "m%d" % i
            q += _matchid(lvar, m)
            gc.append("(group)-[:%s]->(m%d)" % (self.constants.get_label('P107'), i))
            ll.append("m%d.%s" % (i, self.constants.get_label('P3')))
            i += 1
        # We've matched the nodes, now make the group label
        q += "WITH %s, %s as glabel " % (', '.join(["m%d" % x for x in range(1, i)]), ' + "; " + '.join(ll))
        # Since this is a group with symmetrical membership, we have to add a 'DISTINCT' in order to avoid getting
        # mirrored orders of the group
        q += "COMMAND (group:%s {%s:glabel}), %s " \
             "RETURN group" % (self.constants.get_label('E74A'), self.constants.get_label('P3'), ", ".join(gc))
        with self.constants.graphdriver.session() as session:
            g = session.run(q.replace('COMMAND', 'MATCH')).single()
            if g is None:
                # Run it twice to get the UUID assigned
                session.run(q.replace('COMMAND', 'CREATE'))
                g = session.run(q.replace('COMMAND', 'MATCH')).single()
            return g['group'].get('uuid')

    def appellation_handler(self, sourcenode, agent, factoid, graphperson):
        """This handler deals with Second Name factoids and also Alternative Name factoids.
        The Second Names might be in all sorts of languages in the factoid itself, but refer
        to a canonical version of the name in the FamilyName table, which is probably usually
        Greek. The Alternative Name factoids should exclusively use the information in the
        base factoid."""
        orig = "factoid/%d" % factoid.factoidKey
        appassertion = _matchid('p', graphperson)
        appassertion += _matchid('agent', agent)
        appassertion += _matchid('source', sourcenode)
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

        content = '["%s@en","%s@%s"]' % (escape_text(name_en), escape_text(name_ol), olang)
        appassertion += "MERGE (n:%s {%s:%s}) " % (
            self.constants.get_label('E41'), self.constants.get_label('P190'), content)
        appassertion += "WITH p, agent, source, n "
        appassertion += self.create_assertion_query(orig, 'p', 'P1', 'n', 'agent', 'source')
        appassertion += "RETURN a"
        with self.constants.graphdriver.session() as session:
            result = session.run(appassertion.replace('COMMAND', 'MATCH')).single()
            if result is None:
                session.run(appassertion.replace('COMMAND', 'CREATE'))

    def description_handler(self, sourcenode, agent, factoid, graphperson):
        """Record the descriptions given in the sources as P3 data-property assertions."""
        orig = "factoid/%d" % factoid.factoidKey
        olang = _get_source_lang(factoid) or 'grc'
        content = '["%s@en","%s@%s"]' % (
            escape_text(factoid.replace_referents()), escape_text(factoid.origLDesc), olang)
        descassertion = _matchid('p', graphperson)
        descassertion += _matchid('agent', agent)
        descassertion += _matchid('src', sourcenode)
        descassertion += self.create_assertion_query(orig, 'p', 'P3', content, 'agent', 'src')
        descassertion += 'RETURN a'
        with self.constants.graphdriver.session() as session:
            result = session.run(descassertion.replace('COMMAND', 'MATCH')).single()
            if result is None:
                session.run(descassertion.replace('COMMAND', 'CREATE'))

    def death_handler(self, sourcenode, agent, factoid, graphperson):
        # Each factoid is its own set of assertions pertaining to the single death of a person.
        # When there are multiple sources, we will have to review them for consistency and make
        # proxies for the death event as necessary.
        orig = "factoid/%d" % factoid.factoidKey
        # See if we can find the death event
        deathevent = self.find_or_create_event(graphperson, self.constants.get_label('E69'), 'P100')
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

        olang = _get_source_lang(factoid) or 'grc'
        deathdesc = '["%s@en","%s@%s"]' % (
            escape_text(factoid.replace_referents()), escape_text(factoid.origLDesc), olang)
        deathdate = factoid.deathRecord.sourceDate
        if deathdate == '':
            deathdate = None
        deathassertion = _matchid('p', graphperson)
        deathassertion += _matchid('agent', agent)
        deathassertion += _matchid('source', sourcenode)
        deathassertion += _matchid('devent', deathevent)
        if deathdate is not None:
            # Just record the string; many of them don't resolve to a fixed date
            deathassertion += "MERGE (datedesc:%s {%s:\"%s\"}) " % (
                self.constants.get_label('E52'), self.constants.get_label('P80'), deathdate)
        deathassertion += "WITH p, agent, source, devent%s " % (', datedesc' if deathdate else '')
        # Create an assertion that the death happened
        deathassertion += self.create_assertion_query(orig, 'devent', 'P100', 'p', 'agent', 'source')
        # Create an assertion about the description of the death
        deathassertion += self.create_assertion_query(orig, 'devent', 'P3', deathdesc, 'agent', 'source', 'a1')
        # Create an assertion about when the death happened.
        if deathdate is not None:
            deathassertion += self.create_assertion_query(orig, 'devent', 'P4', 'datedesc', 'agent', 'source', 'a2')
        deathassertion += "RETURN a%s" % (", a2" if deathdate else '')

        # print(deathassertion)
        with self.constants.graphdriver.session() as session:
            result = session.run(deathassertion.replace('COMMAND', 'MATCH').replace('ANOTE', 'WHERE')).single()
            if result is None:
                session.run(deathassertion.replace('COMMAND', 'CREATE').replace('ANOTE', 'SET'))

    def ethnicity_handler(self, sourcenode, agent, factoid, graphperson):
        """Assign a group membership for the given ethnicity to the person"""
        orig = "factoid/%d" % factoid.factoidKey
        if factoid.ethnicityInfo is None or factoid.ethnicityInfo.ethnicity is None:
            # We can't assign any ethnicity without the ethnicity info
            warn("Empty ethnicity factoid found: id %s" % factoid.factoidKey)
            return
        elabel = factoid.ethnicityInfo.ethnicity.ethName
        groupid = self.constants.get_ethnicity(elabel)
        gassertion = _matchid('p', graphperson)
        gassertion += _matchid('agent', agent)
        gassertion += _matchid('source', sourcenode)
        gassertion += _matchid('group', groupid)
        gassertion += self.create_assertion_query(orig, 'group', 'P107', 'p', 'agent', 'source')
        gassertion += "RETURN a"
        with self.constants.graphdriver.session() as session:
            result = session.run(gassertion.replace('COMMAND', 'MATCH')).single()
            if result is None:
                session.run(gassertion.replace('COMMAND', 'CREATE'))

    # Helper to create the assertions for our various social designation groups
    def _find_or_create_social_designation(self, sourcenode, agent, factoid, graphperson, des, label, whopred,
                                           whichpred):
        # (grouping:label) [:whopred] person
        # (grouping) [:whichpred] rnode
        orig = "factoid/%d" % factoid.factoidKey
        gassertion = _matchid('p', graphperson)
        gassertion += _matchid('agent', agent)
        gassertion += _matchid('source', sourcenode)
        gassertion += _matchid('designation', des)
        gassertion += self.create_assertion_query(orig, 'persondes:%s' % label, whopred, 'p', 'agent', 'source')
        gassertion += self.create_assertion_query(orig, 'persondes', whichpred, 'designation', 'agent', 'source', 'a1')
        gassertion += "RETURN a"
        with self.constants.graphdriver.session() as session:
            result = session.run(gassertion.replace('COMMAND', 'MATCH')).single()
            if result is None:
                session.run(gassertion.replace('COMMAND', 'CREATE'))

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
        orig = "factoid/%d" % factoid.factoidKey
        if factoid.languageSkill is None:
            return
        lkhid = self.constants.get_language(factoid.languageSkill)
        # This doesn't chain quite the same way as the others do
        # person [rwho:P38 has skill] (r:C21 Skill)
        # (r:C21 Skill) [rwhich:P37 concerns] (l:C29 Know-How)
        lassertion = _matchid('p', graphperson)
        lassertion += _matchid('agent', agent)
        lassertion += _matchid('source', sourcenode)
        lassertion += _matchid('lkh', lkhid)
        lassertion += self.create_assertion_query(orig, 'p', 'SP38', 'ls:%s' % self.constants.get_label('C21'),
                                                   'agent', 'source')
        lassertion += self.create_assertion_query(orig, 'ls', 'SP37', 'lkh', 'agent', 'source', 'a1')
        lassertion += "RETURN a"
        with self.constants.graphdriver.session() as session:
            result = session.run(lassertion.replace('COMMAND', 'MATCH')).single()
            if result is None:
                session.run(lassertion.replace('COMMAND', 'CREATE'))

    def _find_or_create_kinship(self, graphperson, graphkin):
        # See if there is an existing kinship group of any sort with the person as source and their
        # kin as target. If not, return a new (not yet connected) C3 Social Relationship node.
        e13_sp17 = self.constants.get_assertion_for_predicate('SP17')
        e13_sp18 = self.constants.get_assertion_for_predicate('SP18')
        c3 = self.constants.get_label('C3')
        kinquery = _matchid('p', graphperson)
        kinquery += _matchid('kin', graphkin)
        # There might be many assertions about this group by now, so we return distinct because there should
        # still only be one kinship group
        kinquery += "MATCH (p)<-[:%s]-(a1:%s)-[:%s]->(kg:%s)<-[:%s]-(a2:%s)-[:%s]->(kin) " \
                    "RETURN DISTINCT kg" % (self.constants.star_object, e13_sp17, self.constants.star_subject, c3,
                                            self.constants.star_subject, e13_sp18, self.constants.star_object)
        with self.constants.graphdriver.session() as session:
            result = session.run(kinquery).single()
            if result is None:
                # If the kinship pair hasn't been referenced yet, then create a new empty kinship and return it
                # for use in the assertions below.
                session.run("CREATE (kg:%s {justcreated:true}) RETURN kg" % c3)
                result = session.run("MATCH (kg:%s {justcreated:true}) REMOVE kg.justcreated RETURN kg" % c3).single()
            return result['kg'].get('uuid')

    def kinship_handler(self, sourcenode, agent, factoid, graphperson):
        # These are social relationships as opposed to social roles, so they need a partner.
        # (rel:C3 Social Relationship) [pt:P16 has type] (kt:C4 Kinship type)
        # (rel) [src:P17 has source] (p:E21 person)
        # (rel) [trg:P18 has target] (p:E21 kin)
        orig = "factoid/%d" % factoid.factoidKey
        if factoid.kinshipType is None:
            warn("Empty kinship factoid found: id %d" % factoid.factoidKey)
            return
        ktype = self.constants.get_kinship(factoid.kinshipType.gspecRelat)

        for kin in factoid.referents():
            if kin.name == 'Anonymi' or kin.name == 'Anonymae':
                # We skip kin who are anonymous groups
                continue
            graphkin = self.find_or_create_pbwperson(kin)
            if graphkin == graphperson:
                warn("Person %s listed as related to self" % kin)
                continue
            kgroup = self._find_or_create_kinship(graphperson, graphkin)
            kinassertion = _matchid('p', graphperson)
            kinassertion += _matchid('agent', agent)
            kinassertion += _matchid('source', sourcenode)
            kinassertion += _matchid('kin', graphkin)
            kinassertion += _matchid('kg', kgroup)
            kinassertion += _matchid('ktype', ktype)
            # The relationship has type ktype
            kinassertion += self.create_assertion_query(orig, 'kg', 'SP16', 'ktype', 'agent', 'source', 'a1')
            # The relationship has our person as a source
            kinassertion += self.create_assertion_query(orig, 'kg', 'SP17', 'p', 'agent', 'source', 'a2')
            # The relationship has the kinperson as a target
            kinassertion += self.create_assertion_query(orig, 'kg', 'SP18', 'kin', 'agent', 'source', 'a3')
            kinassertion += "RETURN a1, a2, a3"
            with self.constants.graphdriver.session() as session:
                result = session.run(kinassertion.replace('COMMAND', 'MATCH')).single()
                if result is None:
                    session.run(kinassertion.replace('COMMAND', 'CREATE'))

    def possession_handler(self, sourcenode, agent, factoid, graphperson):
        """Ensure the existence of an E18 Physical Thing (we don't have any more category info about
        the possessions). For now, we assume that a possession with an identical description is, in fact,
        the same possession."""
        orig = "factoid/%d" % factoid.factoidKey
        # Give the possession its description, which comes out of the factoid's engDesc
        possession_attrs = "%s: '%s'" % (self.constants.get_label('P1'), escape_text(factoid.replace_referents()))
        # Give the assertion a note, if such note exists in the poorly-named PossessionFactoid.possessionName field
        assertion_attrs = ''
        if factoid.possession is not None and factoid.possession != '':
            assertion_attrs = "ANOTE a.%s = '%s'" % (self.constants.get_label('P3'), escape_text(factoid.possession))
        posassertion = _matchid('p', graphperson)
        posassertion += _matchid('agent', agent)
        posassertion += _matchid('source', sourcenode)
        posassertion += "MERGE (poss:%s {%s}) " % (self.constants.get_label('E18'), possession_attrs)
        posassertion += "WITH p, agent, source, poss "
        posassertion += self.create_assertion_query(orig, 'poss', 'P51', 'p', 'agent', 'source')
        posassertion += "%s RETURN a" % assertion_attrs
        with self.constants.graphdriver.session() as session:
            result = session.run(posassertion.replace('COMMAND', 'MATCH').replace('ANOTE', 'WHERE')).single()
            if result is None:
                session.run(posassertion.replace('COMMAND', 'CREATE').replace('ANOTE', 'SET'))

    def record_assertion_factoids(self):
        """To be run after everything else is done. Creates the assertion record for all assertions created here,
        tying each to the factoid or person record that originated it and tying all the assertion records to the
        database creation event."""
        e31 = self.constants.get_label('E31')  # Document
        e52 = self.constants.get_label('E52')  # Time-Span
        f2 = self.constants.get_label('F2')  # DB record, both ours and PBW's
        f28 = self.constants.get_label('F28')  # Expression Creation
        p4 = self.constants.get_label('P4')  # has time-span
        p14 = self.constants.get_label('P14')  # carried out by
        p70 = self.constants.get_label('P70')  # documents
        p80 = self.constants.get_label('P80')  # end is qualified by
        r17 = self.constants.get_label('R17')  # created
        r76 = self.constants.get_label('R76')  # is derivative of
        with self.constants.graphdriver.session() as session:
            tla = self.get_authority_node([self.constants.ta])
            timestamp = datetime.now(timezone.utc).isoformat()
            # NOTE we will have to remove the a.origsource attribute later
            findnewassertions = "MATCH (tla) WHERE tla.uuid = '%s' " \
                                "CREATE (dbr:%s)-[:%s {role:'recorder'}]->(tla), " \
                                "(dbr)-[:%s]->(ts:%s {%s: datetime('%s')}) " \
                                "WITH tla, dbr " \
                                "MATCH (a) WHERE a.origsource IS NOT NULL AND NOT (a)<-[:%s]-(:%s) " \
                                "MERGE (orig:%s {uri:a.origsource}) " \
                                "CREATE (a)<-[:%s]-(d:%s:%s)<-[:%s]-(dbr), (d)-[:%s]->(orig) " \
                                "REMOVE a.origsource " \
                                "RETURN dbr, count(d) as newrecords" % (tla,
                                                                        f28, p14,
                                                                        p4, e52, p80, timestamp,
                                                                        p70, e31,
                                                                        f2,
                                                                        p70, e31, f2, r17, r76)
            result = session.run(findnewassertions).single()
            new_assertions = result.get('newrecords', 0)
            print("*** Created %d new assertions ***" % new_assertions)
            if new_assertions == 0:
                # go back and delete the db record
                session.run('MATCH (dbr) WHERE dbr.timestamp = "%s" DELETE dbr' % timestamp)
            else:
                # make sure the URI is set for all nodes and remove their UUIDs
                result = session.run('MATCH (n:Resource) WHERE n.uri IS NULL '
                                     'SET n.uri = "https://r11.eu/rdf/resource/" + n.uuid '
                                     'REMOVE n.uuid RETURN COUNT(n) AS nct').single()
                if not result:
                    warn("Something went wrong setting URIs!")
                # clean out the convenience properties
                session.run('MATCH (n:Resource) REMOVE n.pbwid REMOVE n.prefix')

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
    starttime = datetime.now()
    # Process the person records
    gimport = graphimportSTAR()
    gimport.process_persons()
    duration = datetime.now() - starttime
    print("Done! Ran in %s" % str(duration))
