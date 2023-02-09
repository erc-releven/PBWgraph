import pbw
import RELEVEN.PBWstarConstants
import config
import re
from datetime import datetime, timezone
from sqlalchemy import create_engine, and_
from sqlalchemy.orm import sessionmaker
from neo4j import GraphDatabase
from warnings import warn

# Global variable for our constants object
constants = None


def escape_text(t):
    """Escape any single quotes or double quotes in strings that need to go into Neo4J properties"""
    return t.replace("'", "\\'").replace('"', '\\"')


def collect_person_records():
    """Get a list of people whose floruit matches our needs"""
    # relevant = [x for x in constants.sqlsession.query(pbw.Person).all()
    #             if constants.inrange(x.floruit) and len(x.factoids) > 0]
    # # Add the corner cases that we want to include: two emperors and a hegoumenos early in his career
    # for name, code in [('Konstantinos', 8), ('Romanos', 3), ('Neophytos', 107)]:
    #     relevant.append(constants.sqlsession.query(pbw.Person).filter_by(name=name, mdbCode=code).scalar())
    # print("Found %d relevant people" % len(relevant))
    # return relevant
    # Debugging / testing: restrict the list of relevant people
    debugnames = ['Anna', 'Apospharios', 'Balaleca', 'Herve', 'Ioannes', 'Konstantinos', 'Liparites']
    debugcodes = [62, 64, 68, 101, 102, 110]
    return constants.sqlsession.query(pbw.Person).filter(
        and_(pbw.Person.name.in_(debugnames), pbw.Person.mdbCode.in_(debugcodes))
    ).all()


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


def _create_assertion_query(factoid, subj, pred, obj, auth, src, var="a"):
    """Create the query pattern for an assertion with the given connections. Use 'var' to control
    the variable name for the assertion. Attempts to build the query with specific information first,
    assuming that plain node variable names indicate an already known node."""
    apreds = {'subj': '[:%s]' % constants.star_subject,
              'pred': '[:%s]' % constants.star_predicate,
              'obj': '[:%s]' % constants.star_object,
              'auth': '[:%s]' % constants.star_auth,
              'src': '[:%s]' % constants.star_source}
    # Do the subject and object first, then source, authority and predicate
    # as search area probably increases for each in that order
    anodes = [('auth', auth), ('pred', pred)]
    if src is not None:
        anodes.insert(0, ('src', src))
    if re.match(r'^\w+$', obj):
        anodes.insert(0, ('obj', obj))
    else:
        anodes.append(('obj', obj))
    if re.match(r'^\w+$', subj):
        anodes.insert(0, ('subj', subj))
    else:
        anodes.append(('subj', subj))

    # Now build the query using the order in anodes
    afromfact = 'https://pbw2016.kdl.kcl.ac.uk/rdf/' + factoid
    ameta = ':%s {origsource:"%s"}' % (constants.get_label('E13'), afromfact)
    aclassed = False
    aqparts = []
    for nt in anodes:
        aqparts.append("(%s%s)-%s->(%s)" % (var, ameta if not aclassed else '', apreds[nt[0]], nt[1]))
        aclassed = True
    return "COMMAND %s " % ", ".join(aqparts)


def gender_handler(agent, sqlperson, graphperson):
    uncertain = False
    orig = 'person/%d' % sqlperson.personKey
    pbw_sex = sqlperson.sex
    if pbw_sex == 'Mixed':  # we have already excluded Anonymi
        pbw_sex = 'Unknown'
    elif pbw_sex == 'Eunach':  # correct misspelling in source DB
        pbw_sex = 'Eunuch'
    elif pbw_sex == '(Unspecified)':
        pbw_sex = 'Unknown'
    elif pbw_sex == 'Eunuch (Probable)':
        pbw_sex = 'Eunuch'
        uncertain = True
    if uncertain:
        assertion_props = ' {uncertain:true}'
    else:
        assertion_props = ''
    if pbw_sex != "Unknown":
        # print("...setting gender assignment to %s%s" % (pbw_sex, " (maybe)" if uncertain else ""))
        # Make the event tied to this person
        genderassertion = "MATCH (p), (s), (pbw), (sp41) " \
                          "WHERE p.uuid = '%s' AND s.uuid = '%s' AND pbw.uuid = '%s' AND sp41.uuid = '%s' " % \
                          (graphperson, constants.get_gender(pbw_sex), agent, constants.get_predicate('P41'))
        # Have to add the Resource tag here manually, since we are making a custom predicate node
        genderassertion += "MERGE (sp42:Resource:%s%s) " % (constants.get_label('P42'), assertion_props)
        genderassertion += "WITH p, s, pbw, sp41, sp42 "
        genderassertion += _create_assertion_query(orig, 'ga:%s' % constants.get_label('E17'),
                                                   'sp41', 'p', 'pbw', None, 'a1')
        genderassertion += _create_assertion_query(orig, 'ga', 'sp42', 's', 'pbw', None, 'a2')
        genderassertion += "RETURN a1, a2"
        # print(genderassertion)
        with constants.graphdriver.session() as session:
            result = session.run(genderassertion.replace('COMMAND', 'MATCH')).single()
            if result is None:
                session.run(genderassertion.replace('COMMAND', 'CREATE'))


def identifier_handler(agent, sqlperson, graphperson):
    """The identifier in this context is the 'origName' field, thus an identifier assigned by PBW
    not on the basis of any particular source. We turn this into an Appellation assertion"""
    orig = 'person/%d' % sqlperson.personKey
    # Strip any parenthetical from the nameOL field
    withparen = re.search(r'(.*)\s+\(.*\)', sqlperson.nameOL)
    if withparen is not None:
        appellation = withparen.group(1)
    else:
        appellation = sqlperson.nameOL.rstrip()
    idassertion = "MATCH (p), (pbw), (pred) WHERE p.uuid = '%s' AND pbw.uuid = '%s' AND pred.uuid = '%s' " \
                  % (graphperson, agent, constants.get_predicate('P1'))
    idassertion += "MERGE (app:%s {value: \"%s\"}) " % (constants.get_label('E41'), appellation)
    idassertion += "WITH p, pbw, pred, app "
    idassertion += _create_assertion_query(orig, 'p', 'pred', 'app', 'pbw', None)
    idassertion += "RETURN a"
    with constants.graphdriver.session() as session:
        result = session.run(idassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(idassertion.replace('COMMAND', 'CREATE'))


def disambiguation_handler(agent, sqlperson, graphperson):
    """The short description of the person provided by PBW"""
    orig = 'person/%d' % sqlperson.personKey
    disassertion = "MATCH (p), (pbw), (pred) WHERE p.uuid = '%s' AND pbw.uuid = '%s' AND pred.uuid = '%s' " % \
                   (graphperson, agent, constants.get_predicate('P3'))
    disassertion += "MERGE (desc:%s {value:\"%s\"}) " % (constants.get_label('E62'), escape_text(sqlperson.descName))
    disassertion += "WITH p, pred, desc, pbw "
    disassertion += _create_assertion_query(orig, 'p', 'pred', 'desc', 'pbw', None)
    disassertion += "RETURN a"
    with constants.graphdriver.session() as session:
        result = session.run(disassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(disassertion.replace('COMMAND', 'CREATE'))


def _find_or_create_event(person, eventclass, predicate):
    """Helper function to find the relevant event for event-based factoids"""
    query = "MATCH (pers), (pred) WHERE pers.uuid = '%s' AND pred.uuid = '%s' " % (person, predicate)
    query += "MATCH (a:%s)-[:%s]->(event:%s), " % (constants.get_label('E13'), constants.star_subject, eventclass)
    query += "(a)-[:%s]->(pred), " % constants.star_predicate
    query += "(a)-[:%s]->(pers) " % constants.star_object
    query += "RETURN DISTINCT event"  # There may well be multiple assertions about this event
    with constants.graphdriver.session() as session:
        result = session.run(query).single()
        if result is None:
            # If we don't have an event of this class tied to this person yet, create a new one,
            # look it up again for its UUID, and return it for use in the assertion being made about it.
            session.run("CREATE (event:%s {justcreated:true}) RETURN event" % eventclass)
            result = session.run("MATCH (event:%s {justcreated:true}) REMOVE event.justcreated RETURN event"
                                 % eventclass).single()
        return result['event'].get('uuid')


def get_source_and_agent(session, factoid):
    """Returns a node that represents the source for this factoid. Creates the network of nodes and
    relationships to describe that source, if necessary. The source will either be a physical E22 Human-Made Object
    (the boulloterion) or an E31 Document (the written primary source)."""
    # Is this a 'seals' source without a boulloterion? If so warn and return None
    author = None
    orig = "factoid/%d" % factoid.factoidKey
    if constants.authorities(factoid.source) is None:
        if factoid.source != 'Seals' or factoid.boulloterion is None:
            warn("No boulloterion found for seal-sourced factoid %d" % factoid.factoidKey
                 if factoid.source == 'Seals'
                 else "Source %s of factoid %d not known" % (factoid.source, factoid.factoidKey))
            return None, None
    if factoid.boulloterion is not None:
        # This factoid is taken from a seal inscription. Let's pull that out into CRM objects.
        # If the boulloterion has no associated publications, we shouldn't use it.
        if len(factoid.boulloterion.publication) == 0:
            warn("No published source found for boulloterion %d" % factoid.boulloterion.boulloterionKey)
            return None, None
        # First find who did the analysis
        alist = dict()  # It would be a set if we could put dicts in sets
        for pub in factoid.boulloterion.publication:
            # If the publication isn't in the authority list, Michael analysed it
            if pub.bibSource is not None:
                auths = constants.authorities(pub.bibSource.shortName) or [constants.mj]
                for a in auths:
                    alist[a['identifier']] = a
        agent = get_authority_node(session, list(alist.values()))
        # Then get the node that points to the boulloterion's sources
        srclist = get_boulloterion_sourcelist(session, factoid.boulloterion)
        if srclist is not None:
            q = "MATCH (pred), (agent), (srclist) WHERE pred.uuid = '%s' AND agent.uuid = '%s' " \
                "AND srclist.uuid = '%s' " \
                % (constants.get_predicate('P128'), agent, srclist)
        else:
            q = "MATCH (pred), (agent) WHERE pred.uuid = '%s' AND agent.uuid = '%s' " \
                % (constants.get_predicate('P128'), agent)
        # boulloterion is an E22 Human-Made Object
        q += "MERGE (boul:%s {reference:%s}) " % (constants.get_label('E22'), factoid.boulloterion.boulloterionKey)
        # which is asserted by the agent to P128 carry an E34 Inscription (we can even record the inscription)
        q += "MERGE (src:%s {text:\"%s\"}) " \
             % (constants.get_label('E34'), factoid.boulloterion.origLText)
        q += "WITH boul, pred, src, agent%s " % (", srclist" if srclist else "")
        q += _create_assertion_query(orig, "boul", "pred", "src", "agent", "srclist" if srclist else None)
        # MAYBE LATER: which is asserted by MJ to P108 have produced various E22 Human-Made Objects (the seals)
        # - which seals are asserted by the collection authors (with pub source) to also carry the inscriptions?
    else:
        # This factoid is taken from a document.
        # Do we have a known author for this text?
        workinfo = constants.source(factoid.source)
        authorlist = workinfo.get('author', [])
        author = get_author_node(session, authorlist.copy())
        # If not, we use the PBW scholar as the authority.
        agent = get_authority_node(session, workinfo.get('authority'))
        # If there is no PBW scholar known for this source, we use the generic PBW agent.
        if agent is None:
            agent = constants.pbw_agent
        # Now we find an F2 Expression (of the whole work), its author (if any), and the PBW scholar who analyzed it
        work = get_source_work_expression(session, factoid, workinfo, author)
        q = "MATCH (work), (agent), (p165) WHERE work.uuid = '%s' AND agent.uuid = '%s' AND p165.uuid = '%s' " % (
            work, agent, constants.get_predicate('P165'))
        q += "MERGE (src:%s {reference:'%s', text:'%s'}) " % \
             (constants.get_label('F2'), escape_text(factoid.sourceRef), escape_text(factoid.origLDesc))
        q += "WITH work, p165, src, agent "
        # The agent (whoever worked on the source) asserts that the expression is from the given work.
        q += _create_assertion_query(orig, "work", "p165", "src", "agent", None)
    q += "RETURN src"
    source_result = session.run(q.replace('COMMAND', 'MATCH')).single()
    if source_result is None:
        # To run it right, run it twice
        session.run(q.replace('COMMAND', 'CREATE'))
        source_result = session.run(q.replace('COMMAND', 'MATCH')).single()
    return source_result['src'].get('uuid'), author if author else agent


def get_boulloterion_sourcelist(session, boulloterion):
    """A helper function to create the list of publications where the seals allegedly produced by a
    given boulloterion were published. Returns either a single F2 Expression (if there was a single
    publication) or an E73 Information Object that represents a collection of Expressions."""
    if len(boulloterion.publication) == 0:
        return None
    # Get some labels
    f2 = constants.get_label('F2')
    e73 = constants.get_label('E73')
    p165 = constants.get_label('P165')
    i = 0
    q = ""
    for pub in boulloterion.publication:
        # Make sure with 'merge' that each publication node exists
        q += "MERGE (src%d:%s {identifier:'%s', reference:'%s'}) " % \
            (i, f2, pub.bibSource.shortName, pub.publicationRef)
        i += 1
    if i > 1:
        # Check to see whether we have a matching group with only these publication nodes.
        parts = []
        retvar = "srcgrp"
        q += "WITH %s " % ", ".join(["src%d" % x for x in range(i)])
        # This size syntax taken blindly from
        # https://stackoverflow.com/questions/68785613/neo4j-4-3-deprecation-of-size-function-and-pattern-comprehension
        q += "MATCH (srcgrp:%s) WHERE size([(srcgrp)-[:%s]->(:%s) | srcgrp]) = %d " \
             % (e73, p165, f2, i)
        for n in range(i):
            parts.append("(srcgrp)-[:%s]->(src%d)" % (p165, n))
        q += "MATCH " + ", ".join(parts) + " "
    else:
        # We simply return the one node we created
        retvar = "src0"
    q += "RETURN %s" % retvar
    ret = session.run(q).single()
    if ret is None:
        # The plural sources exist, but the source group doesn't. Create it
        i = 0
        matchparts = []
        createparts = ["(srcgrp:%s)" % e73]
        for pub in boulloterion.publication:
            matchparts.append("(src%d:%s {identifier:'%s', reference:'%s'}) " % (
                i, f2, pub.bibSource.shortName, pub.publicationRef))
            createparts.append("(srcgrp)-[:%s]->(src%d)" % (p165, i))
            i += 1
        qc = "MATCH " + ", ".join(matchparts) + " "
        qc += "CREATE " + ", ".join(createparts) + " "
        qc += "RETURN srcgrp"
        ret = session.run(qc).single()
    if 'uuid' not in ret[retvar]:
        # We have to go fishing for the thing again; we reuse the MATCH / MERGE statements.
        ret = session.run(q).single()
    return ret[retvar].get('uuid')


def get_source_work_expression(session, factoid, workinfo, author):
    # Pass in the factoid, the dictionary describing the work, and the node we have already created
    # for the author of the work, if applicable.
    # Ensure the existence of the work and, if it has a declared author, link the author to it via
    # a CREATION event, asserted by the author.
    # TODO we need to properly record all works we use, not just those that have known authors
    if workinfo is None:
        workinfo = dict()
    identifier = workinfo.get('work', factoid.source)
    orig = 'factoid/%d' % workinfo.get('factoid', 0)
    expression = workinfo.get('expression', identifier)
    q = "COMMAND (work:%s {identifier:'%s'})-[:%s]->(expr:%s {identifier:'%s'}) " \
        % (constants.get_label('F1'), escape_text(identifier), constants.get_label('R3'),
           constants.get_label('F2'), escape_text(expression))
    if author is not None:
        # Ensure the existence of the assertions that the author authored the work, and create the
        # expression of the work for segmentation into source references
        q += "WITH work, expr "
        q += "MATCH (author), (p14), (r16) " \
             "WHERE author.uuid = '%s' AND p14.uuid = '%s' AND r16.uuid = '%s' " % (
                author, constants.get_predicate('P14'), constants.get_predicate('R16'))
        # TODO we need to fill out these 'None' sources later with what is in workinfo, but that can get recursive
        q += _create_assertion_query(orig, 'aship:%s' % constants.get_label('F1'), 'r16', 'work', 'author', None, 'a1')
        q += _create_assertion_query(orig, 'aship', 'p14', 'author', 'author', None, 'a2')
    q += "RETURN expr"
    work_result = session.run(q.replace('COMMAND', 'MATCH')).single()
    if work_result is None:
        session.run(q.replace('COMMAND', 'CREATE'))
        work_result = session.run(q.replace('COMMAND', 'MATCH')).single()
    return work_result['expr'].get('uuid')


def _find_or_create_identified_person(agent, identifier, dname):
    """Return an E21 Person, labeled with the name and code via an E14 Identifier Assignment carried out
    by the given agent (so far either PBW or VIAF.)"""
    # We can't merge with comma statements, so we have to do it with successive one-liners.
    # Start the merge from the specific information we have, which is the identifier itself.
    if 'Byzantine' in agent:
        url = 'https://pbw2016.kdl.kcl.ac.uk/person/' + identifier.replace(' ', '/')
    else:
        url = 'https://viaf.org/viaf/%s/' % identifier
    nodelookup = "MATCH (coll) WHERE coll.uuid = '%s' " \
                 "MERGE (idlabel:%s {value:'%s', url:'%s'}) " \
                 "MERGE (coll)<-[:%s]-(idass:%s)-[:%s]->(idlabel) " \
                 "MERGE (idass)-[:%s]->(p:%s {descname:'%s'}) RETURN p" % \
                 (agent,
                  constants.get_label('E42'), escape_text(identifier), url,
                  constants.get_label('P14'), constants.get_label('E15'), constants.get_label('P37'),
                  constants.get_label('P140'), constants.get_label('E21'), escape_text(dname))
    with constants.graphdriver.session() as session:
        graph_person = session.run(nodelookup).single()['p']
        if 'uuid' not in graph_person:
            # do it again to get the UUID that was set
            graph_person = session.run(nodelookup).single()['p']
    return graph_person.get('uuid')


def _find_or_create_pbwperson(name, code, displayname):
    return _find_or_create_identified_person(constants.pbw_agent, "%s %d" % (name, code), displayname)


def _find_or_create_viafperson(name, viafid):
    return _find_or_create_identified_person(constants.viaf_agent, viafid, name)


def get_author_node(session, authorlist):
    """Return the E21 Person node for the author of a text, or a group of authors if authorship was composite"""
    if len(authorlist) == 0:
        return None
    authors = []
    while len(authorlist) > 0:
        pname = authorlist.pop(0)
        pcode = authorlist.pop(0)
        # We need to get the SQL record for the author in case they aren't in the DB yet
        sqlperson = constants.sqlsession.query(pbw.Person).filter_by(name=pname, mdbCode=pcode).scalar()
        authors.append(_find_or_create_pbwperson(pname, pcode, sqlperson.descName))
    if len(authors) > 1:
        # It is our multi-authored text. Make a group because both authors share authority.
        return _find_or_create_authority_group(session, authors)
    else:
        return authors[0]


def get_authority_node(session, authoritylist):
    """Create or find the node for the given authority in our (modern) authority list."""
    if authoritylist is None or len(authoritylist) == 0:
        return None
    if len(authoritylist) == 1:
        authority = authoritylist[0]
        return _find_or_create_viafperson(authority['identifier'], authority['viaf'])
    # If we get here, we have more than one authority for this source.
    # Ensure the existence of the people, and then ensure the existence of their group
    scholars = []
    for p in authoritylist:
        scholars.append(_find_or_create_viafperson(p['identifier'], p['viaf']))
    return _find_or_create_authority_group(session, scholars)


def _find_or_create_authority_group(session, members):
    mc = []
    wc = []
    gc = []
    i = 1
    for m in members:
        mc.append("(m%d)" % i)
        wc.append("m%d.uuid = '%s'" % (i, m))
        gc.append("(group)-[:%s]->(m%d)" % (constants.get_label('P107'), i))
        i += 1
    q = "MATCH " + ', '.join(mc) + " WHERE " + " AND ".join(wc) + " "
    q += "COMMAND (group:%s), %s RETURN group" % (constants.get_label('E74'), ", ".join(gc))
    g = session.run(q.replace('COMMAND', 'MATCH')).single()
    if g is None:
        # Run it twice to get the UUID assigned
        session.run(q.replace('COMMAND', 'CREATE'))
        g = session.run(q.replace('COMMAND', 'MATCH')).single()
    return g['group'].get('uuid')


def _get_source_lang(factoid):
    lkeys = {2: 'gr', 3: 'la', 4: 'ar', 5: 'hy'}
    try:
        return lkeys.get(factoid.oLangKey)
    except NameError:
        return None


def appellation_handler(sourcenode, agent, factoid, graphperson):
    """This handler deals with Second Name factoids and also Alternative Name factoids.
    The Second Names might be in all sorts of languages in the factoid itself, but refer
    to a canonical version of the name in the FamilyName table, which is probably usually
    Greek. The Alternative Name factoids should exclusively use the information in the
    base factoid."""
    orig = "factoid/%d" % factoid.factoidKey
    appassertion = "MATCH (p), (agent), (source), (pred) " \
                   "WHERE p.uuid = '%s' AND agent.uuid = '%s' AND source.uuid = '%s' AND pred.uuid = '%s' " % (
                       graphperson, agent, sourcenode, constants.get_predicate('P1'))
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
            olang = _get_source_lang(factoid.secondName) or 'gr'
        else:
            name_en = factoid.engDesc
            name_ol = factoid.origLDesc
            olang = _get_source_lang(factoid) or 'gr'
        print("Adding second name %s (%s '%s')" % (name_en, olang, name_ol))

    appassertion += "MERGE (n:%s {en:'%s', %s:'%s'}) " % (
        constants.get_label('E41'), escape_text(name_en), olang, escape_text(name_ol))
    appassertion += "WITH p, agent, source, pred, n "
    appassertion += _create_assertion_query(orig, 'p', 'pred', 'n', 'agent', 'source')
    appassertion += "RETURN a"
    with constants.graphdriver.session() as session:
        result = session.run(appassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(appassertion.replace('COMMAND', 'CREATE'))


def death_handler(sourcenode, agent, factoid, graphperson):
    # Each factoid is its own set of assertions pertaining to the single death of a person.
    # When there are multiple sources, we will have to review them for consistency and make
    # proxies for the death event as necessary.
    orig = "factoid/%d" % factoid.factoidKey
    # See if we can find the death event
    deathevent = _find_or_create_event(graphperson, constants.get_label('E69'), constants.get_predicate('P100'))
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

    deathdate = factoid.deathRecord.sourceDate
    if deathdate == '':
        deathdate = None
    deathassertion = "MATCH (p), (agent), (source), (devent), (p100), (p4) " \
                     "WHERE p.uuid = '%s' AND agent.uuid = '%s' AND source.uuid = '%s' AND devent.uuid = '%s' " \
                     "AND p100.uuid = '%s' AND p4.uuid = '%s' " \
                     % (graphperson, agent, sourcenode, deathevent, constants.get_predicate('P100'),
                        constants.get_predicate('P4'))
    if deathdate is not None:
        # Just record the string; many of them don't resolve to a fixed date
        deathassertion += "MERGE (datedesc:%s {content:\"%s\"}) " % (constants.get_label('E52'), deathdate)
    deathassertion += "WITH p, agent, source, devent, p100, p4%s " % (', datedesc' if deathdate else '')
    # Create an assertion that the death happened
    deathassertion += _create_assertion_query(orig, 'devent', 'p100', 'p', 'agent', 'source')
    # Create an assertion about when the death happened.
    if deathdate is not None:
        deathassertion += _create_assertion_query(orig, 'devent', 'p4', 'datedesc', 'agent', 'source', 'a2')
    # Set the death description on the assertion as a P3 note. This should be idempotent...
    deathassertion += "SET a.%s = \"%s\" " % (constants.get_label('P3'), escape_text(factoid.replace_referents()))
    deathassertion += "RETURN a%s" % (", a2" if deathdate else '')

    # print(deathassertion)
    with constants.graphdriver.session() as session:
        result = session.run(deathassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(deathassertion.replace('COMMAND', 'CREATE'))


def ethnicity_handler(sourcenode, agent, factoid, graphperson):
    """Assign a group membership for the given ethnicity to the person"""
    orig = "factoid/%d" % factoid.factoidKey
    if factoid.ethnicityInfo is None or factoid.ethnicityInfo.ethnicity is None:
        # We can't assign any ethnicity without the ethnicity info
        warn("Empty ethnicity factoid found: id %s" % factoid.factoidKey)
        return
    elabel = factoid.ethnicityInfo.ethnicity.ethName
    groupid = constants.get_ethnicity(elabel)
    gassertion = "MATCH (p), (agent), (source), (group), (mpred)  " \
                 "WHERE p.uuid = '%s' AND agent.uuid = '%s' AND source.uuid = '%s' AND group.uuid = '%s' " \
                 "AND mpred.uuid = '%s' " % \
                 (graphperson, agent, sourcenode, groupid,
                  constants.get_predicate('P107'))
    gassertion += _create_assertion_query(orig, 'group', 'mpred', 'p', 'agent', 'source')
    gassertion += "RETURN a"
    with constants.graphdriver.session() as session:
        result = session.run(gassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(gassertion.replace('COMMAND', 'CREATE'))


# Helper to create the assertions for our various social designation groups
def _find_or_create_social_designation(sourcenode, agent, factoid, graphperson, des, label, whopred, whichpred):
    # (grouping:label) [:whopred] person
    # (grouping) [:whichpred] rnode
    orig = "factoid/%d" % factoid.factoidKey
    gassertion = "MATCH (p), (agent), (source), (designation), (whopred), (whichpred) " \
                 "WHERE p.uuid = '%s' AND agent.uuid = '%s' AND source.uuid = '%s' AND designation.uuid = '%s' " \
                 "AND whopred.uuid = '%s' AND whichpred.uuid = '%s' " \
                 % (graphperson, agent, sourcenode, des, whopred, whichpred)
    gassertion += _create_assertion_query(orig, 'persondes:%s' % label, 'whopred', 'p', 'agent', 'source')
    gassertion += _create_assertion_query(orig, 'persondes', 'whichpred', 'designation', 'agent', 'source', 'a1')
    gassertion += "RETURN a"
    with constants.graphdriver.session() as session:
        result = session.run(gassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(gassertion.replace('COMMAND', 'CREATE'))


def religion_handler(sourcenode, agent, factoid, graphperson):
    """Assign a group membership for the given religious confession to the person"""
    if factoid.religion is None:
        warn("Empty religion factoid found: id %d" % factoid.factoidKey)
        return
    rlabel = factoid.religion
    # Special case, database had an error
    if factoid.religion == '':
        rlabel = 'Heretic'
    relid = constants.get_religion(rlabel)
    # (r:C23 Religious identity) [rwho:P36 pertains to] person
    # (r:C23 Religious identity) [rwhich:P35 is defined by] rnode
    _find_or_create_social_designation(sourcenode, agent, factoid, graphperson, relid, constants.get_label('C23'),
                                       constants.get_predicate('SP36'), constants.get_predicate('SP35'))


def societyrole_handler(sourcenode, agent, factoid, graphperson):
    if factoid.occupation is None:
        return
    roleid = constants.get_societyrole(factoid.occupation)
    roletype = constants.get_label('C1')
    whopred = constants.get_predicate('SP13')
    whichpred = constants.get_predicate('SP14')
    if factoid.occupation in constants.legal_designations:
        # We need to treat it as a legal role instead of an occupation
        roletype = constants.get_label('C13')
        whopred = constants.get_predicate('SP26')
        whichpred = constants.get_predicate('SP33')
    # (r:C1 Social Quality of an Actor) [rwho:P13 pertains to] person
    # (r:C1) [rwhich:P14 is defined by] rnode
    _find_or_create_social_designation(sourcenode, agent, factoid, graphperson, roleid, roletype, whopred, whichpred)


def dignity_handler(sourcenode, agent, factoid, graphperson):
    if factoid.dignityOffice is None:
        return
    dignity_id = constants.get_dignity(factoid.dignityOffice.stdName)
    # We treat dignities as legal roles
    # (r:C13 Social Role Embodiment) [dwho:P26 is embodied by] person
    # (r:C13) [dwhich:P33 is embodiment of] dignity
    _find_or_create_social_designation(sourcenode, agent, factoid, graphperson, dignity_id, constants.get_label('C13'),
                                       constants.get_predicate('SP26'), constants.get_predicate('SP33'))


def languageskill_handler(sourcenode, agent, factoid, graphperson):
    """Assign a language skill to the person"""
    orig = "factoid/%d" % factoid.factoidKey
    if factoid.languageSkill is None:
        return
    lkhid = constants.get_language(factoid.languageSkill)
    # This doesn't chain quite the same way as the others
    # person [rwho:P38 has skill] (r:C21 Skill)
    # (r:C21 Skill) [rwhich:P37 concerns] (l:C29 Know-How)
    lassertion = "MATCH (p), (agent), (source), (lkh), (lwhich), (lwho) " \
                 "WHERE p.uuid = '%s' AND agent.uuid = '%s' AND source.uuid = '%s' AND lkh.uuid = '%s' " \
                 "AND lwhich.uuid = '%s' AND lwho.uuid = '%s' " \
                 % (graphperson, agent, sourcenode, lkhid,
                    constants.get_predicate('SP37'), constants.get_predicate('SP38'))
    lassertion += _create_assertion_query(orig, 'p', 'lwho', 'ls:%s' % constants.get_label('C21'), 'agent', 'source')
    lassertion += _create_assertion_query(orig, 'ls', 'lwhich', 'lkh', 'agent', 'source', 'a1')
    lassertion += "RETURN a"
    with constants.graphdriver.session() as session:
        result = session.run(lassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(lassertion.replace('COMMAND', 'CREATE'))


def _find_or_create_kinship(graphperson, graphkin):
    # See if there is an existing kinship group between the person and their kin according to the
    # source in question. If not, return a new (not yet connected) E74 Kinship group node.
    e13 = constants.get_label('E13')
    e74 = constants.get_label('E74')
    p140 = constants.get_label('P140')
    p141 = constants.get_label('P141')
    kinquery = "MATCH (p), (kin) WHERE p.uuid = '%s' AND kin.uuid = '%s' " % (graphperson, graphkin)
    kinquery += "MATCH (p)<-[:%s]-(a1:%s)-[:%s]->(kg:%s)<-[:%s]-(a2)-[:%s]->(kin) " \
                "RETURN DISTINCT kg" % (p141, e13, p140, e74, p140, p141)
    with constants.graphdriver.session() as session:
        result = session.run(kinquery).single()
        if result is None:
            # If the kinship pair hasn't been referenced yet, then create a new empty kinship and return it
            # for use in the assertions below.
            session.run("CREATE (kg:%s {justcreated:true}) RETURN kg" % e74)
            result = session.run("MATCH (kg:%s {justcreated:true}) REMOVE kg.justcreated RETURN kg" % e74).single()
        return result['kg'].get('uuid')


def kinship_handler(sourcenode, agent, factoid, graphperson):
    # Kinships are modeled as two-person groups connected with P107 and with .1 types
    # as property attributes as per the CRM spec. A kinship has to be modeled thus as
    # two assertions, one for each person's membership of the group. The main person
    # has the specific predicate in their assertion; the secondary person has the
    # generic predicate.
    # TODO deal with kin who are Anonymi or Anonymae
    orig = "factoid/%d" % factoid.factoidKey
    if factoid.kinshipType is None:
        warn("Empty kinship factoid found: id %d" % factoid.factoidKey)
        return
    predspec = constants.get_kinship(factoid.kinshipType.gspecRelat)
    predgen = constants.get_predicate('P107')
    for kin in factoid.referents():
        graphkin = _find_or_create_pbwperson(kin.name, kin.mdbCode, kin.descName)
        if graphkin == graphperson:
            warn("Person %s listed as related to self" % kin)
            continue
        kgroup = _find_or_create_kinship(graphperson, graphkin)
        kinassertion = "MATCH (p), (kin), (kg), (agent), (source), (pspec), (pgen) WHERE p.uuid = '%s' " \
                       "AND kin.uuid = '%s' AND kg.uuid = '%s' AND agent.uuid = '%s' AND source.uuid = '%s' " \
                       "AND pspec.uuid = '%s' AND pgen.uuid = '%s' " \
                       % (graphperson, graphkin, kgroup, agent, sourcenode, predspec, predgen)
        kinassertion += _create_assertion_query(orig, 'kg', 'pspec', 'p', 'agent', 'source', 'a1')
        kinassertion += _create_assertion_query(orig, 'kg', 'pgen', 'kin', 'agent', 'source', 'a2')
        kinassertion += "RETURN a1, a2"
        with constants.graphdriver.session() as session:
            result = session.run(kinassertion.replace('COMMAND', 'MATCH')).single()
            if result is None:
                session.run(kinassertion.replace('COMMAND', 'CREATE'))


def possession_handler(sourcenode, agent, factoid, graphperson):
    """Ensure the existence of an E18 Physical Thing (we don't have any more category info about
    the possessions). For now, we assume that a possession with an identical description is, in fact,
    the same possession."""
    orig = "factoid/%d" % factoid.factoidKey
    possession_attributes = {constants.get_label('P1'): escape_text(factoid.engDesc)}
    if factoid.possession is not None and factoid.possession != '':
        possession_attributes[constants.get_label('P3')] = escape_text(factoid.replace_referents())
    possession_attrs = ", ".join(["%s: '%s'" % (k, v) for k, v in possession_attributes.items()])
    posassertion = "MATCH (p), (agent), (source), (pred) " \
                   "WHERE p.uuid = '%s' AND agent.uuid = '%s' AND source.uuid = '%s' AND pred.uuid = '%s' " % \
                   (graphperson, agent, sourcenode, constants.get_predicate('P51'))
    posassertion += "MERGE (poss:%s {%s}) " % (constants.get_label('E18'), possession_attrs)
    posassertion += "WITH p, agent, source, pred, poss "
    posassertion += _create_assertion_query(orig, 'poss', 'pred', 'p', 'agent', 'source')
    posassertion += "RETURN a"
    with constants.graphdriver.session() as session:
        result = session.run(posassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(posassertion.replace('COMMAND', 'CREATE'))


def record_assertion_factoids():
    """To be run after everything else is done. Creates the assertion record for all assertions created here,
    tying each to the factoid or person record that originated it and tying all the assertion records to the
    database creation event."""
    e13 = constants.get_label('E13')  # Attribute_Assignment
    e31 = constants.get_label('E31')  # Document
    f2 = constants.get_label('F2')    # Expression
    f28 = constants.get_label('F28')  # Expression Creation
    p14 = constants.get_label('P14')  # carried out by
    p70 = constants.get_label('P70')  # documents
    r17 = constants.get_label('R17')  # created
    r76 = constants.get_label('R76')  # is derivative of
    with constants.graphdriver.session() as session:
        tla = get_authority_node(session, [constants.ta])
        timestamp = datetime.now(timezone.utc).isoformat()
        findnewassertions = "MATCH (tla) WHERE tla.uuid = '%s' " \
                            "CREATE (dbr:%s {timestamp:datetime('%s')})-[:%s {role:'recorder'}]->(tla) " \
                            "WITH tla, dbr " \
                            "MATCH (a:%s) WHERE NOT (a)<-[:%s]-(:%s) " \
                            "CREATE (a)<-[:%s]-(d:%s:%s)<-[:%s]-(dbr) " \
                            "SET d.%s = a.origsource REMOVE a.origsource " \
                            "RETURN dbr, count(d) as newrecords" % (tla,
                                                                    f28, timestamp, p14,
                                                                    e13, p70, e31,
                                                                    p70, e31, f2, r17,
                                                                    r76)
        result = session.run(findnewassertions).single()
        new_assertions = result.get('newrecords', 0)
        print("*** Created %d new assertions ***" % new_assertions)
        if new_assertions == 0:
            # go back and delete the db record
            session.run('MATCH (dbr) WHERE dbr.timestamp = "%s" DELETE dbr' % timestamp)


def process_persons():
    """Go through the relevant person records and process them for factoids"""
    processed = 0
    used_sources = set()
    for person in collect_person_records():
        # Skip the anonymous groups for now
        if person.name == 'Anonymi':
            continue
        processed += 1
        # Create or find the person node
        print("*** Making/finding node for person %s %d ***" % (person.name, person.mdbCode))
        graph_person = _find_or_create_pbwperson(person.name, person.mdbCode, person.descName)

        # Get the 'factoids' that are directly in the person record
        for ftype in constants.directPersonRecords:
            ourftype = _smooth_labels(ftype)
            try:
                method = eval("%s_handler" % ourftype.lower())
                method(constants.pbw_agent, person, graph_person)
            except NameError:
                warn("No handler for %s record info; skipping." % ourftype)

        # Now get the factoids that are really factoids
        for ftype in constants.factoidTypes:
            ourftype = _smooth_labels(ftype)
            try:
                method = eval("%s_handler" % ourftype.lower())
                fprocessed = 0
                for f in person.main_factoids(ftype):
                    # Note which sources we actually use, so we know what to record
                    used_sources.add(f.source)
                    # Get the source, either a text passage or a seal inscription, and the authority
                    # for the factoid. Authority will either be the author of the text, or the PBW
                    # colleague who read the text and ingested the information.
                    with constants.graphdriver.session() as session:
                        (source_node, authority_node) = get_source_and_agent(session, f)
                    # If the factoid has no source then we skip it
                    if source_node is None:
                        warn("Skipping factoid %d without a traceable source" % f.factoidKey)
                        continue
                    # If the factoid has no authority then we assign it to the generic PBW agent
                    if authority_node is None:
                        authority_node = constants.pbw_agent
                    # Call the handler for this factoid type
                    method(source_node, authority_node, f, graph_person)
                    fprocessed += 1
                if fprocessed > 0:
                    print("Ingested %d %s factoid(s)" % (fprocessed, ftype))

            except NameError:
                pass
    record_assertion_factoids()
    print("Processed %d person records." % processed)
    print("Used the following sources: %s" % sorted(used_sources))


# If we are running as main, execute the script
if __name__ == '__main__':
    # Connect to the SQL DB
    starttime = datetime.now()
    engine = create_engine('mysql+pymysql://' + config.dbstring)
    smaker = sessionmaker(bind=engine)
    mysqlsession = smaker()
    # Connect to the graph DB
    driver = GraphDatabase.driver(config.graphuri, auth=(config.graphuser, config.graphpw))
    # Make / retrieve the global nodes and constants
    constants = RELEVEN.PBWstarConstants.PBWstarConstants(mysqlsession, driver)
    # Process the person records
    process_persons()
    duration = datetime.now() - starttime
    print("Done! Ran in %s" % str(duration))
