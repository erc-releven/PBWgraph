import pbw
import PBWstarConstants
import config
import datetime
import re
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from neo4j import GraphDatabase
from warnings import warn

# Global variable for our constants object
constants = None


def escape_text(t):
    """Escape any single quotes or double quotes in strings that need to go into Neo4J properties"""
    return t.replace("'", "\\'").replace('"', '\\"')


def collect_person_records(sqlsession):
    """Get a list of people whose floruit matches our needs"""
    relevant = [x for x in sqlsession.query(pbw.Person).all() if constants.inrange(x.floruit) and len(x.factoids) > 0]
    print("Found %d relevant people" % len(relevant))
    # return relevant
    # Debugging / testing: restrict the list of relevant people
    debugnames = ['Herve', 'Ioannes', 'Konstantinos', 'Anna']
    debugcodes = [62, 68, 101, 102]
    return [x for x in relevant if x.name in debugnames and x.mdbCode in debugcodes]


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
    ameta = ':crm__E13_Attribute_Assignment {origsource:"%s"}' % afromfact
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
                          "WHERE id(p) = %d AND id(s) = %d AND id(pbw) = %d AND id(sp41) = %d " % \
                          (graphperson.id, constants.cv['Gender'][pbw_sex],
                           agent.id, constants.get_predicate('P41'))
        genderassertion += "MERGE (sp42:%s%s) " % (constants.get_predicate('P42'), assertion_props)
        genderassertion += "WITH p, s, pbw, sp41, sp42 "
        genderassertion += _create_assertion_query(orig, 'ga:%s' % constants.get_label('E17'),
                                                   'sp41', 'p', 'pbw', None, 'a1')
        genderassertion += _create_assertion_query(orig, 'ga', 'sp42', 's', 'pbw', None, 'a2')
        genderassertion += "RETURN a1, a2"
        # print(genderassertion % (graphperson.id, constants[pbw_sex], agent.id, assertion_props))
        with constants.graphdriver.session() as session:
            result = session.run(genderassertion.replace('COMMAND', 'MATCH')).single()
            if result is None:
                session.run(genderassertion.replace('COMMAND', 'CREATE'))


def identifier_handler(agent, sqlperson, graphperson):
    """The identifier in this context is the 'origName' field, thus an identifier assigned by PBW
    not on the basis of any particular source"""
    orig = 'person/%d' % sqlperson.personKey
    idassertion = "MATCH (p), (pbw), (pred) WHERE id(p) = %d AND id(pbw) = %d AND id(pred) = %d " \
                  % (graphperson.id, agent.id, constants.get_predicate('P1'))
    idassertion += "MERGE (app:%s {value: \"%s\"}) " % (constants.get_label('E41'), sqlperson.nameOL)
    idassertion += "WITH p, pbw, pred, app "
    idassertion += _create_assertion_query(orig, 'p', 'pred', 'app', 'pbw', None)
    idassertion += "RETURN a"
    with constants.graphdriver.session() as session:
        result = session.run(idassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            return session.run(idassertion.replace('COMMAND', 'CREATE'))


def disambiguation_handler(agent, sqlperson, graphperson):
    """The short description of the person provided by PBW"""
    orig = 'person/%d' % sqlperson.personKey
    disassertion = "MATCH (p), (pbw), (pred) WHERE id(p) = %d AND id(pbw) = %d AND id(pred) = %d " % \
                   (graphperson.id, agent.id, constants.get_predicate('P3'))
    disassertion += "MERGE (desc:crm__E62_String {value:\"%s\"}) " % escape_text(sqlperson.descName)
    disassertion += "WITH p, pred, desc, pbw "
    disassertion += _create_assertion_query(orig, 'p', 'pred', 'desc', 'pbw', None)
    disassertion += "RETURN a"
    with constants.graphdriver.session() as session:
        result = session.run(disassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            return session.run(disassertion.replace('COMMAND', 'CREATE'))


def _find_or_create_event(person, eventclass, predicate):
    """Helper function to find the relevant event for event-based factoids"""
    query = "MATCH (pers), (pred) WHERE id(pers) = %d AND id(pred) = %d " % (person.id, predicate)
    query += "MATCH (a:%s)-[:%s]->(event:%s), " % (constants.get_label('P13'), constants.star_subject, eventclass)
    query += "(a)-[:%s]->(pred), " % constants.star_predicate
    query += "(a)-[:%s]->(pers) " % constants.star_object
    query += "RETURN DISTINCT event"  # There may well be multiple assertions about this event
    with constants.graphdriver.session() as session:
        result = session.run(query).single()
        if result is None:
            # If we don't have this event tied to this person yet, create a new event of the
            # given class and return it for use in the assertion being made about it.
            result = session.run("CREATE (event:%s) RETURN event" % eventclass).single()
        return result['event']


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
        alist = set()
        for pub in factoid.boulloterion.publication:
            # If the publication isn't in the authority list, Michael analysed it
            if pub.bibSource is not None:
                thispubauth = constants.authoritylist.get(pub.bibSource.shortName, ["Michael Jeffreys"])
                alist.update(thispubauth)
        agent = get_authority_node(session, list(alist))
        # Then get the node that points to the boulloterion's sources
        srclist = get_boulloterion_sourcelist(session, factoid.boulloterion)
        if srclist is not None:
            q = "MATCH (pred), (agent), (srclist) WHERE id(pred) = %d AND id(agent) = %d AND id(srclist) = %d " \
                % (constants.get_predicate('P128'), agent.id, srclist.id)
        else:
            q = "MATCH (pred), (agent) WHERE id(pred) = %d AND id(agent) = %d " \
                % (constants.get_predicate('P128'), agent.id)
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
        workinfo = constants.author(factoid.source)
        author = get_author_node(session, workinfo.get('author'))
        # If not, we use the PBW scholar as the authority.
        agent = get_authority_node(session, constants.authorities(factoid.source))
        # If there is no PBW scholar known for this source, we use the generic PBW agent.
        if agent is None:
            agent = constants.generic_agent
        # Now we find an F2 Expression (of the whole work), its author (if any), and the PBW scholar who analyzed it
        work = get_source_work_expression(session, factoid, workinfo, author)
        q = "MATCH (work), (agent), (p165) WHERE id(work) = %d AND id(agent) = %d AND id(p165) = %d " % (
            work.id, agent.id, constants.get_predicate('P165'))
        q += "MERGE (src:%s {reference:'%s', text:'%s'}) " % \
             (constants.get_label('F2'), escape_text(factoid.sourceRef), escape_text(factoid.origLDesc))
        q += "WITH work, p165, src, agent "
        q += _create_assertion_query(orig, "work", "p165", "src", "agent", None)
    q += "RETURN src"  # SOMEDAY work out why this query needs a 'distinct'
    source_result = session.run(q.replace('COMMAND', 'MATCH')).single()
    if source_result is None:
        source_result = session.run(q.replace('COMMAND', 'CREATE')).single()
    return source_result['src'], author if author else agent


def get_boulloterion_sourcelist(session, boulloterion):
    """A helper function to create the list of publications where the seals allegedly produced by a
    given boulloterion were published. Returns either a single E31 Document (if there was a single
    publication) or an E73 Information Object that represents a group of Documents."""
    if len(boulloterion.publication) == 0:
        return None
    # Get some labels
    e31 = constants.get_label('E31')
    e73 = constants.get_label('E73')
    p165 = constants.get_label('P165')
    i = 0
    q = ""
    for pub in boulloterion.publication:
        # Make sure with 'merge' that each publication node exists
        q += "MERGE (src%d:%s {identifier:'%s', reference:'%s'}) " % \
            (i, e31, pub.bibSource.shortName, pub.publicationRef)
        i += 1
    if i > 1:
        # Check to see whether we have a matching group with only these publication nodes.
        # Teeeeechnically speaking, an Information Object cannot P70 document anything, so
        # we also assign E31 Document as a stopgap since the group of sources, taken together,
        # documents something.
        parts = []
        retvar = "srcgrp"
        q += "WITH %s " % ", ".join(["src%d" % x for x in range(i)])
        q += "MATCH (srcgrp:%s:%s) WHERE size((srcgrp)-[:%s]->(:%s)) = %d " \
             % (e73, e31, p165, e31, i)
        for n in range(i):
            parts.append("(srcgrp)-[:%s]->(src%d)" % (constants.get_label('P165'), n))
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
        createparts = ["(srcgrp:%s:%s)" % (e73, e31)]
        for pub in boulloterion.publication:
            matchparts.append("(src%d:%s {identifier:'%s', reference:'%s'}) " % (
                i, e31, pub.bibSource.shortName, pub.publicationRef))
            createparts.append("(srcgrp)-[:%s]->(src%d)" % (p165, i))
            i += 1
        q = "MATCH " + ", ".join(matchparts) + " "
        q += "CREATE " + ", ".join(createparts) + " "
        q += "RETURN srcgrp"
        ret = session.run(q).single()
    return ret[retvar]


def get_source_work_expression(session, factoid, workinfo, author):
    # Pass in the factoid, the dictionary describing the work, and the node we have already created
    # for the author of the work, if applicable.
    # Ensure the existence of the work and, if it has a declared author, link the author to it via
    # a CREATION event, asserted by the author.
    # TODO we need to properly record all works we use, not just those that have known authors
    identifier = factoid.source
    orig = 'NONE'
    expression = identifier
    if workinfo is not None:
        identifier = workinfo['work']
        orig = 'factoid/%d' % workinfo.get('factoid', 0)
        expression = workinfo.get('expression', identifier)
    q = "MERGE (work:%s {identifier:'%s'})-[:%s]->(expr:%s {identifier:'%s'}) " \
        % (constants.get_label('F1'), escape_text(identifier), constants.get_label('R3'),
           constants.get_label('F2'), escape_text(expression))
    if author is not None:
        # Ensure the existence of the assertions that the author authored the work, and create the
        # expression of the work for segmentation into source references
        q += "WITH work, expr "
        q += "MATCH (author), (p14), (p94) " \
             "WHERE id(author) = %d AND id(p14) = %d AND id(r16) = %d " % (
                author.id, constants.get_predicate('P14'), constants.get_predicate('R16'))
        # TODO we need to fill out these 'None' sources later with what is in workinfo, but that can get recursive
        q += _create_assertion_query(orig, 'aship:%s' % constants.get_label('F1'), 'r16', 'work', 'author', None, 'a1')
        q += _create_assertion_query(orig, 'aship', 'p14', 'author', 'author', None, 'a2')
    q += "RETURN expr"
    work_result = session.run(q.replace('COMMAND', 'MATCH')).single()
    if work_result is None:
        work_result = session.run(q.replace('COMMAND', 'CREATE')).single()
    return work_result['expr']


def get_author_node(session, authorlist):
    """Return the E21 Person node for the author of a text, or a group of authors if authorship was composite"""
    if authorlist is None or len(authorlist) == 0:
        return None
    if len(authorlist) == 2:  # It is a single name and mdbCode
        return _find_or_create_graphperson(authorlist[0], authorlist[1])
    # It is our multi-authored text. Make a group because both authors share authority.
    authors = []
    while len(authorlist) > 0:
        pname = authorlist.pop(0)
        pcode = authorlist.pop(1)
        authors.append(_find_or_create_graphperson(pname, pcode))
    return _find_or_create_authority_group(session, authors)


def get_authority_node(session, authoritylist):
    if authoritylist is None or len(authoritylist) == 0:
        return None
    if len(authoritylist) == 1:
        return session.run("MERGE (p:%s {identifier:'%s'}) RETURN p"
                           % (constants.get_label('E21'), authoritylist[0])).single()['p']
    # If we get here, we have more than one authority for this source.
    # Ensure the existence of the people, and then ensure the existence of their group
    scholars = []
    for p in authoritylist:
        scholars.append(session.run("MERGE (p:%s {identifier:'%s'}) RETURN p" %
                                    (constants.get_label('E21'), p)).single()['p'])
    return _find_or_create_authority_group(session, scholars)


def _find_or_create_authority_group(session, members):
    mc = []
    wc = []
    gc = []
    i = 1
    for a in members:
        mc.append("(a%d)" % i)
        wc.append("id(a%d) = %d" % (i, a.id))
        gc.append("(group)-[:%s]->(a%d)" % (constants.get_label('P107'), i))
        i += 1
    q = "MATCH %s WHERE %s " % (', '.join(mc), " AND ".join(wc))
    q += "(group:%s), %s RETURN group" % (constants.get_label('E74'), ", ".join(gc))
    g = session.run("MATCH " + q).single()
    if g is None:
        g = session.run("CREATE " + q).single()
    return g['group']


def _get_source_lang(factoid):
    lkeys = {2: 'gr', 3: 'la', 4: 'ar', 5: 'hy'}
    try:
        return lkeys.get(factoid.oLangKey)
    except NameError:
        return None


def appellation_handler(graphdriver, sourcenode, agent, factoid, graphperson, constants):
    """This handler deals with Second Name factoids and also Alternative Name factoids.
    The Second Names might be in all sorts of languages in the factoid itself, but refer
    to a canonical version of the name in the FamilyName table, which is probably usually
    Greek. The Alternative Name factoids should exclusively use the information in the
    base factoid."""
    orig = "factoid/%d" % factoid.factoidKey
    appassertion = "MATCH (p), (agent), (source), (pred) " \
                   "WHERE id(p) = %d AND id(agent) = %d AND id(source) = %d AND id(pred) = %d " % (
                       graphperson.id, agent.id, sourcenode.id, constants.get_predicate('P1'))
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
    appassertion += _create_assertion_query(factoid, 'p', 'pred', 'n', 'agent', 'source')
    appassertion += "RETURN a"
    with graphdriver.session() as session:
        result = session.run(appassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(appassertion.replace('COMMAND', 'CREATE'))


def death_handler(graphdriver, sourcenode, agent, factoid, graphperson, constants):
    with graphdriver.session() as session:
        # Each factoid is its own set of assertions pertaining to the single death of a person.
        # When there are multiple sources, we will have to review them for consistency and make
        # proxies for the death event as necessary.
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
        else:
            # TODO parse this later into a real date range
            deathdate = factoid.deathRecord.sourceDate
            if deathdate == '':
                deathdate = None
        deathassertion = "MATCH (p), (agent), (source), (devent), (p100), (p3), (p4) " \
                         "WHERE id(p) = %d AND id(agent) = %d AND id(source) = %d AND id(devent) = %d " \
                         "AND id(p100) = %d AND id(p3) = %d AND id(p4) = %d " \
                         % (graphperson.id, agent.id, sourcenode.id, deathevent.id, constants.get_predicate('P100'),
                            constants.get_predicate('P3'), constants.get_predicate('P4'))
        deathassertion += "MERGE (desc:%s {content:\"%s\"}) " % \
                          (constants.get_label('E62'), escape_text(factoid.replace_referents()))
        if deathdate is not None:
            deathassertion += "MERGE (datedesc:%s {content:\"%s\"}) " % (constants.get_label('E52'), deathdate)
        deathassertion += "WITH p, agent, source, devent, p100, p3, p4, desc%s " % (', datedesc' if deathdate else '')
        deathassertion += _create_assertion_query(factoid, 'devent', 'p100', 'p', 'agent', 'source')
        # Create an assertion about how the death is described
        deathassertion += _create_assertion_query(factoid, 'devent', 'p3', 'desc', 'agent', 'source', 'a1')
        # Create an assertion about when the death happened.
        if deathdate is not None:
            deathassertion += _create_assertion_query(factoid, 'devent', 'p4', 'datedesc', 'agent', 'source', 'a2')
        deathassertion += "RETURN a, a1%s" % (", a2" if deathdate else '')

        # print(deathassertion)
        result = session.run(deathassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(deathassertion.replace('COMMAND', 'CREATE'))


def ethnicity_handler(sourcenode, agent, factoid, graphperson):
    """Assign a group membership for the given ethnicity to the person"""
    if factoid.ethnicityInfo is None or factoid.ethnicityInfo.ethnicity is None:
        # We can't assign any ethnicity without the ethnicity info
        warn("Empty ethnicity factoid found: id %s" % factoid.factoidKey)
        return
    elabel = factoid.ethnicityInfo.ethnicity.ethName
    groupnode = constants.cv['Ethnicity'].get(elabel)
    gassertion = "MATCH (p), (agent), (source), (group), (mpred)  " \
                 "WHERE id(p) = %d AND id(agent) = %d AND id(source) = %d AND id(group) = %d AND id(mpred) = %d " % \
                 (graphperson.id, agent.id, sourcenode.id, groupnode.id, constants.get_predicate('P107'))
    gassertion += _create_assertion_query(None, 'group', 'mpred', 'p', 'agent', 'source')
    gassertion += "RETURN a"
    with constants.graphdriver.session() as session:
        result = session.run(gassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(gassertion.replace('COMMAND', 'CREATE'))


def religion_handler(sourcenode, agent, factoid, graphperson):
    """Assign a group membership for the given religious confession to the person"""
    orig = "factoid/" + factoid.factoidKey
    if factoid.religion is None:
        warn("Empty religion factoid found: id %d" % factoid.factoidKey)
        return
    rlabel = factoid.religion
    # Special case, database had an error
    if factoid.religion == '':
        rlabel = 'Heretic'
    rnode = constants.cv['Religion'].get(rlabel)
    # (r:C23 Religious identity) [rwho:P36 pertains to] person
    # (r:C23 Religious identity) [rwhich:P35 is defined by] rnode
    rassertion = "MATCH (p), (agent), (source), (rel), (rwhich), (rwho) " \
                 "WHERE id(p) = %d AND id(agent) = %d AND id(source) = %d AND id(rel) = %d " \
                 "AND id(rwhich) = %d AND id(rwho) = %d" \
                 % (graphperson.id, agent.id, sourcenode.id, rnode.id,
                    constants.get_predicate('SP35'), constants.get_predicate('SP36'))
    rassertion += _create_assertion_query(orig, 'r:%s' % constants.get_label('C23'), 'rwho', 'p', 'agent', 'source')
    rassertion += _create_assertion_query(orig, 'r', 'rwhich', 'rel', 'agent', 'source', 'a1')
    rassertion += "RETURN a"
    with constants.graphdriver.session() as session:
        result = session.run(rassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(rassertion.replace('COMMAND', 'CREATE'))


def societyrole_handler(sourcenode, agent, factoid, graphperson):
    orig = "factoid/" + factoid.factoidKey
    if factoid.occupation is None:
        return
    rnode = constants.cv['SocietyRole'].get(factoid.religion)
    # (r:C1 Social Quality of an Actor) [rwho:P13 pertains to] person
    # (r:C23 Religious identity) [rwhich:P14 is defined by] rnode
    rassertion = "MATCH (p), (agent), (source), (role), (rwhich), (rwho) " \
                 "WHERE id(p) = %d AND id(agent) = %d AND id(source) = %d AND id(role) = %d " \
                 "AND id(rwhich) = %d AND id(rwho) = %d" \
                 % (graphperson.id, agent.id, sourcenode.id, rnode.id,
                    constants.get_predicate('SP13'), constants.get_predicate('SP14'))
    rassertion += _create_assertion_query(orig, 'r:%s' % constants.get_label('C1'), 'rwho', 'p', 'agent', 'source')
    rassertion += _create_assertion_query(orig, 'r', 'rwhich', 'rel', 'agent', 'source', 'a1')
    rassertion += "RETURN a"
    with constants.graphdriver.session() as session:
        result = session.run(rassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(rassertion.replace('COMMAND', 'CREATE'))


def language_handler(sourcenode, agent, factoid, graphperson):
    """Assign a language skill to the person"""
    orig = 'factoid/' + factoid.factoidKey
    if factoid.languageSkill is None:
        return
    lnode = constants.cv['Language'].get(factoid.languageSkill)
    # person [rwho:P38 has skill] (r:C21 Skill)
    # (r:C21 Skill) [rwhich:P37 concerns] (l:C29 Know-How)
    lassertion = "MATCH (p), (agent), (source), (lkh), (lwhich), (lwho) " \
                 "WHERE id(p) = %d AND id(agent) = %d AND id(source) = %d AND id(lkh) = %d " \
                 "AND id(lwhich) = %d AND id(lwho) = %d" \
                 % (graphperson.id, agent.id, sourcenode.id, lnode.id,
                    constants.get_predicate('SP37'), constants.get_predicate('SP38'))
    lassertion += _create_assertion_query(orig, 'p', 'lwho', 'ls:%s' % constants.get_label('C21'), 'agent', 'source')
    lassertion += _create_assertion_query(orig, 'ls', 'rwhich', 'lkh', 'agent', 'source', 'a1')
    lassertion += "RETURN a"
    with constants.graphdriver.session() as session:
        result = session.run(lassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(lassertion.replace('COMMAND', 'CREATE'))


def description_handler(graphdriver, sourcenode, agent, factoid, graphperson, constants):
    # Get the descriptions and the relevant languages
    langdesc = {'en': escape_text(factoid.replace_referents())}
    langkey = _get_source_lang(factoid)
    if langkey is not None:
        langdesc[langkey] = escape_text(factoid.origLDesc)
    descattributes = []
    for k, v in langdesc.items():
        descattributes.append('%s: \"%s\"' % (k, v))
    # Make the query
    with graphdriver.session() as session:
        descassertion = "MATCH (p), (agent), (source), (pred) " \
                        "WHERE id(p) = %d AND id(agent) = %d AND id (source) = %d AND id(pred) = %d " % \
                        (graphperson.id, agent.id, sourcenode.id, constants['P3'])
        descassertion += 'MERGE (desc:crm__E62_String {%s}) ' % ','.join(descattributes)  # TODO get rid of E62
        descassertion += "WITH p, pred, desc, agent, source "
        descassertion += _create_assertion_query(factoid, 'p', 'pred', 'desc', 'agent', 'source')
        descassertion += 'RETURN a'
        result = session.run(descassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(descassertion.replace('COMMAND', 'CREATE'))


def _find_or_create_kinship(session, graphperson, graphkin):
    # See if there is an existing kinship group between the person and their kin according to the
    # source in question. If not, return a new (not yet connected) kinship group node.
    e74 = constants.get_label('E74')
    p140 = constants.get_label('P140')
    p141 = constants.get_label('P141')
    kinquery = "MATCH (p), (kin) WHERE id(p) = %d AND id(kin) = %d " % (graphperson.id, graphkin.id)
    kinquery += "MATCH (p)<-[:%s]-(a1:crm__E13_Assertion)-[:%s]->(kg:%s)<-[:%s]-(a2)-[:%s]->(kin) " \
                "RETURN DISTINCT kg" % (p141, p140, e74, p140, p141)
    result = session.run(kinquery).single()
    if result is None:
        # If the kinship pair hasn't been referenced yet, then create a new empty kinship and return it
        # for use in the assertions below.
        result = session.run("CREATE (kg:%s) RETURN kg" % e74).single()
    return result['kg']


def kinship_handler(sourcenode, agent, factoid, graphperson):
    # Kinships are modeled as two-person groups with their own separate label (because, honestly)
    # and with .1 types as property attributes as per the CRM spec.
    if factoid.kinshipType is None:
        warn("Empty kinship factoid found: id %d" % factoid.factoidKey)
        return
    predspec = constants.cv.get('Kinship')[factoid.kinshipType.gspecRelat]
    predgen = constants.get_predicate('P107')
    with constants.graphdriver.session() as session:
        for kin in factoid.referents():
            graphkin = _find_or_create_graphperson(kin.name, kin.mdbCode)
            if graphkin.id == graphperson.id:
                warn("Person %s listed as related to self" % kin)
                continue
            kgroup = _find_or_create_kinship(session, graphperson, graphkin)
            kinassertion = "MATCH (p), (kin), (kg), (agent), (source), (pspec), (pgen) WHERE id(p) = %d " \
                           "AND id(kin) = %d AND id(kg) = %d AND id(agent) = %d AND id(source) = %d " \
                           "AND id(pspec) = %d AND id(pgen) = %d " \
                           % (graphperson.id, graphkin.id, kgroup.id, agent.id, sourcenode.id, predspec, predgen)
            kinassertion += _create_assertion_query(factoid, 'kg', 'pspec', 'p', 'agent', 'source', 'a1')
            kinassertion += _create_assertion_query(factoid, 'kg', 'pgen', 'kin', 'agent', 'source', 'a2')
            kinassertion += "RETURN a1, a2"
            result = session.run(kinassertion.replace('COMMAND', 'MATCH')).single()
            if result is None:
                session.run(kinassertion.replace('COMMAND', 'CREATE'))


def possession_handler(sourcenode, agent, factoid, graphperson):
    """Ensure the existence of an E18 Physical Thing (we don't have any more category info about
    the possessions). For now, we assume that a possession with an identical description is, in fact,
    the same possession."""
    possession_attributes = {constants.get_label('P1'): escape_text(factoid.engDesc)}
    if factoid.possession is not None and factoid.possession != '':
        possession_attributes[constants.get_label('P3')] = escape_text(factoid.possession)
    possession_attrs = ", ".join(["%s: '%s'" % (k, v) for k, v in possession_attributes.items()])
    posassertion = "MATCH (p), (agent), (source), (pred) " \
                   "WHERE id(p) = %d AND id(agent) = %d AND id (source) = %d AND id(pred) = %d " % \
                   (graphperson.id, agent.id, sourcenode.id, constants.get_predicate('P51'))
    posassertion += "MERGE (poss:%s {%s}) " % (constants.get_label('E18'), possession_attrs)
    posassertion += "WITH p, agent, source, pred, poss "
    posassertion += _create_assertion_query(factoid, 'poss', 'pred', 'p', 'agent', 'source')
    posassertion += "RETURN a"
    with constants.graphdriver.session() as session:
        result = session.run(posassertion.replace('COMMAND', 'MATCH')).single()
        if result is None:
            session.run(posassertion.replace('COMMAND', 'CREATE'))


def _find_or_create_graphperson(name, code):
    """Return an E21 Person, labeled with the name and code via an E14 Identifier Assignment carried out by PBW."""
    # We can't merge with comma statements, so we have to do it with successive one-liners.
    # Start the merge from the specific information we have, which is the identifier itself.
    nodelookup = "MATCH (pbw) WHERE id(pbw) = %d " \
                 "MERGE (idlabel:%s {value:'%s %d'}) " \
                 "MERGE (pbw)<-[:%s]-(idass:%s)" \
                 "-[:%s]->(idlabel) " \
                 "MERGE (idass)-[:%s]->(p:%s) RETURN p" % \
                 (constants.generic_agent.id, constants.get_label('E42'), name, code, constants.get_label('P14'),
                  constants.get_label('E15'), constants.get_label('P37'),
                  constants.get_label('P140'), constants.get_label('E21'))
    with constants.graphdriver.session() as session:
        graph_person = session.run(nodelookup).single()['p']
    return graph_person


def record_assertion_factoids():
    """To be run after everything else is done. Creates the assertion record for all assertions created here,
    tying each to the factoid or person record that originated it."""
    assertionlookup = "MATCH (a:crm__E13_Attribute_Assignment) " \
                      "WHERE NOT (a)<-[:crm__P70_documents]-(:crm__E31_Document) " \
                      "CREATE (a)<-[:crm__P70_documents]-(d:crm__E31_Document:lrmer__F2_Expression) " \
                      "SET d.lrmer__R76_is_derivative_of = a.origsource " \
                      "REMOVE a.origsource " \
                      "RETURN COUNT(d) AS n"
    with constants.graphdriver.session() as session:
        new_assertions = session.run(assertionlookup).single()['n']
        print("*** Created %d new assertions ***" % new_assertions)


def process_persons(sqlsession, graphdriver):
    """Go through the relevant person records and process them for factoids"""
    processed = 0
    used_sources = set()
    for person in collect_person_records(sqlsession):
        # Skip the anonymous groups for now
        if person.name == 'Anonymi':
            continue
        processed += 1
        # Create or find the person node
        print("*** Making/finding node for person %s %d ***" % (person.name, person.mdbCode))
        graph_person = _find_or_create_graphperson(person.name, person.mdbCode)

        # Get the 'factoids' that are directly in the person record
        for ftype in constants.directPersonRecords:
            ourftype = _smooth_labels(ftype)
            try:
                method = eval("%s_handler" % ourftype.lower())
                method(constants.generic_agent, person, graph_person)
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
                    with graphdriver.session() as session:
                        (source_node, authority_node) = get_source_and_agent(session, f)
                    # If the factoid has no source then we skip it
                    if source_node is None:
                        warn("Skipping factoid %d without a traceable source" % f.factoidKey)
                        continue
                    # If the factoid has no authority then we assign it to the generic PBW agent
                    if authority_node is None:
                        authority_node = constants.generic_agent
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
    starttime = datetime.datetime.now()
    engine = create_engine('mysql+pymysql://' + config.dbstring)
    smaker = sessionmaker(bind=engine)
    mysqlsession = smaker()
    # Connect to the graph DB
    driver = GraphDatabase.driver(config.graphuri, auth=(config.graphuser, config.graphpw))
    # Make / retrieve the global nodes and constants
    constants = PBWstarConstants.PBWstarConstants(mysqlsession, driver)
    # Process the person records
    process_persons(mysqlsession, driver)
    duration = datetime.datetime.now() - starttime
    print("Done! Ran in %s" % str(duration))
