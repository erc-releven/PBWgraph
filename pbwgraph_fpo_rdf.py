import pbw
import config
import re
from rdflib import Graph, Literal, RDF, Namespace
from rdflib.namespace import RDFS, XSD, OWL
from datetime import datetime, date
from sqlalchemy import create_engine, distinct
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote


"""Defines preliminary namespace for PBW RDF representation."""
pbwfpo = Namespace('http://pbw2016.kdl.kcl.ac.uk/rdf/')
"""Defines preliminary namespace for PBW RDF representation."""
pbw_fpo_extension = Namespace('http://pbw2016.kdl.kcl.ac.uk/ontology/')
"""Defines Factoid Prosopography Ontology (KCL/Bradley) namespace."""
fpocore = Namespace('http://github.com/johnBradley501/FPO/raw/master/fpo.owl#')
"""Defines namespace for CIDOC CRM."""
crm = Namespace('http://www.cidoc-crm.org/cidoc-crm/')
"""Defines namespace for APIS database."""
apis = Namespace('https://www.apis.acdh.oeaw.ac.at/')
"""Defines LRMoo namespace."""
lrmoo = Namespace('http://iflastandards.info/ns/lrm/lrmoo/')
""""Namespace for places from Pleiades"""
pleiades_place = Namespace('http://pleiades.stoa.org/places/')
""""Namespace for places from GeoNames"""
geonames_place = Namespace('https://sws.geonames.org/')


def langtag_string(s, lcode):
    ltag = None
    match lcode:
        case 2:
            ltag = 'grc'
        case 3:
            ltag = 'la'
        case 4:
            ltag = 'ar'
        case 5:
            ltag = 'xcl'
    if ltag is not None:
        return Literal(s, datatype=XSD.string, lang=ltag)
    else:
        return Literal(s, datatype=XSD.string, lang='en')


# On both of the following cf.
# https://www.digitalbyzantinist.org/2014/06/17/the_mystery_of_the_character_encoding/
def decode(garbage):
    """For dealing with the strings that are double-encoded in the database."""
    ints = []
    for i in [ord(c) for c in garbage]:
        if i > 256:
            ints.append(ord(chr(i).encode('cp1252')))
        else:
            ints.append(i)
    try:
        return bytearray(ints).decode('utf-8')
    except UnicodeDecodeError:
        return None


def recode(rubbish):
    """For dealing with strings that have been decoded to UTF-8 almost correctly"""
    newchrs = bytearray()
    for cc in rubbish:
        if ord(cc) > 256:
            newchrs.append(ord(cc.encode('cp1252')))
        else:
            newchrs.append(ord(cc))
    return newchrs.decode('utf-8')


def create_persons(g, session):
    for p in session.query(pbw.Person).all():
        # Make the person's URI
        person = pbwfpo.term(f'person/{p.personKey}')
        # Which class is the person?
        if p.name == 'Anonymi' or p.name == 'Anonymae':
            isa = pbw_fpo_extension.Group
        else:
            match p.sex:
                case 'Female':
                    isa = fpocore.FemalePerson
                case 'Eunuch':
                    isa = pbw_fpo_extension.EunuchPerson
                case 'Male':
                    isa = fpocore.MalePerson
                case _:
                    isa = fpocore.Person
        # Make some triples
        g.add((person, RDF.type, isa))
        # name
        g.add((person, fpocore.hasDisplayNameComponent, Literal(p.name, datatype=XSD.string)))
        # mdbCode
        g.add((person, fpocore.hasDisplayNumberComponent, Literal(p.mdbCode, datatype=XSD.integer)))
        # descName
        if p.descName:
            g.add((person, fpocore.hasDisplayName, langtag_string(p.descName, 0)))
        # nameOL
        if p.nameOL:
            g.add((person, pbw_fpo_extension.hasDisplayName, langtag_string(p.nameOL, p.oLangKey)))
        # floruit
        if p.floruit:
            g.add((person, pbw_fpo_extension.floruit, Literal(p.floruit, datatype=XSD.string)))


def create_sources(g, session):
    # Here we have to query the source table and the bibliography table
    for b in session.query(pbw.Source).all():
        source = pbwfpo.term(f'source/s{b.sourceKey}')
        g.add((source, RDF.type, fpocore.Source))
        g.add((source, fpocore.hasSourceName, Literal(b.sourceID)))
    for b in session.query(pbw.Bibliography).all():
        source = pbwfpo.term(f'source/b{b.bibKey}')
        g.add((source, RDF.type, fpocore.Source))
        g.add((source, fpocore.hasSourceName, Literal(b.shortName)))
        if b.latinBib:
            lbstr = b.latinBib if b.bibKey == 816 else recode(b.latinBib)
            g.add((source, fpocore.hasBibliographicEntry, langtag_string(lbstr, 0)))
        if b.greekBib:
            g.add((source, fpocore.hasBibliographicEntry, langtag_string(b.greekBib, 2)))


def create_sealcollections(g, session):
    for c in session.query(pbw.Collection).all():
        colluri = pbwfpo.term(f'sealcollection/{c.collectionKey}')
        g.add((colluri, RDF.type, pbw_fpo_extension.SealCollection))
        g.add((colluri, pbw_fpo_extension.hasShortName, Literal(c.shortName)))
        g.add((colluri, pbw_fpo_extension.hasFullName, Literal(c.collectionName)))


def create_locations(g, session):
    for i in session.query(pbw.Location):
        locuri = pbwfpo.term(f'location/{i.locationKey}')
        g.add((locuri, RDF.type, fpocore.Location))
        g.add((locuri, fpocore.hasLocationName, langtag_string(i.locName, 0)))
        # The original location names are unrecoverable in the database
        # if i.locNameOL:
        #     g.add((locuri, fpocore.hasLocationName, langtag_string(i.locNameOL, i.oLangKey)))
        if i.geonames_id:
            g.add((locuri, OWL.sameAs, geonames_place.term(i.geonames_id)))
        if i.pleiades_id:
            g.add((locuri, OWL.sameAs, pleiades_place.term(i.pleiades_id)))
        if i.extraInfo:
            g.add((locuri, RDFS.comment, Literal(i.extraInfo)))


def create_sourceref(g, srefobj, referent, bibentry=False):
    if bibentry:
        refstr = srefobj.publicationPage if srefobj.publicationPage else srefobj.publicationRef
    else:
        refstr = referent.sourceRef
    urlenc_cit = quote(refstr)
    srckey = srefobj.bibKey if bibentry else referent.sourceKey
    sourcecit_uri = pbwfpo.term(f'citation/{srckey}/{urlenc_cit}')
    g.add((referent, fpocore.publishedAt, sourcecit_uri))
    g.add((sourcecit_uri, RDF.type, fpocore.SourceCitation))
    g.add((sourcecit_uri, fpocore.fromSource, pbwfpo.term(f'source/{srckey}')))
    if refstr and refstr != '0':
        g.add((sourcecit_uri, fpocore.hasPlaceInSource, Literal(refstr)))


def create_boulloterions(g, session):
    for b in session.query(pbw.Boulloterion).all():
        bouluri = pbwfpo.term(f'boulloterion/{b.boulloterionKey}')
        g.add((bouluri, RDF.type, pbw_fpo_extension.Boulloterion))
        g.add((bouluri, pbw_fpo_extension.hasTextInscription, langtag_string(b.text, 0)))
        g.add((bouluri, pbw_fpo_extension.hasTextInscription, langtag_string(b.origLText, b.oLangKey)))
        g.add((bouluri, pbw_fpo_extension.obverseIcon, langtag_string(b.obvIcon, 0)))
        if b.revIcon:
            g.add((bouluri, pbw_fpo_extension.reverseIcon, langtag_string(b.revIcon, 0)))
        if b.diameter:
            g.add((bouluri, pbw_fpo_extension.hasDiameter, Literal(b.diameter, datatype=XSD.integer)))

        # Give the boulloterion its date. This comes in two forms
        dateuri = pbwfpo.term(f'daterange/b{b.boulloterionKey}')
        g.add((dateuri, RDF.type, fpocore.DateRangeTEI))
        dated = False
        if b.scDate:
            dated = True
            # This date range has a particular year associated with it. We set the
            # notBefore and notAfter fields to this date.
            dateobj = pbwfpo.term(f'date/{b.scDate.year}')
            g.add((dateobj, RDF.type, fpocore.Date))
            g.add((dateobj, fpocore.hasYear, Literal(b.scDate.year)))
            g.add((dateuri, fpocore.isNotBefore, dateobj))
            g.add((dateuri, fpocore.isNotAfter, dateobj))
        if b.dateWords:
            dated = True
            g.add((dateuri, fpocore.hasTextualDescription, langtag_string(b.dateWords, 0)))
        if dated:
            g.add((bouluri, fpocore.occurredOn, dateuri))
        else:
            # We don't actually have a date, so remove the triples we just created.
            g.remove((dateuri, None, None))

        # Link the boulloterion to its sources
        for p in b.publication:
            create_sourceref(g, p, b, bibentry=True)

        # Link the boulloterion to its seals
        for s in b.seals:
            sealuri = pbwfpo.term(f'seal/{s.sealKey}')
            colluri = pbwfpo.term(f'sealcollection/{s.collectionKey}')
            g.add((sealuri, RDF.type, pbw_fpo_extension.LeadSeal))
            g.add((sealuri, pbw_fpo_extension.inCollection, colluri))
            g.add((sealuri, pbw_fpo_extension.referencedAs, Literal(s.collectionRef)))
            g.add((bouluri, pbw_fpo_extension.hasProduced, sealuri))


def create_authority_list(g, session, tablename, category, idkey, labelkey):
    """Create the entities that represent our lists of terms."""
    # Get the database table by name
    table = getattr(pbw, tablename)
    umbrella = pbwfpo.term(tablename)
    for item in session.query(table).all():
        idval = getattr(item, idkey)
        if not idval:
            continue
        termuri = pbwfpo.term(f"{category}/{idval}")
        g.add((termuri, RDF.type, umbrella))
        g.add((termuri, RDFS.label, langtag_string(getattr(item, labelkey), 0)))
        # Add our original-language terms
        if tablename in ['DignityOffice', 'Ethnicity', 'FamilyName']:
            label_ol = getattr(item, f"{labelkey}OL")
            if label_ol:
                g.add((termuri, RDFS.label, langtag_string(label_ol, item.oLangKey)))
        # We need to add extra information for dignities
        if tablename == 'DignityOffice':
            if item.parent:
                parenturi = pbwfpo.term(f'dignity/{item.parent.doKey}')
                g.add((termuri, pbw_fpo_extension.hasParentCategory, parenturi))
        # We also need to add extra information for kinships
        if tablename == 'KinshipType':
            if item.gunspecRelat:
                g.add((termuri, pbw_fpo_extension.nonSpecificLabel, Literal(item.gunspecRelat)))


def make_date_range(g, yid, begindate, enddate):
    yearuri = pbwfpo.term(f'year/{yid}')
    g.add((yearuri, RDF.type, fpocore.DateRageTEI))
    # Create the beginning and end dates
    from_date = pbwfpo.term(f'date/{begindate.isoformat()}')
    to_date = pbwfpo.term(f'date/{enddate.isoformat()}')
    g.add((yearuri, RDF.type, fpocore.DateRangeTEI))
    g.add((from_date, RDF.type, fpocore.Date))
    g.add((from_date, fpocore.hasYear, Literal(begindate.year, XSD.integer)))
    g.add((from_date, fpocore.hasMonth, Literal(begindate.month, XSD.integer)))
    g.add((from_date, fpocore.hasDay, Literal(begindate.day, XSD.integer)))
    g.add((to_date, RDF.type, fpocore.Date))
    g.add((to_date, fpocore.hasYear, Literal(enddate.year, XSD.integer)))
    g.add((to_date, fpocore.hasMonth, Literal(enddate.month, XSD.integer)))
    g.add((to_date, fpocore.hasDay, Literal(enddate.day, XSD.integer)))
    g.add((yearuri, fpocore.isNotBefore, from_date))
    g.add((yearuri, fpocore.isNotAfter, to_date))
    return yearuri


def create_narrative_units(g, session):
    for nu in session.query(pbw.NarrativeUnit).all():
        # Skip the root category units
        if nu.narrativeUnitID < 100:
            continue

        # Deal with the year / decade / century units
        m = re.match(r'^(\d+)(-(\d+))?$', nu.description)
        if m:
            # It is a year unit. Make a DateRange out of it and then carry on
            startdate = date.fromisoformat(f"{m.group(1)}-01-01")
            enddate = date.fromisoformat(f"{m.group(3) if m.group(3) else m.group(1)}-12-31")
            make_date_range(g, nu.narrativeUnitID, startdate, enddate)
            continue

        # Deal with the reign units. These are also DateRanges
        if nu.reign == 4:
            # It is a reign. This is also basically a DateRange

        nunit_uri = pbwfpo.term(f'narrativeunit/{nu.narrativeUnitID}')
        g.add((nunit_uri, RDF.type, pbw_fpo_extension.NarrativeUnit))
        # Add the description and notes
        g.add((nunit_uri, pbw_fpo_extension.hasUnitDescription, langtag_string(nu.description, 0)))
        g.add((nunit_uri, pbw_fpo_extension.hasUnitSummary, langtag_string(nu.summary, 0)))
        if nu.notes:
            g.add((nunit_uri, pbw_fpo_extension.hasNote, langtag_string(nu.notes, 0)))
        if nu.primaryAttestation:
            # This is just a string
            g.add((nunit_uri, pbw_fpo_extension.hasPrimaryAttestation, Literal(nu.primaryAttestation)))
        # Associate the unit with its parent, reign, and year. The reign and year can be DateRanges.
        if nu.year:
            yearuri = pbwfpo.term(f'narrativeyear/{nu.year}')
            g.add((nunit_uri, pbw_fpo_extension.belongsToYear, yearuri))


def create_factoids(g, session):
    # print(f"""pbw factoid: {fa.factoidKey}""")
    for a in session.query(pbw.Factoid).all():
        factoid_base_uri = pbwfpo.term(f'factoid/{a.factoidKey}')
        # What type of factoid is it?
        match a.factoidType.typeName:
            case 'Dignity/Office':
                ftype_str = 'Office'
            case 'Ethnic label':
                ftype_str = 'Ethnicity'
            case 'Eunuchs':
                ftype_str = 'Eunuch'
            case 'Kinship':
                ftype_str = 'Relationship'
            case 'Occupation/Vocation':
                ftype_str = 'Occupation'
            case _:
                ftype_str = a.factoidType.typeName
        ftype_str = ftype_str.replace(' ', '')
        g.add((factoid_base_uri, RDF.type, fpocore.term(f"{ftype_str}Factoid")))

        # Get the source citation
        if a.source.sourceID == 'Seals':
            pass  # TODO link to boulloterion
        else:
            create_sourceref(g, a.source, a)

        # Get the text of the factoid. Note that if this is a possession factoid, the text of the factoid
        # is also the name of the possession.
        if a.engDesc:
            g.add((factoid_base_uri, pbw_fpo_extension.hasDescriptiveText, langtag_string(a.engDesc, 0)))
        if a.origLDesc:
            g.add((factoid_base_uri, pbw_fpo_extension.hasDescriptiveText, langtag_string(a.origLDesc, a.oLangKey)))

        # Deal with the factoids that have extra information
        match ftype_str:
            case 'Death':
                # We need to make a daterange object for the factoid.
                if a.deathRecord and a.deathRecord.sourceDate:
                    dateuri = pbwfpo.term(f'daterange/d{a.deathRecord.id}')
                    g.add((dateuri, fpocore.hasTextualDescription, langtag_string(a.deathRecord.sourceDate, 0)))
                    g.add((factoid_base_uri, fpocore.occurredOn, dateuri))
            case 'Office':
                # Add the specific dignity/office
                if a.dignity:
                    douri = pbwfpo.term(f'dignity/{a.dignity.doKey}')
                    g.add((factoid_base_uri, pbw_fpo_extension.officeIs, douri))
            case 'Ethnicity':
                if a.ethnicityInfo:
                    ethuri = pbwfpo.term(f'ethnicity/{a.ethnicityInfo.ethnicityKey}')
                    g.add((factoid_base_uri, pbw_fpo_extension.ethnicityIs, ethuri))
            case 'SecondName':
                if a.secondName:
                    nameuri = pbwfpo.term(f'familyname/{a.secondName.famNameKey}')
                    g.add((factoid_base_uri, pbw_fpo_extension.secondNameIs, nameuri))
            case 'Kinship':
                if a.kinshipType:
                    kinuri = pbwfpo.term(f'kinship/{a.kinshipType.kinKey}')
                    g.add((factoid_base_uri, pbw_fpo_extension.kinshipIs, kinuri))
            case 'LanguageSkill':
                if a.languageSkill:
                    g.add((factoid_base_uri, pbw_fpo_extension.languageIs, pbwfpo.term(f'language/{a.languageSkill}')))
            case 'Location':
                # Decode and add information
                if a.locationInfo:
                    srcname = decode(a.locationInfo.srcName)
                    siginfo = decode(a.locationInfo.sigInfo)
                    notes = a.locationInfo.notes
                    if srcname:
                        g.add((factoid_base_uri, pbw_fpo_extension.referredToAs,
                               langtag_string(srcname, a.locationInfo.oLangKey)))
                    if siginfo:
                        g.add((factoid_base_uri, pbw_fpo_extension.significantInfo,
                               langtag_string(siginfo, a.locationInfo.oLangKey)))
                    if notes:
                        g.add((factoid_base_uri, RDFS.comment, notes))
            case 'Occupation':
                if a.occupation:
                    occuri = pbwfpo.term(f'occupation/{a._occInfo.occupationKey}')
                    g.add((factoid_base_uri, pbw_fpo_extension.occupationIs, occuri))
            case 'Possession':
                # Our possessions are just strings really; this makes it rather difficult to differentiate
                # between the same possession and possessions of the same kind. Furthermore, the possessions
                # are named in the engDesc of the factoid, not the possessionName of the extra table!
                if a.possession:
                    g.add((factoid_base_uri, RDFS.comment, Literal(a.possession.possessionName)))
                    possuri = pbwfpo.term(f'possession/{a.possession.id}')
                    g.add((possuri, RDF.type, fpocore.Possession))
                    g.add((possuri, fpocore.hasPossessionName, langtag_string(a.engDesc, 0)))
                    if a.origLDesc:
                        g.add((possuri, fpocore.hasPossessionName, langtag_string(a.origLDesc, a.oLangKey)))
                    g.add((factoid_base_uri, fpocore.assertsPossessionOf, possuri))
            case 'Religion':
                if a.religion:
                    reluri = pbwfpo.term(f'occupation/{a._relInfo.religionKey}')
                    g.add((factoid_base_uri, pbw_fpo_extension.religionIs, reluri))
            case 'AlternativeName':
                if a.vnameInfo:
                    nameuri = pbwfpo.term(f'name/{a.vnameInfo.vnameKey}')
                    g.add((factoid_base_uri, pbw_fpo_extension.alternativeNameIs, nameuri))


def link_factoids_to_persons(g, session):
    # Make the role types
    for role in session.query(distinct(pbw.FactoidPerson.fpTypeKey)).all():
        # We only create the ones that are actually in use
        roleuri = pbwfpo.term(f'role/{role.fpTypeKey}')
        g.add((roleuri, RDF.type, fpocore.Role))
        g.add((roleuri, fpocore.hasRoleName, Literal(role.fpTypeName)))

    for r in session.query(pbw.FactoidPerson).all():
        # Make the reference and give it its role property
        refuri = pbwfpo.term(f'personref/{r.fpKey}')
        g.add((refuri, RDF.type, fpocore.PersonReference))
        g.add((refuri, fpocore.hasRole, pbwfpo.term(f'role/{r.fpTypeKey}')))
        # Link the reference to the person
        g.add((refuri, fpocore.referencesPerson, pbwfpo.term(f'person/{r.personKey}')))
        # Link the factoid to the reference
        g.add((pbwfpo.term(f'factoid/{r.factoidKey}'), fpocore.hasReference, refuri))


def create_full_graph(session):
    g = Graph(bind_namespaces="rdflib")
    g.bind('crm', crm)
    g.bind('fpo', fpocore)
    g.bind('pbw', pbw)
    g.bind('lrmoo', lrmoo)
    g.bind('apis', apis)
    create_persons(g, session)
    create_sources(g, session)
    create_locations(g, session)
    create_authority_list(g, session, 'DignityOffice', 'dignity', 'doKey', 'stdName')
    create_authority_list(g, session, 'Ethnicity', 'ethnicity', 'ethnicityKey', 'ethName')
    create_authority_list(g, session, 'FamilyName', 'familyname', 'famNameKey', 'famName')
    create_authority_list(g, session, 'KinshipType', 'kinship', 'kinKey', 'gspecRelat')
    create_authority_list(g, session, 'LanguageSkill', 'language', 'languageName', 'languageName')
    create_authority_list(g, session, 'Occupation', 'occupation', 'occupationKey', 'occupationName')
    create_authority_list(g, session, 'Religion', 'religion', 'religionKey', 'religionName')
    create_authority_list(g, session, 'VariantName', 'name', 'vnameKey', 'name')
    # create_boulloterions(g, session)
    # create_seals(g, session)
    create_factoids(g, session)

    g.serialize(destination=f'pbw_rdf_data_{date.today().strftime("%d-%m-%Y")}.ttl', format='turtle')


if __name__ == '__main__':
    # Connect to the SQL DB
    starttime = datetime.now()
    engine = create_engine('mysql+pymysql://' + config.dbstring)
    smaker = sessionmaker(bind=engine)
    mysqlsession = smaker()
    create_full_graph(mysqlsession)
