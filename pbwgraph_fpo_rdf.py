import pbw
import config
from rdflib import Graph, Literal, RDF, Namespace
from rdflib.namespace import RDFS, XSD, OWL
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib.parse import urlencode


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
    for b in session.query(pbw.Source).all():
        source = pbwfpo.term(f'source/{b.sourceKey}')
        g.add((source, RDF.type, fpocore.Source))
        g.add((source, fpocore.hasSourceName, Literal(b.sourceID, datatype=XSD.string)))


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
        if b.dateWords:
            pass  # make date and textual description?


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
            urlenc_cit = urlencode(a.sourceRef)
            sourcecit_uri = pbwfpo.term(f'citation/{urlenc_cit}')
            g.add((factoid_base_uri, fpocore.sourcedFrom, sourcecit_uri))
            g.add((sourcecit_uri, RDF.type, fpocore.SourceCitation))
            g.add((sourcecit_uri, fpocore.fromSource, pbwfpo.term(f'source/{a.sourceKey}')))
            g.add((sourcecit_uri, fpocore.hasPlaceInSource, Literal(a.sourceRef)))

        # Get the text
        if a.engDesc:
            g.add((factoid_base_uri, pbw_fpo_extension.hasDescriptiveText, langtag_string(a.engDesc, 0)))
        if a.origLDesc:
            g.add((factoid_base_uri, pbw_fpo_extension.hasDescriptiveText, langtag_string(a.origLDesc, a.oLangKey)))


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
    # create_boulloterions(g, session)
    # create_seals(g, session)
    create_factoids(g, session)

    g.serialize(destination=f'pbw_rdf_data_{datetime.now().strftime("%d-%m-%Y")}.ttl', format='turtle')


if __name__ == '__main__':
    # Connect to the SQL DB
    starttime = datetime.now()
    engine = create_engine('mysql+pymysql://' + config.dbstring)
    smaker = sessionmaker(bind=engine)
    mysqlsession = smaker()
    create_full_graph(mysqlsession)
