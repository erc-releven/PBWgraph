import pbw
import mariadb
import sys
from rdflib import Graph, Literal, RDF, Namespace, URIRef
from rdflib.namespace import RDFS, FOAF, XSD
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib.parse import urlencode
import re
from pbwgraph_config import create_engine_str
from graphimportSTAR import get_authmap

mj = 'Michael Jeffreys'
tp = 'Tassos Papacostas'
ta = 'Tara Andrews'
jr = 'Judith Ryder'
mw = 'Mary Whitby'
wa = 'Wahid Amin'
bs = 'Bruna Soravia'
hm = 'Harry Munt'
lo = 'Letizia Osti'
cr = 'Charlotte Roueché'
ok = 'Olga Karagiorgiou'


pbwfpo=Namespace('http://example.com/PBW/FPO/test_v1/entity/')
"""Defines preliminary namespace for PBW RDF representation."""
pbw_fpo_extension=Namespace('http://example.com/PBW//')
"""Defines preliminary namespace for PBW RDF representation."""
fpocore=Namespace('http://github.com/johnBradley501/FPO/raw/master/fpo.owl#')
"""Defines Factoid Prosopography Ontology (KCL/Bradley) namespace."""
dc=Namespace('http://purl.org/dc/elements/1.1/')
"""Defines Dublincore namespace."""
crm=Namespace('http://www.cidoc-crm.org/cidoc-crm/')
"""Defines namespace for CIDOC CRM."""
fpocore=Namespace('http://www.intavia.eu/idm-core/')
"""Defines namespace for own ontology."""
owl=Namespace('http://www.w3.org/2002/7/owl#')
"""Defines namespace for Europeana data model vocabulary."""
rdf=Namespace('http://www.w3.org/1999/02/22-rdf-syntax-ns#')
"""Defines namespace for Europeana data model vocabulary."""
xml=Namespace('http://www.w3.org/XML/1998/namespace')
"""Defines namespace for Europeana data model vocabulary."""
xsd=Namespace('http://www.w3.org/2001/XMLSchema#')
"""Defines namespace for Europeana data model vocabulary."""
rdfs=Namespace('http://www.w3.org/2000/01/rdf-schema#')
"""Defines namespace for Europeana data model vocabulary."""
apis=Namespace('https://www.apis.acdh.oeaw.ac.at/')
"""Defines namespace for APIS database."""
owl=Namespace('http://www.w3.org/2002/07/owl#')
"""Defines OWL namespace."""
frbroo=Namespace('http://www.ifla.org/files/assets/cataloguing/FRBRoo/frbroo_v_2.4.pdf#')
"""Defines frbroo namespace."""
pleiades_place=Namespace('http://pleiades.stoa.org/places/')
""""Namespace for places from Pleiades"""
geonames_place=Namespace('http://www.geonames.org/')
""""Namespace for places from Pleiades"""


EventFactoidList = [4, 7]
StateOfAffairsFactoid = [9, 11, 14]
#complete these lists after discussion with Tara

def create_factoid_uris(a):
    factoid_base_uri_str=f"{pbwfpo}factoid/{a}"
    return factoid_base_uri_str

# def create_person_uris(b):
#     person_base_uri_str=f"{pbwfpo}factoid/{b.personKey}"
#     return person_base_uri_str

def create_rdflabel(engDesc):
    if re.search('<PERSREF\sID=\\"(\d)*\\"\/>', engDesc):
        engDesc = re.sub('"\/>',"",engDesc)
        engDesc = re.sub('<PERSREF\sID=\\"', pbwfpo, engDesc)
    return(engDesc)

def create_factoid(engine, g, mysqlsession):
    #print(f"""pbw factoid: {fa.factoidKey}""")
    for a in mysqlsession.query(pbw.Factoid).all():
        factoid_base_uri_str = create_factoid_uris(a.factoidKey)
        g.add(((URIRef(f"{pbw_fpo_extension}{a.factoidTypeKey}"), RDFS.subClassOf, (URIRef(f"{fpocore}Factoid")))))
        g.add((URIRef(factoid_base_uri_str), RDF.type, (URIRef(f"{fpocore}{(a.factoidTypeKey)}"))))
        g.add((URIRef(f"{pbw_fpo_extension}{a.factoidTypeKey}"), RDFS.label, Literal(f"{a.factoidType} Factoid")))
        if a.factoidTypeKey in EventFactoidList:
            g.add((URIRef(f"{pbw_fpo_extension}factiodtype/{a.factoidTypeKey}"), RDFS.subClassOf, (URIRef(f"{fpocore}EventFactoid"))))
        if a.factoidTypeKey in StateOfAffairsFactoid:
            g.add((URIRef(f"{pbw_fpo_extension}factiodtype/{a.factoidTypeKey}"), RDFS.subClassOf, (URIRef(f"{fpocore}StateOfAffairsFactoid"))))
        g.add(((URIRef(factoid_base_uri_str)), fpocore.sourcedForm, (URIRef(f"{pbwfpo}SourceCitation/{a.factoidKey}"))))
        g.add((URIRef(f"{pbwfpo}SourceCitation/{a.factoidKey}"), RDF.type, fpocore.SourceCitation))
        g.add((URIRef(f"{pbwfpo}SourceCitation/{a.factoidKey}"), fpocore.hasPlaceInSource, Literal(a.sourceRef)))
        g.add((URIRef(f"{pbwfpo}SourceCitation/{a.factoidKey}"), fpocore.fromSource, URIRef(f"{pbwfpo}Source/{a.sourceKey}")))
        rdf_engDesc = create_rdflabel(a.engDesc)
        g.add((URIRef(factoid_base_uri_str), RDFS.label, Literal(rdf_engDesc, lang="en")))
        #language tags according to ISO 639-1
        if a.oLangKey == 5:
            g.add((URIRef(factoid_base_uri_str), RDFS.label, Literal(a.origLDesc, lang="hy")))
            #armenian original language tag
        elif a.oLangKey == 4:
            g.add((URIRef(factoid_base_uri_str), RDFS.label, Literal(a.origLDesc, lang="ar")))
            #arabic original language tag
        elif a.oLangKey == 3:
            g.add((URIRef(factoid_base_uri_str), RDFS.label, Literal(a.origLDesc, lang="la")))
            #latin original language tag
        elif a.oLangKey == 2:
            g.add((URIRef(factoid_base_uri_str), RDFS.label, Literal(a.origLDesc, lang="gr")))
            #greek original language tag
        else:
            g.add((URIRef(factoid_base_uri_str), RDFS.label, Literal(a.origLDesc)))
            #unspecified original language tag
    return g

def locationref(engine, g, mysqlsession):
    for h in mysqlsession.query(pbw.FactoidLocation):
        factoid_base_uri_str = create_factoid_uris(h.factoidKey)
        g.add ((URIRef(factoid_base_uri_str), fpocore.hasReference, URIRef((f"{pbwfpo}locationreference/{h.factoidKey}/{h.locationKey}"))))
        g.add ((URIRef(f"{pbwfpo}locationreference/{h.factoidKey}/{h.locationKey}"), RDF.type, fpocore.LocationReference))
        g.add ((URIRef(f"{pbwfpo}locationreference/{h.factoidKey}/{h.locationKey}"), fpocore.referencesLocation, URIRef(f"{pbwfpo}location/{h.locationKey}")))
    return g

def location(engine, g, mysqlsession):
    for i in mysqlsession.query(pbw.Location):
        g.add ((URIRef(f"{pbwfpo}location/{i.locationKey}"), RDF.type, fpocore.Location))
        g.add ((URIRef(f"{pbwfpo}locationreference/{i.locationKey}"), RDFS.label, Literal(i.locName, lang="en")))
        if i.oLangKey == 5:
            g.add((URIRef(f"{pbwfpo}location/{í.locationKey}"), RDFS.label, Literal(i.locNameOL, lang="hy")))
            #armenian original language tag
        elif i.oLangKey == 4:
            g.add((URIRef(f"{pbwfpo}location/{i.locationKey}"), RDFS.label, Literal(i.locNameOL, lang="ar")))
            #arabic original language tag
        elif i.oLangKey == 3:
            g.add((URIRef(f"{pbwfpo}location/{i.locationKey}"), RDFS.label, Literal(i.locNameOL, lang="la")))
            #latin original language tag
        elif i.oLangKey == 2:
            g.add((URIRef(f"{pbwfpo}location/{i.locationKey}"), RDFS.label, Literal(i.locNameOL, lang="gr")))
            #greek original language tag
        else:
            g.add((URIRef(f"{pbwfpo}location/{i.locationKey}"), RDFS.label, Literal(i.locNameOL)))
            #unspecified original language tag
        if i.geonames_id != None:
            g.add ((URIRef(f"{pbwfpo}location/{i.locationKey}"), owl.sameAs, URIRef(f"{geonames_place}{i.geonames_id}")))
        else:
            continue
        if i.pleiades_id != None:
            g.add ((URIRef(f"{pbwfpo}location/{i.locationKey}"), owl.sameAs, URIRef(f"{pleiades_place}{i.pleiades_id}")))
        else:
            continue
    return g

def create_source(engine, g, mysqlsession):
    for b in mysqlsession.query(pbw.Source).all():
        g.add((URIRef(f"{pbwfpo}Source/{b.sourceKey}"), RDFS.label, Literal(b.sourceID)))
        g.add((URIRef(f"{pbwfpo}Source/{b.sourceKey}"), RDF.type, fpocore.Source))
    return g

def create_occupation_factoid(engine, g, mysqlsession):
    for c in mysqlsession.query(pbw.occupation_association).all():
        factoid_base_uri_str = create_factoid_uris(c.factoidKey)
        g.add((URIRef(factoid_base_uri_str), fpocore.hasReference, URIRef(f"{pbwfpo}occupationreference/{c.OccupationKey}")))
    return g

def create_occupation_label(engine, g, mysqlsession):
    for d in mysqlsession.query(pbw.Occupation).all():
        g.add((URIRef(f"{pbwfpo}occupation/{d.occupationKey}"), RDFS.label, Literal(d.occupationName)))
        g.add((URIRef(f"{pbwfpo}occupation/{d.occupationKey}"), RDF.type, pbw_fpo_extension.Occupation))
    return g

def create_person_uris(pk):
    person_base_uri_str=f"{pbwfpo}Person/{pk}"
    return person_base_uri_str


def create_person_reference (engine, g, mysqlsession):
    for e in mysqlsession.query(pbw.FactoidPerson).all():
        factoid_base_uri = create_factoid_uris(e.factoidKey)
        person_uri = create_person_uris(e.personKey)
        g.add((URIRef(factoid_base_uri), fpocore.hasReference, (URIRef(f"{pbwfpo}personref/{e.fpKey}"))))
        g.add((URIRef(f"{pbwfpo}personref/{e.fpKey}"), RDF.type, fpocore.PersonReference))
        g.add((URIRef(f"{pbwfpo}personref/{e.fpKey}"), fpocore.referencesPerson, URIRef(person_uri)))
        g.add((URIRef(person_uri), RDF.type, fpocore.Person))
    return g

def create_person_labels(engine, g, mysqlsession):
    for f in mysqlsession.query(pbw.Person).all():
        person_uri = create_person_uris(f.personKey)
        g.add((URIRef(person_uri), RDFS.label, Literal(f.descName, lang="en")))
    return g


# def create_vname_association(engine, g, mysqlsession):
#     for e in mysqlsession.query(pbw.vname_association).all():
#         factoid_base_uri_str = create_factoid_uris(e.factoidKey)
#         g.add((URIRef(factoid_base_uri_str), fpocore.hasReference, URIRef(f"{pbwfpo}occupationreference/{c.OccupationKey}")))
#     return g

def serializeto_ttl(g):
    g.bind('pbwfpo', pbwfpo)
    g.bind('pbw_fpo_extension', pbw_fpo_extension)
    g.bind('fpocore', fpocore)
    g.bind('dc', dc)
    g.bind('crm', crm)
    g.bind('owl', owl)
    g.bind('rdf', rdf)
    g.bind('xml', xml)
    g.bind('xsd', xsd)
    g.bind('rdfs', rdfs)
    g.bind('apis', apis)
    g.bind('owl', owl)
    g.bind('frbroo', frbroo)
    #Bind namespaces to prefixes for readable output
    ex_pbw_fpo = g.serialize(destination=f'pbw_rdf_data_{datetime.now().strftime("%d-%m-%Y")}.ttl', format='turtle')
    #print(ex_pbw_fpo)
    return(ex_pbw_fpo)

def main(starttime, engine, mysqlsession):
    g = Graph()
    g = create_factoid(engine, g, mysqlsession)
    g = create_source(engine, g, mysqlsession)
    g = create_occupation_factoid(engine, g, mysqlsession)
    g = create_occupation_label(engine, g, mysqlsession)
    g = create_person_reference(engine, g, mysqlsession)
    g = create_person_labels(engine, g, mysqlsession)
    g = locationref(engine, g, mysqlsession)
    g = location(engine, g, mysqlsession)
    pbw_fpo = serializeto_ttl(g)


if __name__ == '__main__':
    # Connect to the SQL DB
    starttime = datetime.now()
    engine = create_engine(create_engine_str)
    smaker = sessionmaker(bind=engine)
    mysqlsession = smaker()
    main(starttime, engine, mysqlsession)