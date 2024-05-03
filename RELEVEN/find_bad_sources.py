from RELEVEN import graphimportSTAR, PBWSources
from rdflib import Graph
import csv

# Use the graphimportSTAR object to get the list of people we wanted to deal with
gimport = graphimportSTAR.graphimportSTAR()
ourpeople = gimport.collect_person_records()
skip_types = ['Narrative', 'Authorship', 'Education', 'Location', 'Eunuchs', 'Uncertain Ident']
# Initialise the canonical source list
srces = PBWSources.PBWSources('pbw_sources.csv')
# Initialise a graph that we can SPARQL against
g = Graph()
# Set up the SPARQL query that will grab the passage information
sparql = """
PREFIX star: <https://r11.eu/ns/star/>
PREFIX lrmoo: <http://iflastandards.info/ns/lrm/lrmoo/>
PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
select distinct ?passage ?bib ?ref where {
  service <https://graphdb.r11.eu/repositories/RELEVEN> {
    ?s a crm:E31_Document ;
       crm:P70_documents ?ass ;
       lrmoo:R76 <FCT> .
    ?ass crm:P17_was_motivated_by ?passage .
    ?link a star:E13_lrmoo_R15 ;
          crm:P140_assigned_attribute_to ?pub ;
          crm:P141_assigned ?passage .
    ?pub crm:P3_has_note ?bib .
    ?passage crm:P3_has_note ?ref.
    MINUS {
        ?passage crm:P190_has_symbolic_content ?ref .
    }
  }
}
"""

# Look at all the factoids and find out what source it should be using. Index by bibstring
passage_urls = dict()
omitted_factoids = []
extraneous_factoids = []
bad_refstrings = []
bad_passage_links = dict()
counted = 0
for person in ourpeople:
    for factoid in person.main_factoids():
        # We only care about the factoid types we ingested in the first place
        if factoid.factoidType in skip_types:
            continue
        # We only care about factoids from our composite sources
        if factoid.source not in srces.composites:
            continue
        # Find out which source this factoid should reference
        real_src_key = srces.key_for(factoid.source, factoid.sourceRef)
        real_src_obj = srces.get(real_src_key)
        real_src_ref = srces.sourceref(factoid.source, factoid.sourceRef)

        # Find the URI of the reference, and which source it claims to belong to
        ql = g.query(sparql.replace('FCT',
                                    f"https://pbw2016.kdl.kcl.ac.uk/rdf/factoid/{factoid.factoidKey}"))
        if len(ql) > 1:
            print(f"WARNING: factoid {factoid.factoidKey} had two source hits?!")
        elif len(ql) == 0 and real_src_key != 'OUT_OF_SCOPE':
            print(f"WARNING: factoid {factoid.factoidKey} ({factoid.source} {factoid.sourceRef} = {real_src_key} {real_src_ref}) got omitted!")
            omitted_factoids.append(factoid.factoidKey)
        for row in ql:
            passage = row.passage   # the URI for the passage
            bibstr = row.bib.value  # the bibliography string to test
            refstr = row.ref.value  # the reference. Sanity check this
            if refstr != real_src_ref:
                print(f"WARNING: factoid {factoid.factoidKey} ({factoid.source} {factoid.sourceRef}) has a divergent source reference!")
                bad_refstrings.append(factoid.factoidKey)
                continue
            if real_src_obj is None:
                print(f"WARNING: factoid {factoid.factoidKey} ({factoid.source} {factoid.sourceRef}) needs to be removed!")
                extraneous_factoids.append(factoid.factoidKey)
                continue
            # Now check that the bibstr matches the expression string of the real source
            if bibstr != real_src_obj.get('expression'):
                print(f"FOUND a bad source link for factoid {factoid.factoidKey}: {factoid.source} {factoid.sourceRef} should point to {real_src_key}")
                bad_passage_links[passage] = real_src_key
        counted += 1
        if counted % 100 == 0:
            print(f"...checked {counted} factoids")

with open('omitted_factoids.txt', 'w') as f:
    f.writelines([f"{f}\n" for f in omitted_factoids])

with open('extraneous_factoids.txt', 'w') as f:
    f.writelines([f"{f}\n" for f in extraneous_factoids])

with open('bad_refstrings.txt', 'w') as f:
    f.writelines([f"{f}\n" for f in bad_refstrings])

with open('wrong_biblinks.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['passage_uri', 'real_sourcekey'])
    for k, v in bad_passage_links.items():
        writer.writerow([k, v])

print("Done")