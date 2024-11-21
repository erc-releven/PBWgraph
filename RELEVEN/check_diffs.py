import config
from rdflib.plugins.stores import sparqlstore
from RELEVEN import PBWstarConstants

origgraph = config.graphuri
store = sparqlstore.SPARQLUpdateStore(origgraph, origgraph + '/statements', method='POST',
                                      auth=(config.graphuser, config.graphpw))
c = PBWstarConstants.PBWstarConstants(store=store)
g = c.graph

oldrunid = 'data:c7de48bc-9a38-4b81-b31e-aa44ad901042'
newrunid = 'data:f2d72930-ee1b-4ba7-8323-ced2a0c28d95'


def get_queries_for_type(ftype):
    if ftype == 'dignity':
        return get_social_queries('SP26', 'SP33')
    elif ftype == 'socialstatus':
        return get_social_queries('SP13', 'SP14')
    elif ftype == 'religion':
        return get_social_queries('SP36', 'SP35')
    elif ftype == 'language':
        return get_language_queries()


def get_social_queries(whopred, whichpred):
    oldq = f"""SELECT ?pnode ?person ?rnode ?role ?pub ?srcreftxt ?srctype WHERE {{
        ?a a {c.get_assertion_for_predicate(whichpred)} ;
           {c.star_subject} ?prole1 ;
           {c.star_object} ?rnode ;
           {c.star_based} ?srcref ;
           ^{c.get_label('L11')} {oldrunid} .
        MINUS{{
            ?a {c.get_label('L11r')} {newrunid} .
        }}
        ?b {c.star_subject} ?prole1 ;
           {c.star_based} ?srcref ;
           a {c.get_assertion_for_predicate(whopred)} ;
           {c.star_object} ?pnode ;
           ^{c.get_label('L11')} {oldrunid} .    
        ?rnode {c.get_label('P1')} ?role .
        ?pnode ^{c.star_subject} [a {c.get_label('E15')} ; 
                                  {c.get_label('P37')} [{c.get_label('P190')} ?person ] ] .
             
        # Get the source, either text or boulloterion
        {{
            ?sa {c.star_object} ?srcref ;
                a {c.get_assertion_for_predicate('R15')} ;
                {c.star_subject} [{c.get_label('P3')} ?pub; a ?srctype ] ;
                ^{c.get_label('L11')} {oldrunid} .
            ?srcref {c.get_label('P3')} ?srcreftxt .            
        }} UNION {{
            ?sa {c.star_object} ?srcref ;
                a {c.get_assertion_for_predicate('P128')} ;
                {c.star_subject} ?pubnode ;
                ^{c.get_label('L11')} {oldrunid} .
            ?srcref {c.get_label('P190')} ?srcreftxt .
            ?pubnode a ?srctype ;
                     ^{c.star_subject} [a {c.get_label('E15')} ; 
                                        {c.get_label('P37')} [{c.get_label('P190')} ?pub ] ] .
        }}
}}
"""

    def newqgen(row):
        return f"""SELECT ?pub WHERE {{
        ?a a {c.get_assertion_for_predicate(whichpred)} ;
           {c.star_subject} ?prole1 ;
           {c.star_object} {row['rnode'].n3()} ;
           {c.star_based} ?srcref ;
           {c.get_label('L11r')} {newrunid} .
        ?b a {c.get_assertion_for_predicate(whopred)} ;
           {c.star_subject} ?prole1 ;
           {c.star_object} {row['pnode'].n3()} ;
           {c.star_based} ?srcref ;
           {c.get_label('L11r')} {newrunid} .
        
        # Get the source, either text or boulloterion
        {{
            ?sa {c.star_object} ?srcref ;
                a {c.get_assertion_for_predicate('R15')} ;
                {c.star_subject} [{c.get_label('P3')} ?pub ; a {row['srctype'].n3()} ] ;
                {c.get_label('L11r')} {newrunid} .
            ?srcref {c.get_label('P3')} {row['srcreftxt'].n3()} .            
        }} UNION {{
            ?sa {c.star_object} ?srcref ;
                a {c.get_assertion_for_predicate('P128')} ;
                {c.star_subject} ?pubnode ;
                {c.get_label('L11r')} {newrunid} .
            ?srcref {c.get_label('P190')} {row['srcreftxt'].n3()} .
            ?pubnode a {row['srctype'].n3()} ;
                     ^{c.star_subject} [a {c.get_label('E15')} ; 
                                        {c.get_label('P37')} [{c.get_label('P190')} ?pub ] ] .
        }}}}"""
    return oldq, newqgen


def get_language_queries():
    oldq = f"""SELECT ?pnode ?person ?lskill ?lang ?pub ?srcreftxt ?srctype WHERE {{
            ?a a {c.get_assertion_for_predicate('SP38')} ;
               {c.star_subject} ?pnode ;
               {c.star_object} ?plang1 ;
               {c.star_based} ?srcref ;
               ^{c.get_label('L11')} {oldrunid} .
            MINUS{{
                ?a {c.get_label('L11r')} {newrunid} .
            }}
            ?b {c.star_subject} ?plang1 ;
               {c.star_based} ?srcref ;
               a {c.get_assertion_for_predicate('SP37')} ;
               {c.star_object} ?lskill ;
               ^{c.get_label('L11')} {oldrunid} .    
            ?lskill {c.get_label('P1')} ?lang .
            ?pnode ^{c.star_subject} [a {c.get_label('E15')} ; 
                                      {c.get_label('P37')} [{c.get_label('P190')} ?person ] ] .

            # Get the source, either text or boulloterion
            {{
                ?sa {c.star_object} ?srcref ;
                    a {c.get_assertion_for_predicate('R15')} ;
                    {c.star_subject} [{c.get_label('P3')} ?pub; a ?srctype ] ;
                    ^{c.get_label('L11')} {oldrunid} .
                ?srcref {c.get_label('P3')} ?srcreftxt .            
            }} UNION {{
                ?sa {c.star_object} ?srcref ;
                    a {c.get_assertion_for_predicate('P128')} ;
                    {c.star_subject} ?pubnode ;
                    ^{c.get_label('L11')} {oldrunid} .
                ?srcref {c.get_label('P190')} ?srcreftxt .
                ?pubnode a ?srctype ;
                         ^{c.star_subject} [a {c.get_label('E15')} ; 
                                            {c.get_label('P37')} [{c.get_label('P190')} ?pub ] ] .
            }}
    }}
    """

    def newqgen(row):
        f"""SELECT ?pub WHERE {{
            ?a a {c.get_assertion_for_predicate('SP38')} ;
               {c.star_subject} {row['pnode'].n3()} ;
               {c.star_object} ?plang1 ;
               {c.star_based} ?srcref ;
               {c.get_label('L11r')} {newrunid} .
            ?b {c.star_subject} ?plang1 ;
               {c.star_based} ?srcref ;
               a {c.get_assertion_for_predicate('SP37')} ;
               {c.star_object} {row['lskill'].n3()} ;
               {c.get_label('L11r')} {newrunid} .

            # Get the source, either text or boulloterion
            {{
                ?sa {c.star_object} ?srcref ;
                    a {c.get_assertion_for_predicate('R15')} ;
                    {c.star_subject} [{c.get_label('P3')} ?pub ; a {row['srctype'].n3()} ] ;
                    {c.get_label('L11r')} {newrunid} .
                ?srcref {c.get_label('P3')} {row['srcreftxt'].n3()} .            
            }} UNION {{
                ?sa {c.star_object} ?srcref ;
                    a {c.get_assertion_for_predicate('P128')} ;
                    {c.star_subject} ?pubnode ;
                    {c.get_label('L11r')} {newrunid} .
                ?srcref {c.get_label('P190')} {row['srcreftxt'].n3()} .
                ?pubnode a {row['srctype'].n3()} ;
                         ^{c.star_subject} [a {c.get_label('E15')} ; 
                                            {c.get_label('P37')} [{c.get_label('P190')} ?pub ] ] .
            }}}}"""
    return oldq, newqgen


if __name__ == '__main__':
    to_check = ['socialstatus'] # , 'dignity', 'religion', 'language'
    for assertion_type in to_check:
        print(f'* * * Checking {assertion_type} assertions * * *')
        q, nqgen = get_queries_for_type(assertion_type)

        # Grab the old assertions
        rows = [x for x in c.graph.query(q)]
        print(f"Found {len(rows)} outdated {assertion_type} assertions")

        # For each one, see if there is a new one that matches it
        missing = set()
        extra = set()
        matched = 0
        fixedpub = 0
        i = 0
        for row in rows:
            i += 1
            print(".", end="" if i % 80 else "\n", flush=True)
            d = f"{row['person']} | {row['role']} | "
            d += f"boulloterion/{row['pub']}" if str(row['srctype']).endswith('Boulloterion') else f"{row['pub']}"
            d += f" | {row['srcreftxt']}"
            # The publication string might be different but the source reference needs to be the same
            nq = nqgen(row)
            # DEBUG
            if str(row['person']) == 'Konstantina 20101':
                print(f"Orig query was:\n{q}")
                print(f"New query was:\n{nq}")
            # Get the list of pub strings for otherwise matching assertions
            res = [x for x in c.graph.query(nq)]
            if len(res) == 0:
                # print(f"Found no new assertion for outdated {d}")
                missing.add(d)
            elif len(res) > 1:
                # print(f"Found {len(res)} new assertions for outdated {d}")
                extra.add(d)
            else:
                matched += 1
                if res[0]['pub'] != row['pub']:
                    # Check the PBW sourcelist to see if the new publication is correct

                    print(f"Publication change:\n\t{row['pub']} to \n\t{res[0]['pub']}\n\tfor {d} ")
                    fixedpub += 1
                    i = 0

        print(f"Matched {matched} outdated {assertion_type} assertions, of which {fixedpub} had a publication fixed.")
        if len(missing) > 0:
            print("Missing assertions:")
            print("\n".join(sorted(missing)))
        if len(extra) > 0:
            print("Extra assertions:")
            print("\n".join(sorted(extra)))

print("Done!")
