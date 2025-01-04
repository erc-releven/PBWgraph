from collections import defaultdict

import config
from rdflib.plugins.stores import sparqlstore
from RELEVEN import PBWstarConstants

store = sparqlstore.SPARQLStore(config.graphuri, method='POST',
                                      auth=(config.graphuser, config.graphpw))
c = PBWstarConstants.PBWstarConstants(store=store, readonly=True)
g = c.graph

oldrunid = 'data:c7de48bc-9a38-4b81-b31e-aa44ad901042'
newrunid = 'data:fff84297-327d-409d-88be-8c2edbb20405'
fromoldrun = f"^{c.get_label('L11')} {oldrunid}"
fromnewrun = f"{c.get_label('L11r')} {newrunid}"


def get_queries_for_type(ftype):
    if ftype in ['dignity', 'socialstatus', 'religion']:
        return get_social_queries(ftype)
    elif ftype == 'kinship':
        return get_relship_queries()
    elif ftype == 'language':
        return get_language_queries()
    elif ftype == 'death':
        return get_death_queries(False)
    elif ftype == 'deathdated':
        return get_death_queries(True)
    else:
        return get_simple_queries(ftype)


def get_simple_queries(ftype):
    pred = {'possession': 'P51',
            'ethnicity': 'P107',
            'appellation': 'P1',
            'inscription': 'P128',
            }[ftype]
    # Possessions have swapped subject and object
    if ftype in ['possession', 'ethnicity']:
        ppred = c.star_object
        tpred = c.star_subject
    else:
        ppred = c.star_subject
        tpred = c.star_object
    labelpred = c.get_label('P190') if ftype == 'appellation' else c.get_label('P1')
    oldq = f"""SELECT ?pnode ?person ?tnode ?thing ?pub ?srcreftxt ?srctype WHERE {{
        ?a a {c.get_assertion_for_predicate(pred)} ;
           {ppred} ?pnode ;
           {tpred} ?tnode ;
           {c.star_based} ?srcref ;
           {fromoldrun} .
        MINUS{{
            ?a {fromnewrun} .
        }}   
        ?tnode {labelpred} ?thing .
        ?pnode ^{c.star_subject} [a {c.get_label('E15')} ; 
                                  {c.get_label('P37')} [{c.get_label('P190')} ?person ] ] .
        
        # Get the source, either text or boulloterion
        {{
            ?sa {c.star_object} ?srcref ;
                a {c.get_assertion_for_predicate('R15')} ;
                {c.star_subject} [{c.get_label('P3')} ?pub; a ?srctype ] ;
                {fromoldrun} .
            ?srcref {c.get_label('P3')} ?srcreftxt .            
        }} UNION {{
            ?sa {c.star_object} ?srcref ;
                a {c.get_assertion_for_predicate('P128')} ;
                {c.star_subject} ?pubnode ;
                {fromoldrun} .
            ?srcref {c.get_label('P190')} ?srcreftxt .
            ?pubnode a ?srctype ;
                     ^{c.star_subject} [a {c.get_label('E15')} ; 
                                        {c.get_label('P37')} [{c.get_label('P190')} ?pub ] ] .
        }}
}}
"""

    def newqgen(row):
        return f"""SELECT ?pub WHERE {{
        ?a a {c.get_assertion_for_predicate(pred)} ;
           {ppred} {row['pnode'].n3()} ;
           {tpred} [{labelpred} {row['thing'].n3()}] ;
           {c.star_based} ?srcref ;
           {fromnewrun} .
        
        # Get the source, either text or boulloterion
        {{
            ?sa {c.star_object} ?srcref ;
                a {c.get_assertion_for_predicate('R15')} ;
                {c.star_subject} [{c.get_label('P3')} ?pub ; a {row['srctype'].n3()} ] ;
                {fromnewrun} .
            ?srcref {c.get_label('P3')} {row['srcreftxt'].n3()} .            
        }} UNION {{
            ?sa {c.star_object} ?srcref ;
                a {c.get_assertion_for_predicate('P128')} ;
                {c.star_subject} ?pubnode ;
                {fromnewrun} .
            ?srcref {c.get_label('P190')} {row['srcreftxt'].n3()} .
            ?pubnode a {row['srctype'].n3()} ;
                     ^{c.star_subject} [a {c.get_label('E15')} ; 
                                        {c.get_label('P37')} [{c.get_label('P190')} ?pub ] ] .
        }}}}"""

    return oldq, newqgen, 'thing'


def get_social_queries(ftype):
    (whopred, whichpred) = {'dignity': ('SP26', 'SP33'),
                            'socialstatus': ('SP13', 'SP14'),
                            'religion': ('SP36', 'SP35'),
                            'gender': ('P41', 'P42'),
                            }[ftype]
    oldq = f"""SELECT ?pnode ?person ?rnode ?role ?pub ?srcreftxt ?srctype WHERE {{
        ?a a {c.get_assertion_for_predicate(whichpred)} ;
           {c.star_subject} ?prole1 ;
           {c.star_object} ?rnode ;
           {c.star_based} ?srcref ;
           {fromoldrun} .
        MINUS{{
            ?a {fromnewrun} .
        }}
        ?b {c.star_subject} ?prole1 ;
           {c.star_based} ?srcref ;
           a {c.get_assertion_for_predicate(whopred)} ;
           {c.star_object} ?pnode ;
           {fromoldrun} .    
        ?rnode {c.get_label('P1')} ?role .
        ?pnode ^{c.star_subject} [a {c.get_label('E15')} ; 
                                  {c.get_label('P37')} [{c.get_label('P190')} ?person ] ] .
             
        # Get the source, either text or boulloterion
        {{
            ?sa {c.star_object} ?srcref ;
                a {c.get_assertion_for_predicate('R15')} ;
                {c.star_subject} [{c.get_label('P3')} ?pub; a ?srctype ] ;
                {fromoldrun} .
            ?srcref {c.get_label('P3')} ?srcreftxt .            
        }} UNION {{
            ?sa {c.star_object} ?srcref ;
                a {c.get_assertion_for_predicate('P128')} ;
                {c.star_subject} ?pubnode ;
                {fromoldrun} .
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
           {fromnewrun} .
        ?b a {c.get_assertion_for_predicate(whopred)} ;
           {c.star_subject} ?prole1 ;
           {c.star_object} {row['pnode'].n3()} ;
           {c.star_based} ?srcref ;
           {fromnewrun} .
        
        # Get the source, either text or boulloterion
        {{
            ?sa {c.star_object} ?srcref ;
                a {c.get_assertion_for_predicate('R15')} ;
                {c.star_subject} [{c.get_label('P3')} ?pub ; a {row['srctype'].n3()} ] ;
                {fromnewrun} .
            ?srcref {c.get_label('P3')} {row['srcreftxt'].n3()} .            
        }} UNION {{
            ?sa {c.star_object} ?srcref ;
                a {c.get_assertion_for_predicate('P128')} ;
                {c.star_subject} ?pubnode ;
                {fromnewrun} .
            ?srcref {c.get_label('P190')} {row['srcreftxt'].n3()} .
            ?pubnode a {row['srctype'].n3()} ;
                     ^{c.star_subject} [a {c.get_label('E15')} ; 
                                        {c.get_label('P37')} [{c.get_label('P190')} ?pub ] ] .
        }}}}"""
    return oldq, newqgen, 'role'


def get_language_queries():
    oldq = f"""SELECT ?pnode ?person ?lskill ?lang ?pub ?srcreftxt ?srctype WHERE {{
            ?a a {c.get_assertion_for_predicate('SP38')} ;
               {c.star_subject} ?pnode ;
               {c.star_object} ?plang1 ;
               {c.star_based} ?srcref ;
               {fromoldrun} .
            MINUS{{
                ?a {fromnewrun} .
            }}
            ?b {c.star_subject} ?plang1 ;
               {c.star_based} ?srcref ;
               a {c.get_assertion_for_predicate('SP37')} ;
               {c.star_object} ?lskill ;
               {fromoldrun} .    
            ?lskill {c.get_label('P1')} ?lang .
            ?pnode ^{c.star_subject} [a {c.get_label('E15')} ; 
                                      {c.get_label('P37')} [{c.get_label('P190')} ?person ] ] .

            # Get the source, either text or boulloterion
            {{
                ?sa {c.star_object} ?srcref ;
                    a {c.get_assertion_for_predicate('R15')} ;
                    {c.star_subject} [{c.get_label('P3')} ?pub; a ?srctype ] ;
                    {fromoldrun} .
                ?srcref {c.get_label('P3')} ?srcreftxt .            
            }} UNION {{
                ?sa {c.star_object} ?srcref ;
                    a {c.get_assertion_for_predicate('P128')} ;
                    {c.star_subject} ?pubnode ;
                    {fromoldrun} .
                ?srcref {c.get_label('P190')} ?srcreftxt .
                ?pubnode a ?srctype ;
                         ^{c.star_subject} [a {c.get_label('E15')} ; 
                                            {c.get_label('P37')} [{c.get_label('P190')} ?pub ] ] .
            }}
    }}
    """

    def newqgen(row):
        return f"""SELECT ?pub WHERE {{
            ?a a {c.get_assertion_for_predicate('SP38')} ;
               {c.star_subject} {row['pnode'].n3()} ;
               {c.star_object} ?plang1 ;
               {c.star_based} ?srcref ;
               {fromnewrun} .
            ?b {c.star_subject} ?plang1 ;
               {c.star_based} ?srcref ;
               a {c.get_assertion_for_predicate('SP37')} ;
               {c.star_object} {row['lskill'].n3()} ;
               {fromnewrun} .

            # Get the source, either text or boulloterion
            {{
                ?sa {c.star_object} ?srcref ;
                    a {c.get_assertion_for_predicate('R15')} ;
                    {c.star_subject} [{c.get_label('P3')} ?pub ; a {row['srctype'].n3()} ] ;
                    {fromnewrun} .
                ?srcref {c.get_label('P3')} {row['srcreftxt'].n3()} .            
            }} UNION {{
                ?sa {c.star_object} ?srcref ;
                    a {c.get_assertion_for_predicate('P128')} ;
                    {c.star_subject} ?pubnode ;
                    {fromnewrun} .
                ?srcref {c.get_label('P190')} {row['srcreftxt'].n3()} .
                ?pubnode a {row['srctype'].n3()} ;
                         ^{c.star_subject} [a {c.get_label('E15')} ; 
                                            {c.get_label('P37')} [{c.get_label('P190')} ?pub ] ] .
            }}}}"""
    return oldq, newqgen, 'lang'


def get_relship_queries():
    oldq = f"""SELECT ?pnode ?person ?kpnode ?kperson ?ktype ?relship ?pub ?srcreftxt ?srctype WHERE {{
            ?a a {c.get_assertion_for_predicate('SP16')} ;
               {c.star_subject} ?knode ;
               {c.star_object} ?ktype ;
               {c.star_based} ?srcref ;
               {fromoldrun} .
            ?b {c.star_subject} ?knode ;
               {c.star_based} ?srcref ;
               a {c.get_assertion_for_predicate('SP17')} ;
               {c.star_object} ?pnode ;
               {fromoldrun} .    
            ?c {c.star_subject} ?knode ;
               {c.star_based} ?srcref ;
               a {c.get_assertion_for_predicate('SP18')} ;
               {c.star_object} ?kpnode ;
               {fromoldrun} .    
            MINUS{{
                ?a {fromnewrun} .
            }}

            ?ktype {c.get_label('P1')} ?relship .
            ?pnode ^{c.star_subject} [a {c.get_label('E15')} ; 
                                      {c.get_label('P37')} [{c.get_label('P190')} ?person ] ] .
            ?kpnode ^{c.star_subject} [a {c.get_label('E15')} ; 
                                      {c.get_label('P37')} [{c.get_label('P190')} ?kperson ] ] .

            # Get the source, either text or boulloterion
            {{
                ?sa {c.star_object} ?srcref ;
                    a {c.get_assertion_for_predicate('R15')} ;
                    {c.star_subject} [{c.get_label('P3')} ?pub; a ?srctype ] ;
                    {fromoldrun} .
                ?srcref {c.get_label('P3')} ?srcreftxt .            
            }} UNION {{
                ?sa {c.star_object} ?srcref ;
                    a {c.get_assertion_for_predicate('P128')} ;
                    {c.star_subject} ?pubnode ;
                    {fromoldrun} .
                ?srcref {c.get_label('P190')} ?srcreftxt .
                ?pubnode a ?srctype ;
                         ^{c.star_subject} [a {c.get_label('E15')} ; 
                                            {c.get_label('P37')} [{c.get_label('P190')} ?pub ] ] .
            }}
    }}
    """

    def newqgen(row):
        return f"""SELECT ?pub WHERE {{
            ?c {c.star_object} {row['kpnode'].n3()} ;
               a {c.get_assertion_for_predicate('SP18')} ;
               {c.star_subject} ?knode ;
               {c.star_based} ?srcref ;
               {fromnewrun} .   
            ?b {c.star_subject} ?knode ;
               {c.star_object} {row['pnode'].n3()} ;
               {c.star_based} ?srcref ;
               a {c.get_assertion_for_predicate('SP17')} ;
               {fromnewrun} .    
            ?a {c.star_subject} ?knode ;
               {c.star_object} {row['ktype'].n3()} ;
               a {c.get_assertion_for_predicate('SP16')} ;
               {c.star_based} ?srcref ;
               {fromnewrun} .

            # Get the source, either text or boulloterion
            {{
                ?sa {c.star_object} ?srcref ;
                    a {c.get_assertion_for_predicate('R15')} ;
                    {c.star_subject} [{c.get_label('P3')} ?pub ; a {row['srctype'].n3()} ] ;
                    {fromnewrun} .
                ?srcref {c.get_label('P3')} {row['srcreftxt'].n3()} .            
            }} UNION {{
                ?sa {c.star_object} ?srcref ;
                    a {c.get_assertion_for_predicate('P128')} ;
                    {c.star_subject} ?pubnode ;
                    {fromnewrun} .
                ?srcref {c.get_label('P190')} {row['srcreftxt'].n3()} .
                ?pubnode a {row['srctype'].n3()} ;
                         ^{c.star_subject} [a {c.get_label('E15')} ; 
                                            {c.get_label('P37')} [{c.get_label('P190')} ?pub ] ] .
            }}}}"""
    return oldq, newqgen, 'relship'

def get_death_queries(dated=False):
    # For the death queries, the P100 assertion won't have authority or source
    dateqold = f"""             ?c {c.star_subject} ?pdeath ;
                {c.star_based} ?srcref ;
                a {c.get_assertion_for_predicate('P4')} ;
                {c.star_object} ?ddate ;
                {fromoldrun} .
"""
    oldq = f"""SELECT ?pnode ?person ?death ?ddesc ?ddate ?pub ?srcreftxt ?srctype WHERE {{
             ?a a {c.get_assertion_for_predicate('P100')} ;
                {c.star_subject} ?pdeath ;
                {c.star_object} ?pnode ;
                {fromoldrun} .
             MINUS{{
                 ?a {fromnewrun} .
             }}
             ?b {c.star_subject} ?pdeath ;
                {c.star_based} ?srcref ;
                a {c.get_assertion_for_predicate('P3')} ;
                {c.star_object} ?ddesc ;
                {fromoldrun} .
             {dateqold if dated else ''} 
             ?pnode ^{c.star_subject} [a {c.get_label('E15')} ; 
                                       {c.get_label('P37')} [{c.get_label('P190')} ?person ] ] .
             ?pdeath {c.get_label('P3')} ?death .

             # Get the source, either text or boulloterion
             {{
                 ?sa {c.star_object} ?srcref ;
                     a {c.get_assertion_for_predicate('R15')} ;
                     {c.star_subject} [{c.get_label('P3')} ?pub; a ?srctype ] ;
                     {fromoldrun} .
                 ?srcref {c.get_label('P3')} ?srcreftxt .            
             }} UNION {{
                 ?sa {c.star_object} ?srcref ;
                     a {c.get_assertion_for_predicate('P128')} ;
                     {c.star_subject} ?pubnode ;
                     {fromoldrun} .
                 ?srcref {c.get_label('P190')} ?srcreftxt .
                 ?pubnode a ?srctype ;
                          ^{c.star_subject} [a {c.get_label('E15')} ; 
                                             {c.get_label('P37')} [{c.get_label('P190')} ?pub ] ] .
             }}
     }}
     """

    def newqgen(row):
        dateqnew = f"""             ?c {c.star_subject} ?pdeath;
                {c.star_based} ?srcref;
                a {c.get_assertion_for_predicate('P4')};
                {c.star_object} ?ddate;
                {fromnewrun} ."""

        return f"""SELECT ?pub WHERE {{
             ?a {c.star_object} {row['pnode'].n3()} ;
                a {c.get_assertion_for_predicate('P100')} ;
                {c.star_subject} ?pdeath ;
                {fromnewrun} .
             ?b {c.star_subject} ?pdeath ;
                {c.star_based} ?srcref ;
                a {c.get_assertion_for_predicate('P3')} ;
                {c.star_object} {row['ddesc'].n3()} ;
                {fromnewrun} .
             {dateqnew if dated else ''}
             # Get the source, either text or boulloterion
             {{
                 ?sa {c.star_object} ?srcref ;
                     a {c.get_assertion_for_predicate('R15')} ;
                     {c.star_subject} [{c.get_label('P3')} ?pub ; a {row['srctype'].n3()} ] ;
                     {fromnewrun} .
                 ?srcref {c.get_label('P3')} {row['srcreftxt'].n3()} .            
             }} UNION {{
                 ?sa {c.star_object} ?srcref ;
                     a {c.get_assertion_for_predicate('P128')} ;
                     {c.star_subject} ?pubnode ;
                     {fromnewrun} .
                 ?srcref {c.get_label('P190')} {row['srcreftxt'].n3()} .
                 ?pubnode a {row['srctype'].n3()} ;
                          ^{c.star_subject} [a {c.get_label('E15')} ; 
                                             {c.get_label('P37')} [{c.get_label('P190')} ?pub ] ] .
             }}}}"""

    return oldq, newqgen, 'death'


if __name__ == '__main__':
    # TODO boulloterion multiplicity
    # 'socialstatus', 'dignity', 'religion', 'language', 'kinship', 'possession', 'ethnicity', 'death', 'deathdated'
    to_check = ['appellation']
    pubchanges = defaultdict(set)
    for assertion_type in to_check:
        print(f'* * * Checking {assertion_type} assertions * * *')
        q, nqgen, label = get_queries_for_type(assertion_type)

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
            d = f"{row['person']} | {row[label]} | "
            if assertion_type == 'kinship':
                d += f"{row['kperson']} | "
            d += f"boulloterion/{row['pub']}" if str(row['srctype']).endswith('Boulloterion') else f"{row['pub']}"
            d += f" | {row['srcreftxt']}"
            # The publication string might be different but the source reference needs to be the same
            nq = nqgen(row)
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
                    # Save the new publication information for sanity checking
                    pubchanges[res[0]['pub']].add(row['srcreftxt'])
                    # print(f"Publication change:\n\t{row['pub']} to \n\t{res[0]['pub']}\n\tfor {d} ")
                    # i = 0
                    fixedpub += 1

        print(f"Matched {matched} outdated {assertion_type} assertions, of which {fixedpub} had a publication fixed.")
        if len(missing) > 0:
            print("Missing assertions:")
            print("\n".join(sorted(missing)))
        if len(extra) > 0:
            print("Extra assertions:")
            print("\n".join(sorted(extra)))

    print("* * * Publication re-linking * * *")
    if len(pubchanges) > 0:
        for k, v in pubchanges.items():
            print(f"References linked to {k}: {';'.join(v)}")

print("Done!")
