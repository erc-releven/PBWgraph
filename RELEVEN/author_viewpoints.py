import config
import RELEVEN.PBWstarConstants
from rdflib import Literal
from rdflib.plugins.stores import sparqlstore

if __name__ == '__main__':
    store = sparqlstore.SPARQLUpdateStore(config.graphuri, config.graphuri + '/statements', method='POST',
                                          auth=(config.graphuser, config.graphpw))
    c = RELEVEN.PBWstarConstants.PBWstarConstants(store=store)
    # Get all our PBW texts that have authors
    expression_sparql = f"""
SELECT DISTINCT ?author ?expression ?label WHERE {{
    ?a3 a {c.get_assertion_for_predicate('R17')} ;
        {c.star_subject} ?creation ;
        {c.star_object} ?expression .
    ?a4 a {c.get_assertion_for_predicate('P14')} ;
        {c.star_subject} ?creation ;
        {c.star_object} ?author .
    ?expression {c.label_n3} ?label .
}}
"""
    # Get the uri strings for the author's respective viewpoints
    expressions = dict()
    res = c.graph.query(expression_sparql)
    for row in res:
        # The viewpoints will be named after the last component of the expression's URI
        expressions[row['expression']] = {'tag': row['expression'].split('-')[-1],
                                          'author': row['author'],
                                          'label': row['label']}

    # Now cycle through the expressions and bundle the assertions that were made by their authors
    # into individual viewpoints.
    for e, edata in expressions.items():
        print(f"Adding viewpoint structure for: {edata['label']}")
        an3 = edata['author'].n3()
        en3 = e.n3()
        pset = c.ns[f"proposition_set/text{edata['tag']}"].n3()
        mbelief = c.ns[f"meaning/pbw{edata['tag']}"].n3()
        claim = c.ns[f"claim/pbw{edata['tag']}"].n3()
        viewpoint_sparql = f"""
INSERT {{
        {pset} a {c.get_label('I4')} ;    # A set of assertions come from a single text.
              {c.get_label('L11r')} {c.swrun.n3()} ;
              {c.get_label('J28')} ?a .
        {mbelief} a {c.get_label('I2')} ;           # The text expresses a belief that all these assertions are true.
                {c.get_label('J4')} {pset} ;
                {c.get_label('J5')} true .
        {claim} a {c.get_label('I1')} ;     # The argumentation (well, expression) of this belief was made by our author.
               {c.get_label('J2')} {mbelief} ;
               {c.get_label('P14')} {an3} .
        ?edition {c.star_src} {claim} .    # The text is the source of this argumentation.
}} WHERE {{
        ?a {c.star_auth} {an3} ;       # some assertion has our author as an authority
           ^{c.star_src} ?passage .    # the assertion was based on some passage... 
        ?a1 a {c.get_assertion_for_predicate('R15')} ;   # which comes from some publication... 
            {c.star_subject} ?edition ;
            {c.star_object} ?passage .
        ?a2 a {c.get_assertion_for_predicate('R76')} ;   # which is the publication of our text expression.
            {c.star_subject} ?edition ;
            {c.star_object} {en3} .
}}
"""
        c.graph.update(viewpoint_sparql)

    # Document this script
    responsible = c.graph.value(None, c.entity_label, Literal('Andrews, Tara Lee'))
    c.record_script_run(responsible)