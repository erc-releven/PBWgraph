import csv
import pbw
import config
import re
from collections import OrderedDict
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from warnings import warn


def add_if_exists(dict, key, datum):
    if datum is not None and not (re.match(r'^\s*$', datum) or datum == 'n/a'):
        dict[key] = datum


def add_one_or_more(dict, key, datum):
    if datum is None or re.match(r'^\s*$', datum) or datum == 'n/a':
        return
    dict[key] = re.split(r';\s*', datum)


def add_editors(dict, source_id, editor_names, editor_viafs):
    """Add the editors of the source edition as a list of {identifier: 'name', viaf: 'viafcode'}"""
    namelist = re.split(r';\s*', editor_names)
    viaflist = re.split(r';\s*', editor_viafs)
    editors = []
    if len(namelist) != len(viaflist):
        raise ImportError(f"Editor list mismatch on line {source_id}")
    for i, name in enumerate(namelist):
        editors.append({'identifier': name, 'viaf': viaflist[i]})
    dict['editor'] = editors


def add_pbw_authorities(dict, authority_string):
    # These are the modern scholars who put the source information into PBW records
    pbw_authorities = dict()
    pbw_authorities['mj'] = {'identifier': 'Jeffreys, Michael J.', 'viaf': '73866641'}
    pbw_authorities['tp'] = {'identifier': 'Papacostas, Tassos', 'viaf': '316793603'}
    pbw_authorities['ta'] = {'identifier': 'Andrews, Tara Lee', 'viaf': '316505144'}
    pbw_authorities['jr'] = {'identifier': 'Ryder, Judith R.', 'viaf': '51999624'}
    pbw_authorities['mw'] = {'identifier': 'Whitby, Mary', 'viaf': '34477027'}
    pbw_authorities['wa'] = {'identifier': 'Amin, Wahid M.', 'viaf': '213149617100303751518'}
    pbw_authorities['bs'] = {'identifier': 'Soravia, Bruna', 'viaf': '69252167'}
    pbw_authorities['hm'] = {'identifier': 'Munt, Harry', 'viaf': '78568758'}
    pbw_authorities['lo'] = {'identifier': 'Osti, Letizia', 'viaf': '236145542536996640148'}
    pbw_authorities['cr'] = {'identifier': 'Roueché, Charlotte', 'viaf': '44335536'}
    pbw_authorities['ok'] = {'identifier': 'Karágiṓrgou, ́Olga', 'viaf': '253347413'}

    pbwauths = re.split(r';\s*', authority_string)
    dict['authority'] = [pbw_authorities[x] for x in pbwauths]


def add_authors(dict, source_id, author_string):
    """Add any PBW authors in the form [name1, code1, name2, code2, ...]"""
    authorlist = re.split(r';\s*', author_string)
    authors = []
    for author in authorlist:
        pbwid = re.match(r'^(\w+) (\d+)', author)
        if pbwid is not None:
            authors.extend([pbwid.group(1), pbwid.group(2)])
    if len(authors) == 0:
        warn(f"Author {author_string} not created for source {source_id}")
    else:
        dict['author'] = authors


def add_provenance(session, dict, source_id, prov_string):
    """We have to check whether it is a factoid number, or the text of a factoid, or something else entirely"""
    # Do we have the factoid ID?
    factoid_id = re.match(r'factoid (\d+)', prov_string)
    if factoid_id is not None:
        dict['factoid'] = factoid_id.group(1)
        return
    # Can we match the provenance string to a factoid?
    factoid_search = re.sub(r'[A-Z]\w+ \d+', '%', prov_string)
    matched = session.query(pbw.Factoid).filter(pbw.Factoid.engDesc.like(factoid_search)).all()
    if len(matched) > 1:
        # See if we can find the exact match
        warn(f"Looking for exact factoid description match for source {source_id}")
        for f in matched:
            engDesc = f.replace_referents().replace('<', '').replace('>', '')
            if engDesc == prov_string:
                warn(f"...matched factoid {f.factoidKey}")
                dict['factoid'] = f.factoidKey
    elif len(matched):
        dict['factoid'] = matched[0].factoidKey
    else:
        # This didn't match a factoid. Put it in as a provenance
        dict['provenance'] = prov_string


def ingest(csvfile):
    """Given a CSV file of sources in the expected format, turn it into a data structure recognisable
    by PBWStarConstants"""
    sourcelist = dict()

    engine = create_engine('mysql+pymysql://' + config.dbstring)
    smaker = sessionmaker(bind=engine)
    session = smaker()

    with open(csvfile, encoding='utf-8', newline='') as fh:
        reader = csv.DictReader(fh, delimiter=';')
        for row in reader:
            source_id = row['PBW Source ID']
            source_data = dict()
            add_authors(source_data, source_id, row['Author(ity)'])
            add_provenance(session, source_data, source_id, row['Evidence of authorship'])
            add_pbw_authorities(source_data, row['PBW editor'])
            add_if_exists(source_data, 'work', row['Source canonical name'])
            add_if_exists(source_data, 'expression', row['Source edition used'])
            add_if_exists(source_data, 'url', row['Edition URL'])
            sourcelist[source_id] = source_data
    return sourcelist


def page_to_num(refstring, prefix, ranges, prefix_in_refstring=False, dotted=False):
    """Given a refstring, the common prefix, and a dictionary of items and their page ranges,
    return which item the refstring is pointing to."""
    # Get the number we are comparing
    matchstring = refstring
    if prefix_in_refstring:
        matchstring = refstring.replace(prefix, '')
    if dotted:
        matcher = re.match(r'\s*(\d+)\.(\d+)', matchstring)
    else:
        matcher = re.match(r'^\s*(d+)', matchstring)
    if matcher:
        x = int(matcher.group(1))
    else:
        raise(f"Unable to find number to compare in {refstring}")
    for n, r in ranges.items():
        if x in range(*r):
            return f"{prefix} {n}"



def parse_psellos_ref(refstring):
    # All the static strings
    for val in ['Actum 2', 'Against Ophrydas', 'Andronikos', 'Apologetikos', 'De omnifari doctrina', 'Eirene',
                'Epiphanios', 'Hypomnema', 'Kategoria', 'Keroularios', 'Leichoudes', 'Malik-shah', 'Mother',
                'Nikolaos of Horaia Pege', 'Niketas Maïstor', 'Philosophica minora I', 'Robert', 'Styliane',
                'Xiphilinos']:
        if refstring.startswith(val):
            return f'Psellos, {val}'

    # All the pre-numbered strings
    for pattern in [r'(Orationes panegyricae [IVX]+)', r'(Poema \d+)\.', ]:
        m = re.match(pattern, refstring)
        if m:
            return f'Psellos, {m.group(1)}'
    # Monodies
    if refstring.startswith('Monodies (Gautier)'):
        return "Psellos, %s" % page_to_num(refstring, 'Monodies (Gautier)', {
            1: (98, 104), 2: (107, 112), 3: (115, 126), 4: (128, 132), 5: (135, 143), 6: (145, 151)
        })
    # Oratoria minora
    if refstring.startswith('Oratoria minora'):
        return page_to_num(refstring, 'Psellos, Oratoria minora', OrderedDict({
            r'[1-3]\.': 1,
            r'4\.9\d': 1,
            r'4\.10\d\.': 1,
            r'[4-6]\.': 2,
        }), True)

    # Letters
    m = re.match(r'Letters \(([\w -]+)\) (\d+)', refstring)
    if m:
        return f'Psellos, {m.group(1)} {m.group(2)}'



def get_source_info(sourcelist, pbw_id, pbw_ref):
    aggregate_sources = {
        # Some of our sources are actually multiple works. Here is the key to disambiguate them: either
        # a list of starting strings or a map of regexp -> starting string.
        'Alexios Stoudites': ['Eleutherios', 'Ralles-Potles', 'VV', ''],
        'Docheiariou': lambda s: page_to_num(s, 'Docheiariou', OrderedDict(
            {r'53': '1', r'58\.[23]\d': '2b', r'58\.1\d': '2a', r'58\.[2-9]\D': '2a', r'58\.1\D': '2',
             r'59\.40': '2'})),
        'Documents d\'ecclesiologie ': lambda s: page_to_num(s, 'Documents', OrderedDict(
            {r'1': 4, r'20[0-6]': 4, r'208': 5, r'21': 5, r'230': 5, r'238': 6, r'250': 7})),
        'Eleousa: Acts': {r'2[5-7]': 'Eleousa: Acts'},
        'Esphigmenou': lambda s: page_to_num(s, 'Esphigmenou', {
            r'4[23]': 1, r'4[56]': 2, r'4[89]': 3, r'5[2-4]': 4, r'5[78]': 5}),
        'Eustathios Romaios': {r'Peira': 'Eustathios Romaios Peira',
                               r'Ralles-Potles V, \d{2}\D': 'Eustathios Romaios RPA',
                               r'Ralles-Potles V, \d{3}\D': 'Eustathios Romaios RPB',
                               r'Schminck ([IV]+)': r'Schminck \1'},
        'Gregory VII, in Caspar': {r'([IVX]+,\s?\d+)': r'\1'},
        'Iveron': {},
        'Keroularios': {r'ep. to Petros of Antioch (I+)', r'Keroularios \1'},
        'Lavra': {},
        'Leo IX': {r'[Ee]p. I? ? to (\w+)': r'to \1'},
        'Mauropous, Orations': {},
        'Mauropous, Letters': {},
        'Nea Mone,': {r'Gedeon': 'Nea Mone, Gedeon',
                      r'Miklosich-Müller 5.(\d)': r'Nea Mone, Miklosich-Müller \1'},
        'Panteleemon': {},
        'Protaton': lambda s: page_to_num(s, 'Protaton', OrderedDict(
            {r'225.[23]\d': '8a', r'22[4-9]': 8, r'23[0-2]': 8, r'23[6-8]': 9})),
        'Psellos': parse_psellos_ref,  # yeah, it needs its own
        'Vatopedi': {},
        'Xenophontos': {},
        'Xeropotamou': {}
    }
    source_id = pbw_id
    if pbw_id in aggregate_sources:
        # We have to look at the source ref to figure out which version of the source we are using
        aggregates = aggregate_sources[pbw_id]
        if isinstance(aggregates, dict):
            for rexp, sstr in aggregates.items():
                result = re.match(rexp, pbw_ref)
                if result:
                    if len(result.groups()):
                        source_id = "%s %s" % (pbw_id, sstr.replace('##', result.group(1)))
                    else:
                        source_id = "%s %s" % (pbw_id, sstr)
                    break
        else:
            for sstr in aggregates:
                if pbw_ref.startswith(sstr):
                    source_id = ' '.join([pbw_id, sstr]).rstrip() # to account for the null match
                    break
    return sourcelist.get(source_id)
