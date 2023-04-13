import csv
import re
from warnings import warn


# Static functions for the import
def add_if_exists(d, key, datum):
    if datum is not None and not (re.match(r'^\s*$', datum) or datum == 'n/a'):
        d[key] = datum


def add_one_or_more(d, key, datum):
    if datum is None or re.match(r'^\s*$', datum) or datum == 'n/a':
        return
    d[key] = re.split(r';\s*', datum)


def add_editors(d, source_id, editor_names, editor_viafs):
    """Add the editors of the source edition as a list of {identifier: 'name', viaf: 'viafcode'}"""
    namelist = re.split(r';\s*', editor_names)
    viaflist = re.split(r';\s*', editor_viafs)
    editors = []
    if len(namelist) != len(viaflist):
        raise ImportError(f"Editor list mismatch on line {source_id}")
    for i, name in enumerate(namelist):
        editors.append({'identifier': name, 'viaf': viaflist[i]})
    d['editor'] = editors


def add_pbw_authorities(d, authority_string, pbw_authorities):
    pbwauths = re.split(r';\s*', authority_string)
    d['authority'] = [pbw_authorities[x] for x in pbwauths]


def add_authors(d, source_id, author_string):
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
        d['author'] = authors


def add_provenance(d, source_id, prov_string):
    """We have to check whether it is a factoid number, or the text of a factoid, or something else entirely"""
    # Do we have the factoid ID?
    factoid_id = re.match(r'factoid (\d+)', prov_string)
    if factoid_id is not None:
        d['factoid'] = factoid_id.group(1)
    else:
        # This didn't match a factoid. Put it in as a provenance
        warn(f"No factoid key found for provenance of source {source_id}")
        d['provenance'] = prov_string


def make_refrange(refrange):
    match_set = set()
    for m in re.split(r',\s*', refrange):
        mprefix = None
        mparts = m.split('.')
        if len(mparts) > 1:
            mprefix = '.'.join(mparts[:-1])
        mspan = [int(x) for x in mparts[-1].split('-')]
        mrange = range(mspan[0], mspan[-1]+1)
        if mprefix:
            match_set.update([f"{mprefix}.{x}" for x in mrange])
        else:
            match_set.update(mrange)
    return match_set


# Functions to lambdafy for the page range parsing

def ref_substring(refstring, prefix, possible):
    for p in possible:
        source_id = ' '.join([prefix, possible]).rstrip()
        if refstring.startswith(p):
            return source_id
    return None


def page_to_key(refstring, prefix, sourcelist, strip=None):
    """Given a refstring, the common prefix, and a dictionary of items and their page ranges,
    return which item the refstring is pointing to."""
    # Get the list of ranges from the source list
    ranges = {sourcelist[x].get('range'): x for x in sourcelist.keys()
              if x.startswith(prefix) and 'range' in sourcelist[x]}
    # Strip off the beginning of the ref string if necessary
    matchstring = refstring
    if strip:
        matchstring = refstring.replace(strip, '').lstrip()
    for r, k in ranges.items():
        if matchstring.startswith(r):
            return k


def appended_string(refstring, prefix, sourcelist, strip=None):
    components = [x for x in sourcelist.keys() if x.startswith(prefix)]
    for key in components:
        mstr = key.replace(prefix, '').lstrip()
        matchstring = refstring
        if strip:
            matchstring = refstring.replace(strip, '').lstrip()
        if matchstring.startswith(mstr):
            return key


# Some of the sources are special
def parse_psellos_ref(refstring, sourcelist):
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
        return page_to_key(refstring, 'Psellos, Monodies (Gautier)', sourcelist, strip='Monodies (Gautier)')
    # Oratoria minora
    if refstring.startswith('Oratoria minora'):
        return page_to_key(refstring, 'Psellos, Oratoria minora', sourcelist, strip='Oratoria minora')

    # Letters
    m = re.match(r'Letters \(([\w -]+)\) (\d+)', refstring)
    if m:
        return f'Psellos {m.group(1)} {m.group(2)}'
    return None


def parse_romaios(refstring):
    (tocheck, pages) = refstring.split(', ', 1)
    answer = 'Eustathios Romaios '
    for p in ['Peira', 'Schminck']:
        if tocheck.startswith(p):
            return answer + p
    if tocheck == 'Ralles-Potles V':
        return answer + ('RPB' if re.match(r'\d{3}', pages) else 'RPA')
    return None


def parse_neamone(refstring, sourcelist):
    if refstring.startswith('Gedeon'):
        return 'Nea Mone, Gedeon'
    else:
        kstr = 'Nea Mone, Miklosich-Müller'
        return page_to_key(refstring, kstr, {x: sourcelist[x] for x in sourcelist.keys() if x.startswith(kstr)},
                           strip="Miklosich-Müller 5.")


# Make a persistent class to hold the data
class PBWSources:
    def __init__(self, csvfile):
        """Given a CSV file of sources in the expected format, turn it into a data structure recognisable
        by PBWStarConstants"""
        # This will contain the information parsed from the CSV file
        self.sourcelist = dict()
        # These are the sources that need something stripped from a reference string.
        self.stripped = {
            'Alexios Stoudites': ['Eleutherios', 'Ralles-Potles', 'VV', ''],
            'Eustathios Romaios': ['Peira', 'Ralles-Potles V', r'Schminck I*'],
            'Keroularios': ['ep. to Petros of Antioch'],
            'Nea Mone,': ['Gedeon', 'Miklosich-Müller 5.'],
            'Psellos': [r'Actum 2', r'Against Ophrydas', r'Andronikos', r'Apologetikos', r'De omnifari doctrina',
                        r'Eirene', r'Epiphanios', r'Hypomnema', r'Kategoria', r'Keroularios', r'Leichoudes',
                        r'Malik-shah', r'Mother', r'Nikolaos of Horaia Pege', r'Niketas Maïstor',
                        r'Philosophica minora I', r'Robert', r'Styliane', r'Xiphilinos', r'Monodies \(Gautier\)',
                        r'(Orationes panegyricae [IVX]+)', r'(Poema \d+)\.', 'Oratoria minora',
                        r'Letters \(([\w -]+)\) (\d+)']
        }
        # These are our composite sources; we will have to store a function for each which takes a
        # reference string and returns the right source part.
        self.composites = {
            'Alexios Stoudites': lambda x: ref_substring(x, 'Alexios Stoudites', self.stripped['Alexios Stoudites']),
            'Docheiariou': lambda x: page_to_key(x, 'Docheiariou', self.sourcelist),
            'Documents d\'ecclesiologie': lambda x: page_to_key(x, 'Documents', self.sourcelist),
            'Esphigmenou': lambda x: page_to_key(x, 'Esphigmenou', self.sourcelist),
            'Eustathios Romaios': parse_romaios,
            'Gregory VII, in Caspar': lambda x: appended_string(x, 'Gregory VII, in Caspar', self.sourcelist),
            'Iveron': lambda x: page_to_key(x, 'Iveron', self.sourcelist),
            'Keroularios': lambda x: appended_string(x, 'Keroularios', self.sourcelist,
                                                     strip=self.stripped['Keroularios'][0]),
            'Lavra': lambda x: page_to_key(x, 'Lavra', self.sourcelist),
            'Mauropous: Orations': lambda x: page_to_key(x, 'Mauropous: Orations', self.sourcelist),
            'Mauropous: Letters': lambda x: page_to_key(x, 'Mauropous: Letters', self.sourcelist),
            'Nea Mone,': lambda x: parse_neamone(x, self.sourcelist),
            'Panteleemon': lambda x: page_to_key(x, 'Panteleemon', self.sourcelist),
            'Patmos: Acts': lambda x: page_to_key(x, 'Patmos: Acts', self.sourcelist),
            'Protaton': lambda x: page_to_key(x, 'Protaton', self.sourcelist),
            'Psellos': parse_psellos_ref,
            'Theophylaktos of Ohrid, Letters': lambda x: page_to_key(x, 'Theophylaktos of Ohrid, Letters',
                                                                     self.sourcelist),
            'Vatopedi': lambda x: page_to_key(x, 'Vatopedi', self.sourcelist),
            'Xenophontos': lambda x: page_to_key(x, 'Xenophontos', self.sourcelist),
            'Xeropotamou': lambda x: page_to_key(x, 'Xeropotamou', self.sourcelist)
        }

        # These are the modern scholars who put the source information into PBW records
        self.authorities = dict()
        self.authorities['mj'] = {'identifier': 'Jeffreys, Michael J.', 'viaf': '73866641'}
        self.authorities['tp'] = {'identifier': 'Papacostas, Tassos', 'viaf': '316793603'}
        self.authorities['ta'] = {'identifier': 'Andrews, Tara Lee', 'viaf': '316505144'}
        self.authorities['jr'] = {'identifier': 'Ryder, Judith R.', 'viaf': '51999624'}
        self.authorities['mw'] = {'identifier': 'Whitby, Mary', 'viaf': '34477027'}
        self.authorities['wa'] = {'identifier': 'Amin, Wahid M.', 'viaf': '213149617100303751518'}
        self.authorities['bs'] = {'identifier': 'Soravia, Bruna', 'viaf': '69252167'}
        self.authorities['hm'] = {'identifier': 'Munt, Harry', 'viaf': '78568758'}
        self.authorities['lo'] = {'identifier': 'Osti, Letizia', 'viaf': '236145542536996640148'}
        self.authorities['cr'] = {'identifier': 'Roueché, Charlotte', 'viaf': '44335536'}
        self.authorities['ok'] = {'identifier': 'Karágiṓrgou, ́Olga', 'viaf': '253347413'}

        with open(csvfile, encoding='utf-8', newline='') as fh:
            reader = csv.DictReader(fh, delimiter=';')
            for row in reader:
                # Make an object for the source information
                source_id = row['PBW Source ID']
                source_data = dict()
                add_authors(source_data, source_id, row['Author(ity)'])
                add_provenance(source_data, source_id, row['Evidence of authorship'])
                add_pbw_authorities(source_data, row['PBW editor'], self.authorities)
                add_if_exists(source_data, 'work', row['Source canonical name'])
                add_if_exists(source_data, 'expression', row['Source edition used'])
                add_if_exists(source_data, 'url', row['Edition URL'])
                if row['Page range'] is not None:
                    source_data['range'] = make_refrange(row['Page range'])
                self.sourcelist[source_id] = source_data

    def key_for(self, source, refstring):
        if source in self.sourcelist:
            return source
        if source in self.composites:
            return self.composites[source](refstring)

    def get(self, source):
        return self.sourcelist.get(source)

    def sourceref(self, source, refstring):
        """Return the source reference, modified to account for our aggregate sources."""
        if source in self.stripped:
            # We have to look at the source ref to figure out which version of the source we are using
            for expr in self.stripped[source]:
                refstring = re.sub(expr, '', refstring).lstrip()
        return refstring
