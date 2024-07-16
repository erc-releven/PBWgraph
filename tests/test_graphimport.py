import os
import unittest
from collections import Counter, defaultdict
from functools import reduce
from rdflib import Graph, RDF, Literal
from rdflib.exceptions import UniquenessError
from RELEVEN import PBWstarConstants, graphimportSTAR
from sqlalchemy.exc import DatabaseError
from tempfile import NamedTemporaryFile


def pburi(x):
    return f"https://pbw2016.kdl.kcl.ac.uk/person/{x}/"


def count_result(res):
    return reduce(lambda x, y: x + 1, res, 0)


class GraphImportTests(unittest.TestCase):
    graphdriver = None
    constants = None
    # Data keys are gender, identifier (appellation), second-name appellation, alternate-name appellation,
    # death, ethnicity, religion, societyrole, legalrole, language, kinship, possession
    td_people = {
        'Anna 62': {'gender': ['Female'], 'identifier': 'Ἄννα Κομνηνή',
                    'secondname': {'Κομνηνοῦ': 2},
                    'death': {'count': 1, 'dated': 0},
                    'religion': {'Christian': ['Georgios 25002']},
                    'legalrole': {'Basilis': 1, 'Basilissa': 1, 'Kaisarissa': 4,
                                  'Pansebastos sebaste': 1, 'Porphyrogennetos': 6},
                    'kinship': {'daughter': ['Alexios 1', 'Eirene 61'],
                                'sister': ['Ioannes 2', 'Maria 146'],
                                'wife': ['Nikephoros 117'],
                                'sister-in-law': ['Nikephoros 178'],
                                'niece': ['Ioannes 65', 'Isaakios 61', 'Michael 121'],
                                'aunt': ['Manuel 1'],
                                'daughter (eldest)': ['Eirene 61'],
                                'fiancée': ['Konstantinos 62'],
                                'granddaughter': ['Anna 61', 'Ioannes 63', 'Maria 62'],
                                'kin': ['Michael 7'],
                                'mother': ['Alexios 17005', 'Andronikos 118', 'Eirene 25003', 'Konstantinos 285',
                                           'Maria 171']}
                    },
        'Anna 64': {'gender': ['Female'], 'identifier': 'τῆς κουροπαλατίσσης Ἄννης',
                    'legalrole': {'Kouropalatissa': 1},
                    'kinship': {'grandmother': ['Anonymus 61'],
                                'mother': ['Ioannes 61', 'Nikephoros 62']}},
        'Anna 101': {'gender': ['Female'], 'identifier': 'Ἄννα',
                     'altname': {'Ἀρετῆς': 1}, 'legalrole': {'Nun': 1},
                     'kinship': {'daughter': ['Eudokia 1', 'Konstantinos 10']}},
        'Anna 102': {'gender': ['Female'], 'identifier': ' Ἄννῃ',
                     'death': {'count': 1, 'dated': 0}, 'legalrole': {'Nun': 1},
                     'kinship': {'wife': ['Eustathios 105'],
                                 'mother': ['Romanos 106']}},
        'Apospharios 101': {'gender': ['Male'], 'identifier': ' Ἀποσφάριον', 'legalrole': {'Slave': 1},
                            'kinship': {'husband': ['Selegno 101']}},
        'Bagrat 101': {'gender': ['Male'], 'identifier': 'τῷ Παγκρατίῳ بقراط بن جرجس',
                       'legalrole': {'King': 3, 'Kouropalates': 2, 'Sebastos': 1},
                       'kinship': {'son': ['Anonyma 6003', 'Georgios 105', 'Maria 103'],
                                   'husband': ['Helena 104'], 'father': ['Maria 61']}},
        'Balaleca 101': {'gender': ['Male'], 'identifier': 'Βαλαλεχα', 'language': 'Georgian'},
        'Gagik 101': {'gender': ['Male'], 'identifier': 'Κακίκιος',
                      'legalrole': {'Archon': 2, 'King': 1, 'Magistros': 1},
                      'kinship': {'son': ['Ashot 101'],
                                  'husband': ['Anonyma 158', 'Anonyma 159'],
                                  'son (in fact, nephew)': ['Ioannes 106']},
                      'possession': {'villages yielding a high income in Cappadocia, Charsianon and Lykandos':
                                         ['Ioannes 110', '437.28-29'],
                                     'Estates much poorer than Ani and its territory':
                                         ['Aristakes 101', '63.8-9 (55)']}},
        'Herve 101': {'gender': ['Male'], 'identifier': 'Ἐρβέβιον τὸν Φραγγόπωλον',
                      'secondname': {'Φραγγόπωλον': 2},
                      'ethnicity': {'Norman': 1},
                      'legalrole': {'Stratelates': 1, 'Vestes': 1, 'Magistros': 1},
                      'possession': {'House at Dagarabe in Armeniakon': ['Ioannes 110', '485.52']}},
        'Ioannes 62': {'gender': ['Male'], 'identifier': 'Ἰωάννης',
                       'secondname': {'Δούκα': 6}, 'altname': {'Ἰγνάτιος': 1},
                       'death': {'count': 1, 'dated': 0},
                       'legalrole': {'Stratarches': 1, 'Kaisar': 21, 'Basileopator': 1, 'Strategos autokrator': 2,
                                     'Basileus': 3, 'Monk': 4},
                       'kinship': {'brother': ['Konstantinos 10'],
                                   'husband': ['Eirene 20117'],
                                   'uncle': ['Andronikos 62', 'Konstantios 61', 'Michael 7'],
                                   'father': ['Andronikos 61', 'Konstantinos 61'],
                                   'father-in-law': ['Maria 62'],
                                   'grandfather': ['Eirene 61', 'Ioannes 65', 'Michael 121'],
                                   'nephew (son of brother)': ['Michael 7'],
                                   'relative by marriage': ['Nikephoros 101']
                                   },
                       'possession': {
                           'Palace in Bithynia at foot of Mount Sophon': ['Nikephoros 117', '173.7-8, 179.15']}},
        'Ioannes 68': {'gender': ['Eunuch'], 'identifier': 'τοῦ Ὀρφανοτρόφου',
                       'death': {'count': 4, 'dated': 1},
                       'legalrole': {'Praipositos': 1, 'Orphanotrophos': 12, 'Synkletikos': 1, 'Monk': 7},
                       'occupation': {'Beggar': 1, 'Servant': 1},
                       'kinship': {'brother': ['Georgios 106', 'Konstantinos 64', 'Maria 104', 'Michael 4',
                                               'Niketas 104', 'Stephanos 101'],
                                   'uncle': ['Michael 5'], 'uncle (maternal)': ['Michael 5'],
                                   'brother (first)': ['Michael 4'],
                                   'cousin of parent': ['Konstantinos 9101'],
                                   'kin': ['Konstantinos 9101']}
                       },
        'Ioannes 101': {'gender': ['Male'], 'identifier': 'Ἰωάννην',
                        'death': {'count': 1, 'dated': 0},
                        'legalrole': {'Archbishop': 3, 'Monk': 3}},
        'Ioannes 102': {'gender': ['Eunuch'], 'identifier': 'Ἰωάννην',
                        'legalrole': {'Bishop': 1, 'Metropolitan': 13, 'Protoproedros': 1, 'Hypertimos': 2,
                                      'Protoproedros of the protosynkelloi': 2, 'Protosynkellos': 2}},
        'Ioannes 110': {'gender': ['Male'], 'identifier': 'Ἰωάννου...τοῦ Σκυλίτζη',
                        'legalrole': {'Megas droungarios of the vigla': 1, 'Kouropalates': 1}},
        'Konstantinos 62': {'gender': ['Male'], 'identifier': 'Κωνσταντίνῳ',
                            'secondname': {'Δούκα': 1},
                            'death': {'count': 2, 'dated': 0},
                            'legalrole': {'Basileus': 3, 'Basileus (co-emperor)': 3, 'Porphyrogennetos': 5},
                            'kinship': {'son': ['Maria 61', 'Michael 7'],
                                        'fiancé': ['Anna 62'],
                                        'grandson': ['Konstantinos 10'],
                                        'husband (betrothed)': ['Helena 101'],
                                        'husband (proposed)': ['Helena 101'],
                                        'son (only)': ['Maria 61'],
                                        'son-in-law': ['Alexios 1', 'Eirene 61']},
                            'possession': {
                                'An estate, Pentegostis, near Serres, with excellent water and buildings to house the '
                                'imperial entourage': ['Eustathios 20147', '269.60-62']}},
        'Konstantinos 64': {'gender': ['Eunuch'], 'identifier': 'Κωνσταντῖνος', 'altname': {'Θεοδώρῳ': 1},
                            'death': {'count': 1, 'dated': 0},
                            'legalrole': {'Domestikos of the eastern tagmata': 1, 'Nobelissimos': 7, 'Praipositos': 1,
                                          'Domestikos of the scholai': 1, 'Proedros': 1, 'Vestarches': 1,
                                          'Domestikos': 4, 'Megas domestikos': 2, 'Doux': 4, 'Patrikios': 1, 'Monk': 2,
                                          'Domestikos of the scholai of Orient': 1,
                                          'Domestikos of the scholai of the East': 1},
                            'occupation': {'Beggar': 1},
                            'kinship': {'brother': ['Ioannes 68', 'Michael 4'], 'uncle': ['Michael 5']},
                            'possession': {
                                'Estates in Opsikion where he was banished by <Zoe 1>': ['Ioannes 110', '416.77'],
                                'A house with a cistern near the Holy Apostles (in Constantinople)': ['Ioannes 110',
                                                                                                      '422.18']}},
        'Konstantinos 101': {'gender': ['Male'], 'identifier': 'Κωνσταντῖνος ὁ Διογένης',
                             'secondname': {'Διογένης': 6},
                             'death': {'count': 2, 'dated': 0},
                             'legalrole': {'Doux': 4, 'Patrikios': 2, 'Strategos': 3, 'Archon': 1, 'Monk': 1},
                             'kinship': {'husband': ['Anonyma 108'],
                                         'father': ['Romanos 4'],
                                         'husband of niece': ['Romanos 3'],
                                         'nephew (by marriage)': ['Romanos 3']}},
        'Konstantinos 102': {'gender': ['Male'], 'identifier': 'Κωνσταντίνῳ',
                             'secondname': {'Βοδίνῳ': 3},
                             'altname': {'Πέτρον ἀντὶ Κωνσταντίνου μετονομάσαντες': 1},
                             'legalrole': {'King': 2, 'Basileus': 1},
                             'kinship': {'son': ['Michael 101'], 'father': ['Georgios 20253']}},
        'Konstantinos 110': {'gender': ['Male'], 'identifier': 'Κωνσταντῖνος',
                             'legalrole': {'Patrikios': 1},
                             'kinship': {'nephew': ['Michael 4']}},
        'Liparites 101': {'gender': ['Male'], 'identifier': 'τοῦ Λιπαρίτου قاريط ملك الابخاز',
                          'ethnicity': {'Georgian': 2}, 'legalrole': {'Lord of part of the Iberians': 1}}
    }

    td_boulloterions = {
        112: {'inscription': 'Konstantinos, proedros domestikos / of the scholai of the Orient and doux of Antioch',
              'seals': {2706: 'Vienna, private collection of Prof. Werner Seibt'}, 'sources': {
                'Seibt, BBÖ I': 'W. Seibt, Die byzantinischen Bleisiegel in Österreich I. Teil, Kaiserhof, Vienna 1978 [reviewed by V. Šandrovskaja and I.V.Sokolova in Byzantinoslavica 41 (1980), 251-255]',
                'Wassiliou - Seibt BBÖ II ': 'A.-K. Wassiliou - W. Seibt, Die byzantinischen Bleisiegel in Österreich, 2. Teil: Zentral- und Provinzialverwaltung, Vienna 2003'}},
        114: {'inscription': 'Lord aid your servant / Ignatios, monk and kaisar ',
              'seals': {187: 'Vienna, Kunsthistorisches Museum, Münzkabinett', 188: 'Cambridge, Mass., Fogg Art Museum',
                        189: 'Athens, Nomismatikon Mouseion, Main collection',
                        190: 'Private collection: Basel, G. Zacos (largely dispersed)', 191: 'St Petersburg, Hermitage',
                        8827: 'Unknown collection: details temporarily or permanently unavailable'}, 'sources': {
                'Seibt, BBÖ I': 'W. Seibt, Die byzantinischen Bleisiegel in Österreich I. Teil, Kaiserhof, Vienna 1978 [reviewed by V. Šandrovskaja and I.V.Sokolova in Byzantinoslavica 41 (1980), 251-255]',
                'Konstantopoulos, Nom. Mous.': 'K.M. Konstantopoulos, Byzantiaka molyvdoboulla tou en Athenais Ethnikou Nomismatikou Mouseiou, Athens 1917',
                'Laurent, Corpus V.2': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, V.2, L'Église, Paris 1965 [reviewed by V. Grumel in Byzantinische Zeitschrift 61 (1968), 129; W. Seibt in Byzantinoslavica 35 (1974), 73-84]",
                'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel',
                'Stavrakos': 'Ch. Stavrakos, Die byzantinischen Bleisiegel mit Familiennamen aus der Sammlung des Numismatischen Museums Athen, Wiesbaden 2000 [reviewed by Cl. Sode in Byzantinische Zeitschrift 95 (2002), 168-170 and J. Nesbitt in Speculum 77 (2002), 996-998]',
                'Iashvili - Seibt': 'I. Iashvili - W. Seibt, "Byzantinische Siegel aus Petra in Westgeorgien", Studies in Byzantine Sigillography 9, pp. 1-9'}},
        271: {'inscription': 'Mother of God. / Theotokos aid your servant Ioannes, monk and archbishop of all Bulgaria',
              'seals': {463: 'Washington, Dumbarton Oaks Research Library and Collection: 58 series',
                        464: 'Cambridge, Mass., Fogg Art Museum', 471: 'Sale Catalogue: Hirsch 186 (May, 1995)'},
              'sources': {
                  'Nesbitt - Oikonomides I': 'J. Nesbitt and N. Oikonomides, Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 1: Italy, North of the Balkans, North of the Black Sea, Washington D.C. 1991 [reviewed by W. Seibt in Byzantinische Zeitschrift 84/85 (1991), 548-5',
                  'Laurent, Corpus V.2': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, V.2, L'Église, Paris 1965 [reviewed by V. Grumel in Byzantinische Zeitschrift 61 (1968), 129; W. Seibt in Byzantinoslavica 35 (1974), 73-84]",
                  'Laurent, Corpus V.3': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, V.3, L'Église: Supplément, Paris 1972 [reviewed by W. Seibt in Byzantinoslavica 35 (1974), 73-84 and by N. Oikonomides in Speculum 49 (1974), 746-7]",
                  'Jordanov, Corpus I': 'I. Jordanov, Corpus of Byzantine Seals from Bulgaria, vol. 1: Byzantine Seals with Geographical Names, Sofia 2003 [reviewed by W. Seibt in Byzantinische Zeitschrift 98, 2005, pp. 129-133]'}},
        272: {
            'inscription': '[Mother of God]. / Theotokos aid your servant Ioannes, monk and archbishop of all Bulgaria',
            'seals': {465: 'Washington, Dumbarton Oaks Research Library and Collection: 55 series'}, 'sources': {
                'Nesbitt - Oikonomides I': 'J. Nesbitt and N. Oikonomides, Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 1: Italy, North of the Balkans, North of the Black Sea, Washington D.C. 1991 [reviewed by W. Seibt in Byzantinische Zeitschrift 84/85 (1991), 548-5'}},
        283: {'inscription': 'Lord aid your servant Konstantinos / Diogenes, patrikios and strategos of Serbia',
              'seals': {480: 'Cambridge, Mass., Fogg Art Museum'}, 'sources': {
                'Nesbitt - Oikonomides I': 'J. Nesbitt and N. Oikonomides, Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 1: Italy, North of the Balkans, North of the Black Sea, Washington D.C. 1991 [reviewed by W. Seibt in Byzantinische Zeitschrift 84/85 (1991), 548-5',
                'Laurent, Serbie': 'V. Laurent, "La thème byzantine de Serbie au XIe siècle", Revue des Études Byzantines 15, 1957'}},
        1406: {'inscription': 'St Nikolaos. / Lord aid your servant Niketas and man of the most felicitous kaisar',
               'seals': {5297: 'Sale Catalogue: Spink: October 6, 1999'}, 'sources': {
                'Zacos II': 'G. Zacos, Byzantine Lead Seals II, compiled and edited by J.W. Nesbitt, Bern 1984 [reviewed by H. Hunger in Jahrbuch der Ã–sterreichischen Byzantinistik 36 (1986), 333-339 and by N. Oikonomides, "A propos d\'une nouvelle publication de sceaux byzantins", Re'}},
        2216: {
            'inscription': 'Michael / See the seal of (Ioannes) protoproedros (of the protosynkelloi) metropolitan (protoproedros) of Side (metrical, with one word performing two functions)',
            'seals': {2607: 'Cambridge, Mass., Fogg Art Museum',
                      2608: 'Washington, Dumbarton Oaks Research Library and Collection: 58 series',
                      2609: 'Washington, Dumbarton Oaks Research Library and Collection: 55 series',
                      2610: 'Athens, Nomismatikon Mouseion, Main collection',
                      2611: 'Paris, Institut Français d’études byzantines',
                      2612: 'Paris, Institut Français d’études byzantines', 9968: 'Étampes, Thierry collection'},
            'sources': {
                'Nesbitt - Oikonomides II': 'J. Nesbitt and N. Oikonomides,  Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 2: South of the Balkans, the Islands, South of Asia Minor, Washington D.C. 1994 [reviewed by W. Seibt in Byzantinische Zeitschrift 90 (1997), 460-',
                'Laurent, Corpus V.1': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, V.1, L'Église, Paris 1963 [reviewed by V. Grumel in Byzantinische Zeitschrift 59 (1966), 392-396 and by W. Seibt in Byzantinoslavica 35 (1974), 73-84]",
                'Konstantopoulos, Nom. Mous.': 'K.M. Konstantopoulos, Byzantiaka molyvdoboulla tou en Athenais Ethnikou Nomismatikou Mouseiou, Athens 1917',
                'Laurent, Corpus V.3': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, V.3, L'Église: Supplément, Paris 1972 [reviewed by W. Seibt in Byzantinoslavica 35 (1974), 73-84 and by N. Oikonomides in Speculum 49 (1974), 746-7]"}},
        2217: {'inscription': 'Michael. / Seal of Ioannes, metropolitan (proedros) of Side and hypertimos (metrical)',
               'seals': {2613: 'Washington, Dumbarton Oaks Research Library and Collection: 58 series',
                         2614: 'Washington, Dumbarton Oaks Research Library and Collection: 58 series',
                         2615: 'Vienna, Kunsthistorisches Museum, Münzkabinett', 2616: 'St Petersburg, Hermitage'},
               'sources': {
                   'Nesbitt - Oikonomides II': 'J. Nesbitt and N. Oikonomides,  Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 2: South of the Balkans, the Islands, South of Asia Minor, Washington D.C. 1994 [reviewed by W. Seibt in Byzantinische Zeitschrift 90 (1997), 460-',
                   'Pančenko IRAIK 8': 'B.A. Pančenko, Kollekcii Russkago Archeologičeskago Instituta v Konstantinopolě. Katalog molivdovulov, Sofia 1908 (repr. from Izvestija Russkago Archeologičeskago Instituta v Konstantinopolě 8 (1903), 199-246)',
                   'Laurent, Corpus V.1': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, V.1, L'Église, Paris 1963 [reviewed by V. Grumel in Byzantinische Zeitschrift 59 (1966), 392-396 and by W. Seibt in Byzantinoslavica 35 (1974), 73-84]"}},
        2218: {'inscription': 'Michael. / Lord aid Ioannes, metropolitan of Side and protosynkellos',
               'seals': {2617: 'Washington, Dumbarton Oaks Research Library and Collection: 55 series'}, 'sources': {
                'Laurent, Corpus V.3': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, V.3, L'Église: Supplément, Paris 1972 [reviewed by W. Seibt in Byzantinoslavica 35 (1974), 73-84 and by N. Oikonomides in Speculum 49 (1974), 746-7]",
                'Nesbitt - Oikonomides II': 'J. Nesbitt and N. Oikonomides,  Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 2: South of the Balkans, the Islands, South of Asia Minor, Washington D.C. 1994 [reviewed by W. Seibt in Byzantinische Zeitschrift 90 (1997), 460-'}},
        2566: {
            'inscription': 'Michael the Archangel | the commander in chief. | St Demetrios. / Lord aid your servant Ioannes, nobelissimos protovestiarios and megas domestikos of the scholai of Orient',
            'seals': {2914: 'Washington, Dumbarton Oaks Research Library and Collection: 58 series'}, 'sources': {
                'Laurent, Corpus II': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, II, L'administration centrale, Paris 1981 [reviewed by J. Nesbitt in Speculum 58 (1983), 771-772, and by W. Seibt in Jahrbuch der Österreichischen Byzantinistik 26 (1977), 325]",
                'Nesbitt - Oikonomides III': 'J. Nesbitt and N. Oikonomides, Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 3: West, Northwest and Central Asia Minor and the Orient, Washington D.C. 1996 [reviewed by W. Seibt in Byzantinische Zeitschrift 92 (1999), 538-54',
                'Cheynet, Par St Georges': 'J.-Cl. Cheynet, Par St Georges, par St Michel, Travaux et Mémoires 14, 2002, pp. 114-134'}},
        2567: {
            'inscription': 'Archangel Michael of Chonai. / Lord aid Ioannes, nobelissimos protovestiarios and megas domestikos of the scholai of Orient',
            'seals': {2913: 'Washington, Dumbarton Oaks Research Library and Collection: 47 series'}, 'sources': {
                'Laurent, Corpus II': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, II, L'administration centrale, Paris 1981 [reviewed by J. Nesbitt in Speculum 58 (1983), 771-772, and by W. Seibt in Jahrbuch der Österreichischen Byzantinistik 26 (1977), 325]",
                'Nesbitt - Oikonomides III': 'J. Nesbitt and N. Oikonomides, Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 3: West, Northwest and Central Asia Minor and the Orient, Washington D.C. 1996 [reviewed by W. Seibt in Byzantinische Zeitschrift 92 (1999), 538-54'}},
        2625: {
            'inscription': 'Lord aid your servant Andronikos, protoproedros and / strategos of Thrakesion, man of the kaisar (Ioannes) Doukas',
            'seals': {2661: 'Washington, Dumbarton Oaks Research Library and Collection: 58 series'}, 'sources': {
                'Nesbitt - Oikonomides III': 'J. Nesbitt and N. Oikonomides, Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 3: West, Northwest and Central Asia Minor and the Orient, Washington D.C. 1996 [reviewed by W. Seibt in Byzantinische Zeitschrift 92 (1999), 538-54'}},
        2799: {
            'inscription': 'Lord aid your servant Konstantinos, patrikios / praipositos vestarches and domestikos of Orient',
            'seals': {2912: 'Cambridge, Mass., Fogg Art Museum'}, 'sources': {
                'Nesbitt - Oikonomides III': 'J. Nesbitt and N. Oikonomides, Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 3: West, Northwest and Central Asia Minor and the Orient, Washington D.C. 1996 [reviewed by W. Seibt in Byzantinische Zeitschrift 92 (1999), 538-54',
                'Wassiliou - Seibt BBÖ II ': 'A.-K. Wassiliou - W. Seibt, Die byzantinischen Bleisiegel in Österreich, 2. Teil: Zentral- und Provinzialverwaltung, Vienna 2003'}},
        2991: {
            'inscription': 'St Nikolaos. / 12 (or 2? Indiction no. ?) Lord aid your servant Ioannes monk and orphanotrophos (copper tessera)',
            'seals': {5401: 'Washington, Dumbarton Oaks Research Library and Collection: 58 series',
                      5402: 'Cambridge, Mass., Fogg Art Museum'},
            'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel',
                        'Nesbitt, Orphanotrophos': 'J. Nesbitt, "The orphanotrophos: some observations on the history of the office in the light of seals", Studies in Byzantine Sigillography 8 (2003), pp. 51-61'}},
        2992: {'inscription': 'St Nikolaos. / Lord aid your servant Ioannes monk and orphanotrophos',
               'seals': {5403: 'Private collection: Basel, G. Zacos (largely dispersed)',
                         8893: 'St Petersburg, Hermitage'},
               'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel',
                           'Schlumberger, Sigillographie': "G. Schlumberger, Sigillographie de l'empire byzantin, Paris 1884",
                           'Schlumberger, Mélanges': "G. Schlumberger, Mélanges d'archéologie byzantine, Paris 1895 [= extract from Revue des Études grecques 2 (1889), 245-59; 4 (1891), 111-42 and 7 (1894), 319-336]",
                           'Stepanova, St Nicholas': 'E. Stepanova, "The image of St Nicholas on Byzantine seals", Studies in Byzantine Sigillography 9 (2006), pp. 185-195'}},
        2993: {'inscription': 'St Nikolaos. / Lord aid your servant Ioannes monk and orphanotrophos',
               'seals': {5404: 'Private collection: Basel, G. Zacos (largely dispersed)',
                         5405: 'Private collection: Basel, G. Zacos (largely dispersed)',
                         5406: 'Private collection: Basel, G. Zacos (largely dispersed)',
                         5407: 'Private collection: Basel, G. Zacos (largely dispersed)',
                         5408: 'Private collection: Basel, G. Zacos (largely dispersed)'},
               'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel'}},
        2996: {'inscription': 'Lord aid your servant Konstantinos / the most illustrious nobelissimos',
               'seals': {5413: 'Private collection: Basel, G. Zacos (largely dispersed)'},
               'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel'}},
        4143: {'inscription': 'Through piety Anna Komnene’s seal does not bear holy pictures, but verses (metrical)',
               'seals': {6052: 'Mordtmann collection'},
               'sources': {'Mordtmann, Komnenon': 'A. Mordtmann, "Molybdoboulla ton Komnenon", EPhS 13, Suppl.',
                           'Schlumberger, Sigillographie': "G. Schlumberger, Sigillographie de l'empire byzantin, Paris 1884",
                           'Schlumberger, Inédits 5': 'G. Schlumberger, "Sceaux byzantins inédits (Cinquième série)", Revue Numismatique 9 (1905), 321-354, nos. 204-295. ',
                           'Laurent, Bulles métriques': 'V. Laurent, Les bulles métriques dans la sigillographie byzantine, Athens 1932 [repr. from Hellenika 4 (1931), 191-228 (nos. 1-110) and 321-360 (nos. 111-224); Hellenika 5 (1932), 137-174 (nos. 225-331) and 389-420 (nos. 331a-423); Hellenika 6 (1933), 81-'}},
        3000: {'inscription': 'Theotokos aid your servant / Ioannes the most fortunate kaisar',
               'seals': {6777: 'Private collection: Basel, G. Zacos (largely dispersed)',
                         6778: 'Private collection: Basel, G. Zacos (largely dispersed)',
                         6779: 'Private collection: Basel, G. Zacos (largely dispersed)',
                         6780: 'Private collection: Basel, G. Zacos (largely dispersed)',
                         6782: 'Paris, Bibliotheque nationale'},
               'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel'}},
        3001: {'inscription': 'Theotokos aid your servant / Ioannes the most fortunate kaisar',
               'seals': {6781: 'Private collection: Basel, G. Zacos (largely dispersed)'},
               'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel'}},
        3002: {'inscription': 'Theotokos aid your servant / Ioannes the most fortunate kaisar',
               'seals': {6783: 'Private collection: Basel, G. Zacos (largely dispersed)'},
               'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel'}},
        3003: {'inscription': 'Theotokos aid your servant / Ioannes the most fortunate kaisar',
               'seals': {6784: 'Private collection: Basel, G. Zacos (largely dispersed)'},
               'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel'}},
        3004: {'inscription': 'Lord aid your servant Ioannes / Doukas, basileopator',
               'seals': {6785: 'Private collection: Basel, G. Zacos (largely dispersed)',
                         6786: 'Private collection: Basel, G. Zacos (largely dispersed)',
                         9051: 'Khoury collection (largely purchased around Antioch and in Lebanon)'},
               'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel',
                           'Cheynet, Collection Khoury': 'J.-Cl. Cheynet, Sceaux de la collection Khoury, Revue Numismatique 159, 2003, 419-456'}},
        4941: {'inscription': 'St Petros. / (H)erbebios Phrangopoulos, magistros vestes stratelates of Orient',
               'seals': {7419: 'USA (private collection)'}, 'sources': {
                'Braunlin - Nesbitt, Selections': 'M. Braunlin and J. Nesbitt, "Selections from a private collection of Byzantine bullae", Byzantion 68 (1998), 157-182'}},
        5253: {'inscription': 'St Georgios. / Georgius, son of king Bodinus',
               'seals': {7860: 'Institute and Museum of Archaeology, Sofia', 7861: 'Bulgaria (private collection)'},
               'sources': {
                   'Jordanov, Corpus II': 'I. Jordanov, Corpus of Byzantine Seals from Bulgaria, vol. 2: Byzantine Seals with Family Names, Sofia 2006',
                   'Gerasimov, Georges': 'Gerasimov Th., "Un sceau en plomb de Georges, fils du roi Bodine". Studia Serdicensia 1, pp. 217-218',
                   'Jouroukova, Georgi Bodin': 'J. Jouroukova, "Nov oloven pečat na Georgi Bodin", Numizmatika 2, 8-13'}},
        6463: {'inscription': 'Lord aid your servant / Ignatios kaisar (?), monk',
               'seals': {9707: 'Washington, Dumbarton Oaks Research Library and Collection: 47 series'}, 'sources': {
                'Laurent, Corpus V.2': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, V.2, L'Église, Paris 1965 [reviewed by V. Grumel in Byzantinische Zeitschrift 61 (1968), 129; W. Seibt in Byzantinoslavica 35 (1974), 73-84]",
                'Seibt, review of Laurent, Corpus V ': 'W. Seibt, review of Laurent, Corpus V, Byzantinoslavica 35 (1974), 73-84',
                'Seibt, BBÖ I': 'W. Seibt, Die byzantinischen Bleisiegel in Österreich I. Teil, Kaiserhof, Vienna 1978 [reviewed by V. Šandrovskaja and I.V.Sokolova in Byzantinoslavica 41 (1980), 251-255]',
                'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel'}},
        6798: {'inscription': '[...] | Lord aid / Konstantinos Diogenes, patrikios and strategos',
               'seals': {10217: 'Regional Historical Museum, Shumen ', 10218: 'Istanbul Archaological Musum'},
               'sources': {
                   'Jordanov - Zhekova, Shumen': 'I. Jordanov - Z. Zhekova, Catalogue of Medieval Seals at the Regional Historical Museum of Shumen, Shumen 2007'}}}

    spot_sources = {
        # Source with authorship factoid
        # The Chronographia was created, according to Michael, based on a source passage therein
        # That creation was by Michael, according to Michael, based on that same source passage
        # The Chronographia has an edition, according to the editor, based on the edition itself
        # The edition has a whole bunch of passages, according to the PBW editor (no source given)
        'psellos_chronographia': {
            'work': 'Chronographia',
            'edition': 'Michel Psellos, Chronographie, 2 vols., Paris 1967',
            'author': 'Michael Psellos (named Konstantinos till tonsure in 1054)',
            'authority': 'Michael Psellos (named Konstantinos till tonsure in 1054)',
            'editor': 'Renauld, Émile',
            'pbwed': 'Whitby, Mary',
            'passages': 99
        },
        # Source with narrative factoid
        'praktikon_adam': {
            'work': 'Praktikon of Adam notary',
            'edition': 'Βυζαντινὰ ἔγγραφα τῆς μονῆς Πάτμου 1. Αὐτοκρατορικά, 2. Δημοσίων λειτουργῶν, Athens 1980, 2.7-20',
            'author': 'Adam, domestikos of the sekreton of the euageis oikoi',
            'authority': 'Papacostas, Tassos',
            'editor': 'Vranoúsīs, Léandros I.; Nystazopoúlou-Pelekídou, María',
            'pbwed': 'Papacostas, Tassos',
            'passages': 3
        },

        # Source with author but no factoid
        'kecharitomene_typikon': {
            'work': 'Kecharitomene typikon',
            'edition': '“Le typikon de la Théotokos Kécharitôménè", Revue des Études Byzantines, 43 (1985), 5-165',
            'author': 'Eirene Doukaina, wife of Alexios I',
            'authority': 'Gautier, Paul',
            'editor': 'Gautier, Paul',
            'pbwed': 'Jeffreys, Michael J.',
            'passages': 5
        },

        # Source with author outside of PBW
        'yahya': {
            'work': 'Ta’rikh Yahya ibn Said al-Antaki (The History of Yahya ibn Sa’id of Antioch)',
            'edition': 'Histoire de Yahya ibn Sa’id d’Antioche, Patrologia Orientalis 47.4 (no.212), Turnhout 1997',
            'author': 'Yaḥyā ibn Saʻīd al-Anṭākī',
            'authority': 'Kračkovskij, Ignati; Micheau, Françoise; Troupeau, Gérard',
            'editor': 'Kračkovskij, Ignati; Micheau, Françoise; Troupeau, Gérard',
            'pbwed': 'Papacostas, Tassos; Osti, Letizia; Munt, Harry',
            'passages': 5
        },

        # Source without author
        # There is no work creation recorded
        # The synodal decree has an edition, according to the editor, based on the edition itself
        # The edition has a passage, according to the PBW editor
        'synod_1094': {
            'work': 'Synodal decree of 1094',
            'edition': '“Le synode des Blachernes (fin 1094). Étude prosopographique”, Revue des Études Byzantines 29 (1971) 213-284',
            'editor': 'Gautier, Paul',
            'passages': 1,
            'pbwed': 'Papacostas, Tassos'
        },

        # Secondary source without primary work
        # There is no work recorded, nor work creation, nor R3 predicate
        # The expression was created, according to its authors, based on itself
        # The creation was by its authors, according to its authors, based on itself
        #
        'chris_philanth': {
            'edition': '“Commémoraisons des Comnènes dans le typikon liturgique du monastère du Christ Philanthrope (ms. Panaghia Kamariotissa 29)”, Revue des études Byzantines 63 (2005), 41-69',
            'editor': 'Rízou-Kouroúpou, Matoúla; Vannier, Jean-François',
            'passages': 14,
            'pbwed': 'Jeffreys, Michael J.'
        }

    }

    def setUp(self):
        # Make sure our triples file exists
        testfile = 'statements-test.ttl'
        if not os.path.isfile(testfile):
            gimport = graphimportSTAR.graphimportSTAR(origgraph=testfile, testmode=True)
            try:
                gimport.process_persons()
            except DatabaseError:
                self.fail("Cannot run tests without a MySQL connection or an existing triples file with test data.")
            self.constants = gimport.constants
        else:
            g = Graph()
            self.constants = PBWstarConstants.PBWstarConstants(g)

        c = self.constants
        # Get the URIs for each of our test people
        for p in self.td_people.keys():
            # Chain through to find the person from the ID
            try:
                e42 = c.graph.value(None, c.predicates['P190'], Literal(p), any=False)
                self.assertIsNotNone(e42)
                e15 = c.graph.value(None, c.predicates['P37'], e42, any=False)
                self.assertIsNotNone(e15)
                puri = c.graph.value(e15, c.predicates['P140'], any=False)
                self.assertIsNotNone(puri)
                self.td_people[p]['uri'] = puri.n3()
            except UniquenessError:
                self.fail("ID should lead to unique person")

    # TODO add extra assertions for the eunuchs and K62
    def test_gender(self):
        """Test that each person has the gender assignments we expect"""
        c = self.constants
        sparql = f"""
select ?p_uri ?gender where {{
    ?a1 a {c.get_assertion_for_predicate('P41')} ;
        {c.star_subject} ?ga ;
        {c.star_object} ?p_uri ;
        {c.star_auth} {c.pbw_agent.n3()} .
    ?a2 a {c.get_assertion_for_predicate('P42')} ;
        {c.star_subject} ?ga ;
        {c.star_object} [a {c.get_label('C11')} ; {c.get_label('P1')} ?gender ] ;
        {c.star_auth} {c.pbw_agent.n3()} .
}}"""
        res = c.graph.query(sparql)
        # Save the results for lookup
        genders = dict()
        for row in res:
            genders[row['p_uri']] = row['gender']
        # Check that they are correct
        for person, pinfo in self.td_people.items():
            p_uri = pinfo['uri']
            self.assertIsNotNone(genders.get(p_uri))
            self.assertEqual(genders[p_uri], pinfo['gender'], f"Test gender for {person}")

    # The identifier is the name as PBW has it in the original language.
    def test_identifier(self):
        """Test that each person has the main appellation given in the PBW database"""
        c = self.constants
        sparql = f"""
select ?p_uri ?mainid where {{
    ?a1 a {c.get_assertion_for_predicate('P1')} ;
        {c.star_subject} ?p_uri ;
        {c.star_object} [a {c.get_label('E41')} ; {c.get_label('P190')} ?mainid ] ;
        {c.star_auth} {c.pbw_agent.n3()} .
}}"""
        res = c.graph.query(sparql)
        # Save the results for lookup
        identifiers = dict()
        for row in res:
            identifiers[row['p_uri']] = row['mainid']
        # Check that they are correct
        for person, pinfo in self.td_people.items():
            p_uri = pinfo['uri']
            self.assertIsNotNone(identifiers.get(p_uri))
            self.assertEqual(pinfo['identifier'], identifiers['uri'], f"Test identifier for {person}")

    def test_appellation(self):
        """Test that each person has the second or alternative names assigned, as sourced assertions"""
        c = self.constants
        for person, pinfo in self.td_people.items():
            names = dict()
            if 'secondname' in pinfo:
                names.update(pinfo['secondname'])
            if 'altname' in pinfo:
                names.update(pinfo['altname'])
            if len(names) > 0:
                sparql = f"""
select ?appellation where {{
    ?a1 a {c.get_assertion_for_predicate('P1')} ;
        {c.star_subject} {pinfo['uri']} ;
        {c.star_object} [a {c.get_label('E41')} ; {c.get_label('P190')} ?appellation ] ;
        {c.star_based} ?src .
}}"""
                res = c.graph.query(sparql)
                found_appels = defaultdict(int)
                for row in res:
                    for appel in row['appellation']:
                        # Fortunately for us all the appellations are Greek
                        self.assertEqual(appel.language, 'grc')
                        found_appels[appel.toPython()] += 1
                self.assertDictEqual(names, found_appels)

    def test_death(self):
        """Test that each person has at most one death event, since they all only died once. Also
        test that the assertions look correct"""
        c = self.constants
        # Look for all death events and see who they are about
        deathevents = dict()
        sparql = f"""
select ?person ?de where {{
    ?a a {c.get_assertion_for_predicate('P100')} ;
        {c.star_subject} ?de ;
        {c.star_object} ?person .
}}"""
        res = c.graph.query(sparql)
        for row in res:
            person = row['person']
            de = row['de']
            self.assertIsNone(deathevents.get(person), f"{self.get_pbw_id(person)} should not die twice")
            deathevents[person] = de

        for person, pinfo in self.td_people.items():
            # Check if the person should have a death event.
            devent = deathevents.get(pinfo['uri'])
            ddescpred = c.get_assertion_for_predicate('P3')
            ddatepred = c.get_assertion_for_predicate('P4')
            if 'death' not in pinfo:
                # Make sure that the death was found
                self.assertIsNone(devent)
                continue
            else:
                self.assertIsNotNone(devent)
                # See if we have the expected info about the death event in question.
                # Each event should have N description assertions, each with a P3 attribute.
                sparql = f"""
select ?desc ?src where {{
    ?a a {ddescpred} ;
        {c.star_subject} {devent.n3()} ;
        {c.star_object} ?desc .
}}"""
                res = c.graph.query(sparql)
                self.assertEqual(pinfo['death']['count'], count_result(res), "Death description count for %s" % person)

                # and M date assertions.
                sparql = f"""
select ?a where {{
    ?a a {ddatepred} ;
        {c.star_subject} {devent.n3()} ;
        {c.star_object} [a {c.get_label('E52')}] .
}}"""
                res = c.graph.query(sparql)
                self.assertEqual(pinfo['death']['dated'], count_result(res), "Death date count for %s" % person)

                # Each event should also have N different sources across both sorts of assertion.
                sparql = f"""
select distinct ?sref where {{
    ?a {c.star_subject} {devent.n3()} ;
        {c.star_based} ?sref .
}}"""
                res = c.graph.query(sparql)
                self.assertEqual(pinfo['death']['count'], count_result(res))

    def test_ethnicity(self):
        """Test that the ethnicity was created correctly for our people with listed ethnicities"""
        c = self.constants
        for person, pinfo in self.td_people.items():
            # Find those with a declared ethnicity. This means a membership in a group of the given type.
            if 'ethnicity' in pinfo:
                eths = pinfo['ethnicity']
                sparql = f"""
select ?eth (count(?eth) as ?act) where {{
    ?a a {c.get_assertion_for_predicate('P107')} ;
        {c.star_subject} {pinfo['uri']} ;
        {c.star_object} [a {c.get_label('E74E')} ; {c.get_label('P1')} ?eth ] .
}}"""
                res = c.graph.query(sparql)
                rowct = 0
                for row in res:
                    rowct += 1
                    self.assertTrue(row['eth'] in eths)
                    self.assertEqual(eths[row['eth']], row['act'])
                self.assertEqual(len(eths.keys()), rowct, "Ethnicity count for %s" % person)

    def test_religion(self):
        """Test that our one religious affiliation was created correctly"""
        c = self.constants
        for person, pinfo in self.td_people.items():
            # Check that the religious assertions were created, and that they have the correct authority.
            if 'religion' in pinfo:
                rels = pinfo['religion']
                sparql = f"""
select ?rel ?auth where {{
    ?a a {c.get_assertion_for_predicate('SP36')} ;
        {c.star_subject} ?relaff ;
        {c.star_object} {pinfo['uri']} ;
        {c.star_auth} ?anode .
    ?a2 a {c.get_assertion_for_predicate('SP35')} ;
        {c.star_subject} ?relaff ;
        {c.star_object} [a {c.get_label('C24')} ; {c.get_label('P1')} ?rel ] .
    ?anode {c.get_label('P3')} ?auth .
}}"""
                res = c.graph.query(sparql)
                # We are cheating by knowing that no test person has more than one religion specified
                rows = [x for x in res]
                self.assertEqual(1, len(rows))
                self.assertTrue(rows[0]['rel'] in rels)
                self.assertIn('Georgios Tornikes', rows[0]['auth'])

    def test_occupation(self):
        """Test that occupations / non-legal designations are set correctly"""
        c = self.constants
        for person, pinfo in self.td_people.items():
            # Check that the occupation assertions were created
            if 'occupation' in pinfo:
                occs = pinfo['occupation']
                sparql = f"""
select ?occ where {{
    ?a a {c.get_assertion_for_predicate('SP13')} ;
        {c.star_subject} ?pocc ;
        {c.star_object} {pinfo['uri']} .
    ?a2 a {c.get_assertion_for_predicate('SP14')} ;
        {c.star_subject} ?pocc ;
        {c.star_object} [a {c.get_label('C7')} ; {c.get_label('P1')} ?rel ] .
    ?pocc a {c.get_label('C1')} .
}}"""
                res = c.graph.query(sparql)
                ctr = Counter([row['occ'].toPython() for row in res])
                self.assertDictEqual(occs, ctr, "Test occupations for %s" % person)

    def test_legalrole(self):
        """Test that legal designations are set correctly"""
        c = self.constants
        for person, pinfo in self.td_people.items():
            # Check that the occupation assertions were created
            if 'legalrole' in pinfo:
                roles = pinfo['legalrole']
                sparql = f"""
select ?role where {{
    ?a a {c.get_assertion_for_predicate('SP26')} ;
        {c.star_subject} ?prole ;
        {c.star_object} {pinfo['uri']} .
    ?a2 a {c.get_assertion_for_predicate('SP33')} ;
        {c.star_subject} ?prole ;
        {c.star_object} [a {c.get_label('C12')} ; {c.get_label('P1')} ?role ] .
    ?prole a {c.get_label('C13')} .
}}"""
                res = c.graph.query(sparql)
                ctr = Counter([row['occ'].toPython() for row in res])
                self.assertDictEqual(roles, ctr, "Test legal roles for %s" % person)

    def test_languageskill(self):
        """Test that our Georgian monk has his language skill set correctly"""
        c = self.constants
        for person, pinfo in self.td_people.items():
            # Find those with a language skill set
            if 'language' in pinfo:
                sparql = f"""
select ?kh where {{
    ?a a {c.get_assertion_for_predicate('SP38')} ;
        {c.star_subject} {pinfo['uri']} ;
        {c.star_object} ?lskill .
    ?a2 a {c.get_assertion_for_predicate('SP37')} ;
        {c.star_subject} ?lskill ;
        {c.star_object} [ a {c.get_label('C29')} ; {c.get_label('P1')} ?kh ] .
    ?lskill a {c.get_label('C21')} .
}}"""
                res = c.graph.query(sparql)
                rows = [x for x in res]
                # At the moment we do only have one
                self.assertEquals(1, len(rows))
                self.assertEqual(pinfo['language'], rows[0]['kh'].toPython(), "Test language for %s" % person)

    def test_kinship(self):
        """Test the kinship assertions for one of our well-connected people"""
        c = self.constants
        for person, pinfo in self.td_people.items():
            if 'kinship' in pinfo:
                sparql = f"""
select distinct ?kin ?kintype where {{
    ?a a {c.get_assertion_for_predicate('SP17')} ;
        {c.star_subject} ?kg ;
        {c.star_object} {pinfo['uri']} .
    ?a2 a {c.get_assertion_for_predicate('SP18')} ;
        {c.star_subject} ?kg ;
        {c.star_object} ?kin .
    ?a3 a {c.get_assertion_for_predicate('SP16')} ;
        {c.star_subject} ?kg ;
        {c.star_object} [ a {c.get_label('C4')} ; {c.get_label('P1')} ?kintype ] .
}}"""
                res = c.graph.query(sparql)
                foundkin = defaultdict(list)
                for row in res:
                    k = row['kintype'].toPython()
                    foundkin[k].append(row['kin'].toPython())
                for k in foundkin:
                    foundkin[k] = sorted(foundkin[k])
                self.assertDictEqual(pinfo['kinship'], foundkin, "Kinship links for %s" % person)

    def test_possession(self):
        """Check possession assertions. Test the sources and authors/authorities while we are at it."""
        c = self.constants
        for person, pinfo in self.td_people.items():
            # Find those who possess something. All the possessions are documented in written sources
            # whose authors are also in PBW; exploit this to test that the written sources were set up correctly.
            if 'possession' in pinfo:
                # a: person is object property of possession
                # idass: according to author, who is known by an identifier
                # a2: as we know from source extract, which belongs to the edition
                # a3: the edition belongs to a work
                # a4: the work belongs to a creation event
                # a5: the creation involves our author, who carried it out.
                sparql = f"""
select ?poss ?authorid ?src where {{
    ?a a {c.get_assertion_for_predicate('P51')} ;
        {c.star_subject} [a {c.get_label('E18')} ; {c.get_label('P1')} ?poss ] ;
        {c.star_object} {pinfo['uri']} ;
        {c.star_auth} ?author ;
        {c.star_based} ?srcuri .
    ?idass a {c.get_label('E15')} ;
        {c.star_subject} ?author ;
        {c.get_label('P37')} [ a {c.get_label('E42')} ; {c.get_label('P190')} ?authorid ] ;
        {c.star_auth} {c.pbw_agent.n3()} .
    ?a2 a {c.get_assertion_for_predicate('R15')} ;
        {c.star_subject} ?edition ;
        {c.star_object} ?srcuri .
    ?a3 a {c.get_assertion_for_predicate('R3')} ;
        {c.star_subject} ?work ;
        {c.star_object} ?edition .
    ?a4 a {c.get_assertion_for_predicate('R16')} ;
        {c.star_subject} ?creation ;
        {c.star_object} ?work .
    ?a5 a {c.get_assertion_for_predicate('P14')} ;
        {c.star_subject} ?creation ;
        {c.star_object} ?author .
    ?srcuri a {c.get_label('E33')} ;
        {c.get_label('P3')} ?src .
}}"""
                res = c.graph.query(sparql)
                rowct = 0
                for row in res:
                    rowct += 1
                    poss = row['poss']
                    author = row['id']
                    src = row['src']
                    self.assertTrue(poss in pinfo['possession'], "Test possession is correct for %s" % person)
                    (agent, reference) = pinfo['possession'][poss]
                    self.assertEqual(author, agent, "Test possession authority is set for %s" % person)
                    self.assertEqual(reference, src, "Test possession source ref is set for %s" % person)
                self.assertEqual(rowct, len(pinfo['possession'].keys()),
                                 "Test %s has the right number of possessions" % person)

    def test_boulloterions(self):
        """For each boulloterion, check that it exists only once and has only one inscription."""
        c = self.constants
        found = set()
        sparql = f"""
select ?boul ?inscr ?src ?auth where {{
    ?a a {c.get_assertion_for_predicate('P128')} ;
        {c.star_subject} ?boul ;
        {c.star_object} ?inscr ;
        {c.star_auth} ?auth ;
        {c.star_based} ?src .
}}"""
        res = c.graph.query(sparql)
        for row in res:
            # Check the types
            self.check_class(row['boul'], 'E22B')
            self.check_class(row['inscr'], 'E34')

            # The boulloterion should have a single descname that starts with 'Boulloterion'
            descname = self.get_object(row['boul'], 'P3')
            self.assertTrue(descname.startswith('Boulloterion of'))
            # Its identity should be a PBW URL with some boulloterion ID
            boulid = int(self.get_pbw_id(row['boul']))
            # We should be expecting this boulloterion
            self.assertIn(boulid, self.td_boulloterions, "Boulloterion %d should exist" % boulid)
            boulinfo = self.td_boulloterions[boulid]

            # but we should not have seen it yet
            self.assertNotIn(boulid, found, "Boulloterion %d should not be duplicated" % boulid)
            found.add(boulid)
            # The boulloterion should have a correct inscription
            inscr = self.get_object(row['inscr'], 'P190')
            self.assertEqual(boulinfo['inscription'], inscr)
            # The boulloterion should have the correct named authority
            auth = self.get_object(row['auth'], 'P3')
            self.assertEqual('Jeffreys, Michael J.', auth)
            # Separate query to check the boulloterion sources on the inscription assertion
            if len(boulinfo['sources']) > 1:
                # Source should be a Bibliography which contains a set of works
                self.check_class(row['src'], 'E73')
                # Get the texts of this bibliography
                sources = c.graph.objects(row['src'], c.entitylabels['P165'])
            else:
                # Source should be a single text
                self.check_class(row['src'], 'F2T')
                sources = [row['src']]
            for source in sources:
                self.assertIn(self.get_object(source, 'P1'), boulinfo['sources'])
                self.assertEqual(self.get_object(source, 'P102'), boulinfo['sources'][sid])

            # Separate query to check the boulloterion seals and their respective assertions. A seal was
            # produced by a boulloterion and belongs to a collection according to the same authority as above,
            # and these assertions have no explicit source.
            sealq = f"""
select ?seal ?coll where {{
    ?sealass a {c.get_assertion_for_predicate('L1')} ;
        {c.star_subject} {row['boul'].n3()} ;
        {c.star_object} ?seal ;
        {c.star_auth} ?auth .
    ?collass a {c.get_assertion_for_predicate('P46')} ;
        {c.star_subject} [ a {c.get_label('E78')} ; {c.get_label('P1')} ?coll ];
        {c.star_object} ?seal ;
        {c.star_auth} ?auth .
}}"""
            r3 = c.graph.query(sealq)
            sealcolls = dict()
            for row3 in r3:
                # Get the seal dict value and check its type
                self.check_class(row3['seal'], 'E22S')
                sealid = self.get_object(row3['seal'], 'P3')
                # Check that the seal hasn't been seen yet
                self.assertIsNone(sealcolls.get(sealid))
                # Add the seal and its collection
                collid = self.get_object(row3['coll'], 'P1')
                sealcolls[sealid] = collid
            # Check that we found all the seals
            self.assertDictEqual(boulinfo['seals'], sealcolls)

    def test_text_sources(self):
        """Spot-check different textual sources and make sure they are set up correctly"""
        c = self.constants
        p3 = c.get_label('P3')
        for s, sinfo in self.spot_sources.items():
            if 'author' in sinfo:
                # The Chronographia: work was created by author according to author based on passage
                # The typikon: work was created by author according to editor
                # The praktikon: work was created by author according to PBW editor
                # Yahya: work was created by author according to editor based on *edition*
                # All: work has edition according to editor based on edition
                motivation = c.get_label('F3') if s == 'yahya' else c.get_label('E33')
                q = 'MATCH (work:%s)<-[:%s]-(wc1:%s)-[:%s]->(wc:%s)<-[:%s]-(wc2:%s)-[:%s]->(author:%s), ' \
                    '(wc1)-[:%s]->(authority), (wc2)-[:%s]->(authority), ' \
                    '(wc1)-[:%s]->(passage:%s), (wc2)-[:%s]->(passage), ' \
                    '(work)<-[:%s]-(wed:%s)-[:%s]->(edition:%s), (wed)-[:%s]->(editor), (wed)-[:%s]->(edition) ' \
                    'WHERE work.%s = "%s" ' \
                    'RETURN work.%s as work, author.%s as author, authority.%s as authority, editor.%s as editor, ' \
                    'edition.%s as edition, passage.%s as passage' % (
                        c.get_label('F1'), c.star_object, c.get_assertion_for_predicate('R16'), c.star_subject,
                        c.get_label('F27'), c.star_subject, c.get_assertion_for_predicate('P14'), c.star_object,
                        c.get_label('E21'), c.star_auth, c.star_auth, c.star_source, motivation, c.star_source,
                        c.star_subject, c.get_assertion_for_predicate('R3'), c.star_object, c.get_label('F3'),
                        c.star_auth, c.star_source, p3, sinfo.get('work'), p3, p3, p3, p3, p3, p3
                    )
            elif 'work' in sinfo:
                # The 1094 synod: work has edition according to editor based on edition
                q = 'MATCH (work:%s)<-[:%s]-(wed:%s)-[:%s]->(edition:%s), ' \
                    '(wed)-[:%s]->(editor), (wed)-[:%s]->(edition) ' \
                    'WHERE work.%s = "%s" ' \
                    'RETURN work.%s as work, editor.%s as editor, edition.%s as edition' % (
                        c.get_label('F1'), c.star_subject, c.get_assertion_for_predicate('R3'), c.star_object,
                        c.get_label('F3'), c.star_auth, c.star_source,
                        p3, sinfo.get('work'),
                        p3, p3, p3
                    )
            else:
                # The Christos Philanthropos doc: edition was created by editors according to editors based on edition.
                # We have to match the group of editors.
                q = 'MATCH (edition:%s)<-[:%s]-(ec1:%s)-[:%s]->(ec:%s)<-[:%s]-(ec2:%s)-[:%s]->(editor), ' \
                    '(ec1)-[:%s]->(editor), (ec2)-[:%s]->(editor), ' \
                    '(ec1)-[:%s]->(edition), (ec2)-[:%s]->(edition) ' \
                    'WHERE edition.%s = "%s" ' \
                    'RETURN editor.%s as editor, edition.%s as edition' % (
                        c.get_label('F3'), c.star_object, c.get_assertion_for_predicate('R17'), c.star_subject,
                        c.get_label('F28'), c.star_subject, c.get_assertion_for_predicate('P14'), c.star_object,
                        c.star_auth, c.star_auth, c.star_source, c.star_source,
                        p3, sinfo.get('edition'), p3, p3)
            with self.graphdriver.session() as session:
                # There should only be one result
                result = session.run(q).single(strict=True)
                if 'author' in sinfo:
                    self.assertEqual(sinfo.get('author'), result['author'])
                if 'authority' in sinfo:
                    self.assertEqual(sinfo.get('authority'), result['authority'])
                if 'work' in sinfo:
                    self.assertEqual(sinfo.get('work'), result['work'])
                self.assertEqual(sinfo.get('edition'), result['edition'])
                self.assertEqual(sinfo.get('editor'), result['editor'])

                # Now check that the passages are present & correct and have the right authority
                pq = 'MATCH (edition:%s {%s:\'%s\'})<-[%s]-(psa:%s)-[:%s]->(passage:%s), ' \
                     '(psa)-[:%s]->(pbwed) RETURN passage.%s as passage, pbwed.%s as pbwed' % (
                         c.get_label('F3'), p3, sinfo.get('edition'), c.star_subject,
                         c.get_assertion_for_predicate('R15'),
                         c.star_object, c.get_label('E33'), c.star_auth, p3, p3
                     )
                passages = session.run(pq)
                ct = 0
                for row in passages:
                    ct += 1
                    self.assertEqual(row['pbwed'], sinfo.get('pbwed'))
                self.assertEqual(ct, sinfo.get('passages'))

    def test_db_entry(self):
        """All the assertions in the database should be attached to DB records, linked to the single entry
        that created them."""
        c = self.constants
        p70 = c.get_label('P70')  # the documents predicate
        f2 = c.get_label('F2')  # the DB record per assertion
        r17 = c.get_label('R17')  # linking the record to its creation
        f28 = c.get_label('F28')  # the data creation record
        p4 = c.get_label('P4')  # created at
        e52 = c.get_label('E52')  # a particular time
        p80 = c.get_label('P80')  # with this timestamp
        p14 = c.get_label('P14')  # carried out by...
        e21 = c.get_label('E21')  # ...me.
        totalq = "MATCH (a) WHERE ANY (l IN labels(a) WHERE l =~ 'star__E13_.*') RETURN COUNT(a) AS numass"
        linkedq = "MATCH (a)<-[:%s]-(record:%s)<-[:%s]-(dbevent:%s)-[:%s]->(me:%s)," \
                  "(dbevent)-[:%s]->(tstamp:%s) " \
                  "RETURN count(a) as numass, count(record) as numrec, tstamp.%s as tstamp, me" % (
                      p70, f2, r17, f28, p14, e21, p4, e52, p80)
        with self.graphdriver.session() as session:
            total = session.run(totalq).single()['numass']
            linked = session.run(linkedq).single(strict=True)
            self.assertEqual(total, linked['numass'])
            self.assertEqual(total, linked['numrec'])
            self.assertIsNotNone(linked['tstamp'])
            self.assertEqual('Andrews, Tara Lee', linked['me'].get(c.get_label('P3')))

    def testRepeat(self):
        """If we have a DB connection and re-run the import, there should be zero new assertions
        and the graph should not change."""
        # See how many triples are in the graph
        c = self.constants
        graph_size = len(c.graph)
        # Write out the graph as is
        with NamedTemporaryFile(delete=False) as tf:
            tf_name = tf.name
            c.graph.serialize(tf)
        # Regenerate the graph. If we don't have a database connection, we end the test.
        gimport = graphimportSTAR.graphimportSTAR(origgraph=tf_name, testmode=True)
        try:
            gimport.process_persons()
        except DatabaseError:
            self.skipTest("Cannot run repetition test without a MySQL connection.")
        # Check the length of the resulting graph
        self.assertEqual(graph_size, len(gimport.g), "Regeneration of graph results in no change to size of graph")
        # Check that the triples are all identical
        for triple in c.graph:
            self.assertTrue(triple in gimport.g, f"Triple {triple} exists in both graphs")

    def check_class(self, uri, ocl):
        """Helper to check that a URI is defined as the given class"""
        c = self.constants
        self.assertTrue((uri, RDF.type, c.entitylabels.get(ocl, c.predicates.get(ocl))) in c.graph,
                        f"Class of {uri} should be {ocl}")

    def get_pbw_id(self, uri):
        """Return the E15 identifier set by PBW for the given entity."""
        # Find the E15
        c = self.constants
        e15s = [x for x in c.graph.subjects(c.predicates['P140'], uri)
                if c.graph.value(x, RDF.type) == c.entitylabels['E15']]
        self.assertEqual(1, len(e15s), f"There should be one E15 assertion for {uri}")
        # Chain down the E15 to find the identifier value
        e42 = c.graph.value(e15s[0], c.predicates['P37'], any=False)
        self.assertIsNotNone(e42, "Identifier should exist")
        pbwid = c.graph.value(e42, c.predicates['P190'], any=False)
        self.assertIsNotNone(pbwid, "Identifier content should exist")
        return pbwid

    def get_object(self, subj, pred):
        c = self.constants
        try:
            obj = c.graph.value(subj, c.predicates[pred], any=False)
        except UniquenessError:
            self.fail(f"Object of {subj} : {pred} should be unique")
        self.assertIsNotNone(obj, f"Object of {subj} : {pred} should exist")
        return obj.toPython()

if __name__ == '__main__':
    unittest.main()
