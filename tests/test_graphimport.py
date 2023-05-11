import unittest
import RELEVEN.PBWstarConstants
import config
from collections import defaultdict
from neo4j import GraphDatabase


def pburi(x):
    return f"https://pbw2016.kdl.kcl.ac.uk/person/{x}"


def pbwid(x):
    return x.lstrip('https://pbw2016.kdl.kcl.ac.uk/person/')


class GraphImportTests(unittest.TestCase):

    graphdriver = None
    constants = None
    # Data keys are gender, identifier (appellation), second-name appellation, alternate-name appellation,
    # death, ethnicity, religion, societyrole, legalrole, language, kinship, possession
    td_people = {
        'Anna/62': {'gender': ['Female'], 'identifier': 'Ἄννα Κομνηνή',
                    'secondname': {'Κομνηνοῦ': 2},
                    'death': {'count': 1, 'dated': 0},
                    'religion': {'Christian': ['Georgios 25002']},
                    'legalrole': {'Basilis': 1, 'Basilissa': 1, 'Kaisarissa': 4,
                                  'Pansebastos sebaste': 1, 'Porphyrogennetos': 6},
                    'kinship': {'daughter': ['Alexios/1', 'Eirene/61'],
                                'sister': ['Ioannes/2', 'Maria/146'],
                                'wife': ['Nikephoros/117'],
                                'sister-in-law': ['Nikephoros/178'],
                                'niece': ['Ioannes/65', 'Isaakios/61', 'Michael/121'],
                                'aunt': ['Manuel/1'],
                                'daughter (eldest)': ['Eirene/61'],
                                'fiancée': ['Konstantinos/62'],
                                'granddaughter': ['Anna/61', 'Ioannes/63', 'Maria/62'],
                                'kin': ['Michael/7'],
                                'mother': ['Alexios/17005', 'Andronikos/118', 'Eirene/25003', 'Konstantinos/285',
                                           'Maria/171']}
                    },
        'Anna/64': {'gender': ['Female'], 'identifier': 'τῆς κουροπαλατίσσης Ἄννης',
                    'legalrole': {'Kouropalatissa': 1},
                    'kinship': {'grandmother': ['Anonymus/61'],
                                'mother': ['Ioannes/61', 'Nikephoros/62']}},
        'Anna/101': {'gender': ['Female'], 'identifier': 'Ἄννα',
                     'altname': {'Ἀρετῆς': 1}, 'legalrole': {'Nun': 1},
                     'kinship': {'daughter': ['Eudokia/1', 'Konstantinos/10']}},
        'Anna/102': {'gender': ['Female'], 'identifier': ' Ἄννῃ',
                     'death': {'count': 1, 'dated': 0}, 'legalrole': {'Nun': 1},
                     'kinship': {'wife': ['Eustathios/105'],
                                 'mother': ['Romanos/106']}},
        'Apospharios/101': {'gender': ['Male'], 'identifier': ' Ἀποσφάριον', 'legalrole': {'Slave': 1},
                            'kinship': {'husband': ['Selegno/101']}},
        'Balaleca/101': {'gender': ['Male'], 'identifier': 'Βαλαλεχα', 'language': 'Georgian'},
        'Gagik/101': {'gender': ['Male'], 'identifier': 'Κακίκιος',
                      'legalrole': {'Archon': 2, 'King': 1, 'Magistros': 1},
                      'kinship': {'son': ['Ashot/101'],
                                  'husband': ['Anonyma/158', 'Anonyma/159'],
                                  'son (in fact, nephew)': ['Ioannes/106']},
                      'possession': {'villages yielding a high income in Cappadocia, Charsianon and Lykandos':
                                         ['Ioannes/110', '437.28-29'],
                                     'Estates much poorer than Ani and its territory':
                                         ['Aristakes/101', '63.8-9 (55)']}},
        'Herve/101': {'gender': ['Male'], 'identifier': 'Ἐρβέβιον τὸν Φραγγόπωλον',
                      'secondname': {'Φραγγόπωλον': 2},
                      'ethnicity': {'Norman': 1},
                      'legalrole': {'Stratelates': 1, 'Vestes': 1, 'Magistros': 1},
                      'possession': {'House at Dagarabe in Armeniakon': ['Ioannes/110', '485.52']}},
        'Ioannes/62': {'gender': ['Male'], 'identifier': 'Ἰωάννης',
                       'secondname': {'Δούκα': 6}, 'altname': {'Ἰγνάτιος': 1},
                       'death': {'count': 1, 'dated': 0},
                       'legalrole': {'Stratarches': 1, 'Kaisar': 21, 'Basileopator': 1, 'Strategos autokrator': 2,
                                     'Basileus': 3, 'Monk': 4},
                       'kinship': {'brother': ['Konstantinos/10'],
                                   'husband': ['Eirene/20117'],
                                   'uncle': ['Andronikos/62', 'Konstantios/61', 'Michael/7'],
                                   'father': ['Andronikos/61', 'Konstantinos/61'],
                                   'father-in-law': ['Maria/62'],
                                   'grandfather': ['Eirene/61', 'Ioannes/65', 'Michael/121'],
                                   'nephew (son of brother)': ['Michael/7'],
                                   'relative by marriage': ['Nikephoros/101']
                                   },
                       'possession': {'Palace in Bithynia at foot of Mount Sophon': ['Nikephoros/117', '173.7-8, 179.15']}},
        'Ioannes/68': {'gender': ['Eunuch'], 'identifier': 'τοῦ Ὀρφανοτρόφου',
                       'death': {'count': 4, 'dated': 1},
                       'legalrole': {'Praipositos': 1, 'Orphanotrophos': 12, 'Synkletikos': 1, 'Monk': 7},
                       'occupation': {'Beggar': 1, 'Servant': 1},
                       'kinship': {'brother': ['Georgios/106', 'Konstantinos/64', 'Maria/104', 'Michael/4',
                                               'Niketas/104', 'Stephanos/101'],
                                   'uncle': ['Michael/5'], 'uncle (maternal)': ['Michael/5'],
                                   'brother (first)': ['Michael/4'],
                                   'cousin of parent': ['Konstantinos/9101'],
                                   'kin': ['Konstantinos/9101']}
                       },
        'Ioannes/101': {'gender': ['Male'], 'identifier': 'Ἰωάννην',
                        'death': {'count': 1, 'dated': 0},
                        'legalrole': {'Archbishop': 3, 'Monk': 3}},
        'Ioannes/102': {'gender': ['Eunuch'], 'identifier': 'Ἰωάννην',
                        'legalrole': {'Bishop': 1, 'Metropolitan': 12, 'Protoproedros': 1, 'Hypertimos': 2,
                                      'Protoproedros of the protosynkelloi': 2, 'Protosynkellos': 2}},
        'Ioannes/110': {'gender': ['Male'], 'identifier': 'Ἰωάννου...τοῦ Σκυλίτζη',
                        'legalrole': {'Megas droungarios of the vigla': 1, 'Kouropalates': 1}},
        'Konstantinos/62': {'gender': ['Male'], 'identifier': 'Κωνσταντίνῳ',
                            'secondname': {'Δούκα': 1},
                            'death': {'count': 2, 'dated': 0},
                            'legalrole': {'Basileus': 3, 'Basileus (co-emperor)': 3, 'Porphyrogennetos': 5},
                            'kinship': {'son': ['Maria/61', 'Michael/7'],
                                        'fiancé': ['Anna/62'],
                                        'grandson': ['Konstantinos/10'],
                                        'husband (betrothed)': ['Helena/101'],
                                        'husband (proposed)': ['Helena/101'],
                                        'son (only)': ['Maria/61'],
                                        'son-in-law': ['Alexios/1', 'Eirene/61']},
                            'possession': {
                                'An estate, Pentegostis, near Serres, with excellent water and buildings to house the '
                                'imperial entourage': ['Eustathios/20147', '269.60-62']}},
        'Konstantinos/64': {'gender': ['Eunuch'], 'identifier': 'Κωνσταντῖνος', 'altname': {'Θεοδώρῳ': 1},
                            'death': {'count': 1, 'dated': 0},
                            'legalrole': {'Domestikos of the eastern tagmata': 1, 'Nobelissimos': 7, 'Praipositos': 1,
                                          'Domestikos of the scholai': 1, 'Proedros': 1, 'Vestarches': 1,
                                          'Domestikos': 4, 'Megas domestikos': 2, 'Doux': 4, 'Patrikios': 1, 'Monk': 2,
                                          'Domestikos of the scholai of Orient': 1,
                                          'Domestikos of the scholai of the East': 1},
                            'occupation': {'Beggar': 1},
                            'kinship': {'brother': ['Ioannes/68', 'Michael/4'], 'uncle': ['Michael/5']},
                            'possession': {
                                'Estates in Opsikion where he was banished by <Zoe 1>': ['Ioannes/110', '416.77'],
                                'A house with a cistern near the Holy Apostles (in Constantinople)': ['Ioannes/110',
                                                                                                      '422.18']}},
        'Konstantinos/101': {'gender': ['Male'], 'identifier': 'Κωνσταντῖνος ὁ Διογένης',
                             'secondname': {'Διογένης': 6},
                             'death': {'count': 2, 'dated': 0},
                             'legalrole': {'Doux': 4, 'Patrikios': 2, 'Strategos': 3, 'Archon': 1, 'Monk': 1},
                             'kinship': {'husband': ['Anonyma/108'],
                                         'father': ['Romanos/4'],
                                         'husband of niece': ['Romanos/3'],
                                         'nephew (by marriage)': ['Romanos/3']}},
        'Konstantinos/102': {'gender': ['Male'], 'identifier': 'Κωνσταντίνῳ',
                             'secondname': {'Βοδίνῳ': 3},
                             'altname': {'Πέτρον ἀντὶ Κωνσταντίνου μετονομάσαντες': 1},
                             'legalrole': {'King': 2, 'Basileus': 1},
                             'kinship': {'son': ['Michael/101'], 'father': ['Georgios/20253']}},
        'Konstantinos/110': {'gender': ['Male'], 'identifier': 'Κωνσταντῖνος',
                             'legalrole': {'Patrikios': 1},
                             'kinship': {'nephew': ['Michael/4']}},
        'Liparites/101': {'gender': ['Male'], 'identifier': 'τοῦ Λιπαρίτου قاريط ملك الابخاز',
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
            'passages': 40
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
        self.graphdriver = GraphDatabase.driver(config.graphuri, auth=(config.graphuser, config.graphpw))
        self.constants = RELEVEN.PBWstarConstants.PBWstarConstants(self.graphdriver)
        # Get the UUIDs for each of our test people
        c = self.constants
        q = 'MATCH (id:%s)<-[:%s]-(idass:%s)-[:%s]->(person:%s), (idass)-[:%s]->(agent:%s) ' \
            'WHERE id.%s IN %s ' \
            'AND agent.%s = "Prosopography of the Byzantine World" ' \
            'RETURN id.%s as uri, person.uuid as uuid' % (
                c.get_label('E42'), c.get_label('P37'), c.get_label('E15'), c.get_label('P140'), c.get_label('E21'),
                c.get_label('P14'), c.get_label('E39'), c.get_label('P190'),
                [pburi(x) for x in self.td_people.keys()], c.get_label('P3'), c.get_label('P190'))
        with self.graphdriver.session() as session:
            result = session.run(q)
            self.assertIsNotNone(result)
            for record in result:
                idval = pbwid(record['uri'])
                self.td_people[idval]['uuid'] = record['uuid']

    # TODO add extra assertions for the eunuchs and K62
    def test_gender(self):
        """Test that each person has the gender assignments we expect"""
        c = self.constants
        for person, pinfo in self.td_people.items():
            q = "MATCH (p:%s)<-[:%s]-(a1:%s)-[:%s]->(ga:%s)<-[:%s]-(a2:%s)-[:%s]->(gender:%s) " \
                "WHERE p.uuid = '%s' RETURN p.%s as descname, gender.%s as gender" \
                % (c.get_label('E21'), c.star_object, c.get_assertion_for_predicate('P41'), c.star_subject,
                   c.get_label('E17'), c.star_subject, c.get_assertion_for_predicate('P42'), c.star_object,
                   c.get_label('C11'), pinfo['uuid'], c.get_label('P3'), c.get_label('P1'))
            with self.graphdriver.session() as session:
                result = session.run(q)
                self.assertIsNotNone(result)
                self.assertListEqual(pinfo['gender'], sorted(result.value('gender')),
                                     "Test gender for %s" % person)

    # The identifier is the name as PBW has it in the original language.
    def test_identifier(self):
        """Test that each person has the main appellation given in the PBW database"""
        c = self.constants
        for person, pinfo in self.td_people.items():
            # We want the appellation that was assigned by the generic PBW agent, not any of
            # the sourced ones
            pbwagent = '%s {%s: "Prosopography of the Byzantine World"}' % (c.get_label('E39'), c.get_label('P3'))
            q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(id:%s), (a)-[:%s]->(pbw:%s) WHERE p.uuid = '%s' RETURN id.%s AS id" \
                % (c.get_label('E21'), c.star_subject, c.get_assertion_for_predicate('P1'), c.star_object,
                   c.get_label('E41'), c.star_auth, pbwagent, pinfo['uuid'], c.get_label('P190'))
            with self.graphdriver.session() as session:
                result = session.run(q).single(strict=True)
                self.assertIsNotNone(result)
                self.assertEqual(pinfo['identifier'], result['id'], "Test identifier for %s" % person)

    def _check_dict_equiv(self, reference, nodelist, key, message):
        returned = dict()
        for node in nodelist:
            # Fortunately for us all the appellations are Greek
            thename = node.get(key)
            if thename in returned:
                returned[thename] += 1
            else:
                returned[thename] = 1
        self.assertDictEqual(reference, returned, message)

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
                q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(appel:%s), (a)-[:%s]->(src) " \
                    "WHERE p.uuid = '%s' RETURN appel" \
                    % (c.get_label('E21'), c.star_subject, c.get_assertion_for_predicate('P1'), c.star_object,
                       c.get_label('E41'), c.star_source, pinfo['uuid'])
                with self.graphdriver.session() as session:
                    result = session.run(q).value('appel')
                    self.assertIsNotNone(result)
                    # Check that the Greek appellations exist
                    found_appels = defaultdict(int)
                    for row in result:
                        for appel in row[c.get_label('P190')]:
                            if appel.endswith('@grc'):
                                found_appels[appel.rstrip('@grc')] += 1
                    self.assertDictEqual(names, found_appels)

    def test_death(self):
        """Test that each person has at most one death event, since they all only died once. Also
        test that the assertions look correct"""
        c = self.constants
        # Look for all death events and see who they are about
        deathevents = dict()
        q = "MATCH (de:%s)<-[:%s]-(a:%s)-[:%s]->(person:%s) RETURN DISTINCT person.uuid, de.uuid" \
            % (c.get_label('E69'), c.star_subject, c.get_assertion_for_predicate('P100'),
               c.star_object, c.get_label('E21'))
        with self.graphdriver.session() as session:
            result = session.run(q)
            for row in result:
                p = row['person.uuid']
                de = row['de.uuid']
                # Each person should have max one death event
                self.assertIsNone(deathevents.get(p))
                deathevents[p] = de

        for person, pinfo in self.td_people.items():
            # Check if the person should have a death event.
            devent = deathevents.get(pinfo['uuid'])
            dpred = c.get_assertion_for_predicate('P100')
            ddatepred = c.get_assertion_for_predicate('P4')
            if 'death' not in pinfo:
                # Make sure that the death was found
                self.assertIsNone(devent)
                continue
            else:
                self.assertIsNotNone(devent)
                # See if we have the expected info about the death event in question.
                # Each event should have N description assertions, each with a P3 attribute.
                q = "MATCH (de:%s)<-[:%s]-(a:%s) WHERE de.uuid = '%s' AND a.%s IS NOT NULL RETURN a" \
                    % (c.get_label('E69'), c.star_subject, dpred, devent, c.get_label('P3'))
                with self.graphdriver.session() as session:
                    result = session.run(q).value('a')
                    self.assertEqual(pinfo['death']['count'], len(result), "Death count for %s" % person)
                # and M date assertions.
                q = "MATCH (de:%s)<-[:%s]-(a:%s)-[:%s]->(dating:%s) WHERE de.uuid = '%s' " \
                    "RETURN COUNT(dating) AS num" \
                    % (c.get_label('E69'), c.star_subject, ddatepred, c.star_object,
                       c.get_label('E52'), devent)
                with self.graphdriver.session() as session:
                    result = session.run(q).single()['num']
                    self.assertEqual(pinfo['death']['dated'], result)
                # Each event should also have N different sources.
                q = "MATCH (de:%s)<-[:%s]-(a:%s)-[:%s]->(sref) WHERE de.uuid = '%s' " \
                    "RETURN COUNT(DISTINCT sref.uuid) AS num" \
                    % (c.get_label('E69'), c.star_subject, dpred, c.star_source, devent)
                with self.graphdriver.session() as session:
                    result = session.run(q).single()['num']
                    self.assertEqual(pinfo['death']['count'], result)

    def test_ethnicity(self):
        """Test that the ethnicity was created correctly for our people with listed ethnicities"""
        c = self.constants
        for person, pinfo in self.td_people.items():
            # Find those with a declared ethnicity. This means a membership in a group of the given type.
            if 'ethnicity' in pinfo:
                eths = pinfo['ethnicity']
                q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(group:%s)-[:%s]->(type:%s {%s: 'Ethnic Group'}) " \
                    "WHERE p.uuid = '%s' RETURN group.%s AS eth, COUNT(group.%s) AS act" \
                    % (c.get_label('E21'), c.star_object, c.get_assertion_for_predicate('P107'), c.star_subject,
                       c.get_label('E74'), c.get_label('P2'), c.get_label('E55'), c.get_label('P1'), pinfo['uuid'],
                       c.get_label('P1'), c.get_label('P1'))
                with self.graphdriver.session() as session:
                    result = session.run(q)
                    rowct = 0
                    for row in result:
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
                q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(relaff:%s)<-[:%s]-(a2:%s)-[%s]->(rel:%s), (auth)<-[:%s]-(a) " \
                    "WHERE p.uuid = '%s' RETURN rel.%s as rel, auth.%s as auth" \
                    % (c.get_label('E21'), c.star_object, c.get_assertion_for_predicate('SP36'), c.star_subject,
                       c.get_label('C23'), c.star_subject, c.get_assertion_for_predicate('SP35'), c.star_object,
                       c.get_label('C24'), c.star_auth, pinfo['uuid'], c.get_label('P1'), c.get_label('P3'))
                with self.graphdriver.session() as session:
                    # We are cheating by knowing that no test person has more than one religion specified
                    result = session.run(q).single(strict=True)
                    self.assertIsNotNone(result)
                    self.assertTrue(result['rel'] in rels)
                    self.assertIn('Georgios Tornikes', result['auth'])

    def test_occupation(self):
        """Test that occupations / non-legal designations are set correctly"""
        c = self.constants
        for person, pinfo in self.td_people.items():
            # Check that the occupation assertions were created
            if 'occupation' in pinfo:
                occs = pinfo['occupation']
                q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(pocc:%s)<-[:%s]-(a2:%s)-[%s]->(occ:%s) " \
                    "WHERE p.uuid = '%s' RETURN occ" \
                    % (c.get_label('E21'), c.star_object, c.get_assertion_for_predicate('SP13'), c.star_subject,
                       c.get_label('C1'), c.star_subject, c.get_assertion_for_predicate('SP14'), c.star_object,
                       c.get_label('C7'), pinfo['uuid'])
                with self.graphdriver.session() as session:
                    result = session.run(q).value('occ')
                    self.assertIsNotNone(result)
                    self._check_dict_equiv(occs, result, c.get_label('P1'), "Test occupations for %s" % person)

    def test_legalrole(self):
        """Test that legal designations are set correctly"""
        c = self.constants
        for person, pinfo in self.td_people.items():
            # Check that the occupation assertions were created
            if 'legalrole' in pinfo:
                roles = pinfo['legalrole']
                q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(prole:%s)<-[:%s]-(a2:%s)-[%s]->(role:%s) " \
                    "WHERE p.uuid = '%s' RETURN role" \
                    % (c.get_label('E21'), c.star_object, c.get_assertion_for_predicate('SP26'), c.star_subject,
                       c.get_label('C13'), c.star_subject, c.get_assertion_for_predicate('SP33'), c.star_object,
                       c.get_label('C12'), pinfo['uuid'])
                with self.graphdriver.session() as session:
                    result = session.run(q).value('role')
                    self.assertIsNotNone(result)
                    self._check_dict_equiv(roles, result, c.get_label('P1'), "Test legal roles for %s" % person)

    def test_languageskill(self):
        """Test that our Georgian monk has his language skill set correctly"""
        c = self.constants
        for person, pinfo in self.td_people.items():
            # Find those with a language skill set
            if 'language' in pinfo:
                q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(skill:%s)<-[:%s]-(a2:%s)-[:%s]->(kh:%s), " \
                    "(kh)-[:%s]->(type:%s) " \
                    "WHERE p.uuid = '%s' RETURN kh.%s as kh, type.%s as type" % (
                    c.get_label('E21'), c.star_subject, c.get_assertion_for_predicate('SP38'), c.star_object,
                    c.get_label('C21'), c.star_subject, c.get_assertion_for_predicate('SP37'), c.star_object,
                    c.get_label('C29'), c.get_label('P2'), c.get_label('E55'), pinfo['uuid'],
                    c.get_label('P1'), c.get_label('P1'))
                with self.graphdriver.session() as session:
                    result = session.run(q).single()  # At the moment we do only have one
                    self.assertIsNotNone(result)
                    self.assertEqual(pinfo['language'], result['kh'], "Test language for %s" % person)
                    self.assertEqual('Language Skill', result['type'])

    def test_kinship(self):
        """Test the kinship assertions for one of our well-connected people"""
        c = self.constants
        for person, pinfo in self.td_people.items():
            if 'kinship' in pinfo:
                q = "MATCH (p:%s {uuid: '%s'})<-[:%s]-(a:%s)-[:%s]->" \
                    "(kg:%s)<-[:%s]-(a2:%s)-[:%s]->(kin:%s), " \
                    "(kg)<-[:%s]-(a3:%s)-[:%s]->(ktype:%s), " \
                    "(kin)<-[:%s]-(ia2:%s)-[:%s]->(kinid:%s) " \
                    "RETURN DISTINCT kinid.%s as kin, ktype.%s as kintype" % (
                    c.get_label('E21'), pinfo['uuid'], c.star_object, c.get_assertion_for_predicate('SP17'),
                    c.star_subject, c.get_label('C3'), c.star_subject, c.get_assertion_for_predicate('SP18'),
                    c.star_object, c.get_label('E21'), c.star_subject, c.get_assertion_for_predicate('SP16'),
                    c.star_object, c.get_label('C4'),
                    c.star_subject, c.get_label('E15'), c.get_label('P37'), c.get_label('E42'),
                    c.get_label('P190'), c.get_label('P1')  # CHANGE TO P1
                )
                with self.graphdriver.session() as session:
                    result = session.run(q)
                    foundkin = dict()
                    for row in result:
                        k = row['kintype']
                        if k not in foundkin:
                            foundkin[k] = []
                        foundkin[k].append(pbwid(row['kin']))
                    for k in foundkin:
                        foundkin[k] = sorted(foundkin[k])
                    self.assertDictEqual(pinfo['kinship'], foundkin, "Kinship links for %s" % person)


    def test_possession(self):
        """Check possession assertions. Test the sources and authors/authorities while we are at it."""
        c = self.constants
        a = c.get_assertion_for_predicate('P51')
        a2 = c.get_assertion_for_predicate('R15')
        a3 = c.get_assertion_for_predicate('R3')
        a4 = c.get_assertion_for_predicate('R16')
        a5 = c.get_assertion_for_predicate('P14')
        for person, pinfo in self.td_people.items():
            # Find those who possess something. All the possessions are documented in written sources
            # whose authors are also in PBW; exploit this to test that the written sources were set up correctly.
            if 'possession' in pinfo:
                q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(poss:%s), " \
                    "(a)-[:%s]->(author)<-[:%s]-(idass:%s)-[%s]->(id:%s), " \
                    "(a)-[:%s]->(src:%s)<-[:%s]-(a2:%s)-[:%s]->(edition:%s), " \
                    "(edition)<-[:%s]-(a3:%s)-[:%s]->(work:%s), " \
                    "(work)<-[:%s]-(a4:%s)-[:%s]->(creation:%s), " \
                    "(creation)<-[:%s]-(a5:%s)-[:%s]->(author) " \
                    "WHERE p.uuid = '%s' RETURN poss.%s as poss, id.%s as id, src.%s as src" % (
                        # person is object property of possession
                        c.get_label('E21'), c.star_object, a, c.star_subject, c.get_label('E18'),
                        # ...according to author, who is known by an identifier
                        c.star_auth, c.star_subject, c.get_label('E15'), c.star_object, c.get_label('E42'),
                        # as we know from source extract, which belongs to the edition
                        c.star_source, c.get_label('E33'), c.star_object, a2, c.star_subject, c.get_label('F2'),
                        # the edition belongs to a work
                        c.star_object, a3, c.star_subject, c.get_label('F1'),
                        # the work belongs to a creation event
                        c.star_object, a4, c.star_subject, c.get_label('F27'),
                        # the creation involves our author, who carried it out
                        c.star_subject, a5, c.star_object, pinfo['uuid'],
                        # Just get back the data fields we want
                        c.get_label('P1'), c.get_label('P190'), c.get_label('P3')
                    )
                with self.graphdriver.session() as session:
                    result = session.run(q)  # At the moment we do only have one
                    rowct = 0
                    for row in result:
                        rowct += 1
                        poss = row['poss']
                        author = pbwid(row['id'])
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
        bq = "MATCH (boul:%s)<-[:%s]-(idass:%s)-[:%s]->(ident:%s), " \
             "(boul)<-[:%s]-(a:%s)-[:%s]->(inscr:%s), " \
             "(a)-[:%s]->(src), (a)-[:%s]->(auth) RETURN DISTINCT boul, ident, inscr, src, auth" % (
            c.get_label('E22'), c.star_subject, c.get_label('E15'), c.get_label('P37'), c.get_label('E42'),
            c.star_subject, c.get_assertion_for_predicate('P128'), c.star_object, c.get_label('E34'),
            c.star_source, c.star_auth)
        with self.graphdriver.session() as session:
            result = session.run(bq)
            for row in result:
                # The boulloterion should have a descname that starts with 'Boulloterion'
                self.assertTrue(row['boul'].get(c.get_label('P3'), '').startswith('Boulloterion of'))
                # Its identity should be a PBW URL with some boulloterion ID
                pbwid = row['ident'].get(c.get_label('P190'))
                boulid = int(pbwid.replace('https://pbw2016.kdl.kcl.ac.uk/boulloterion/', ''))
                # We should be expecting this boulloterion
                self.assertIn(boulid, self.td_boulloterions, "Boulloterion %d should exist" % boulid)
                boulinfo = self.td_boulloterions[boulid]

                # but we should not have seen it yet
                self.assertNotIn(boulid, found, "Boulloterion %d should not be duplicated" % boulid)
                found.add(boulid)
                # The boulloterion should have a correct inscription
                self.assertEqual(boulinfo['inscription'], row['inscr'].get(c.get_label('P190')))
                # The boulloterion should have the correct named authority
                self.assertEqual('Jeffreys, Michael J.', row['auth'].get(c.get_label('P3')))
                # Separate query to check the boulloterion sources on the inscription assertion
                if len(boulinfo['sources']) > 1:
                    bsq = 'MATCH (src:%s)-[:%s]-(ed:%s) WHERE src.uuid = "%s" RETURN ed' % (
                        c.get_label('E73'), c.get_label('P165'), c.get_label('F2'), row['src'].get('uuid'))
                else:
                    bsq = 'MATCH (ed:%s) WHERE ed.uuid = "%s" RETURN ed' % (c.get_label('F2'), row['src'].get('uuid'))
                r2 = session.run(bsq)
                for row2 in r2:
                    sid = row2['ed'].get(c.get_label('P1'))
                    self.assertIn(sid, boulinfo['sources'])
                    self.assertEqual(row2['ed'].get(c.get_label('P102')), boulinfo['sources'][sid])

                # Separate query to check the boulloterion seals and their respective assertions. A seal was
                # produced by a boulloterion and belongs to a collection according to the same authority as above,
                # and these assertions has no explicit source.
                sealq = 'MATCH (boul:%s)<-[:%s]-(sealass:%s)-[:%s]->(seal:%s), ' \
                        '(seal)<-[:%s]-(collass:%s)-[:%s]->(coll:%s), ' \
                        '(sealass)-[:%s]->(auth), (collass)-[:%s]->(auth) RETURN seal, coll, auth, src' % (
                    c.get_label('E22B'), c.star_subject, c.get_assertion_for_predicate('P108'), c.star_object,
                    c.get_label('E22S'), c.star_object, c.get_assertion_for_predicate('P46'), c.star_subject,
                    c.get_label('E78'), c.star_auth, c.star_auth)
                r3 = session.run(sealq)
                sealcolls = list(boulinfo['seals'].values())
                for row3 in r3:
                    # Check that the seal's collection ID is in the sealcolls list
                    try:
                        sealcolls.remove(row3['coll'].get(c.get_label('P1')))
                    except ValueError:
                        self.fail()
                # Check that we found all the seals
                self.assertEqual(0, len(sealcolls))


    def test_text_sources(self):
        """Spot-check different textual sources and make sure they are set up correctly"""
        c = self.constants
        p3 = c.get_label('P3')
        for s, sinfo in self.spot_sources.items():
            if 'author' in sinfo:
                # The Chronographia: work was created by author according to author based on passage
                # The typikon: work was created by author according to editor
                # The praktikon: work was created by author according to PBW editor
                # All: work has edition according to editor based on edition
                q = 'MATCH (work:%s)<-[:%s]-(wc1:%s)-[:%s]->(wc:%s)<-[:%s]-(wc2:%s)-[:%s]->(author:%s), ' \
                    '(wc1)-[:%s]->(authority), (wc2)-[:%s]->(authority), ' \
                    '(wc1)-[:%s]->(passage:%s), (wc2)-[:%s]->(passage), ' \
                    '(work)<-[:%s]-(wed:%s)-[:%s]->(edition:%s), (wed)-[:%s]->(editor), (wed)-[:%s]->(edition) ' \
                    'WHERE work.%s = "%s" '\
                    'RETURN work.%s as work, author.%s as author, authority.%s as authority, editor.%s as editor, ' \
                    'edition.%s as edition, passage.%s as passage' % (
                    c.get_label('F1'), c.star_object, c.get_assertion_for_predicate('R16'), c.star_subject,
                    c.get_label('F27'), c.star_subject, c.get_assertion_for_predicate('P14'), c.star_object,
                    c.get_label('E21'), c.star_auth, c.star_auth, c.star_source, c.get_label('E33'), c.star_source,
                    c.star_subject, c.get_assertion_for_predicate('R3'), c.star_object, c.get_label('F2'),
                    c.star_auth, c.star_source, p3, sinfo.get('work'), p3, p3, p3, p3, p3, p3
                )
            elif 'work' in sinfo:
                # The 1094 synod: work has edition according to editor based on edition
                q = 'MATCH (work:%s)<-[:%s]-(wed:%s)-[:%s]->(edition:%s), ' \
                    '(wed)-[:%s]->(editor), (wed)-[:%s]->(edition) ' \
                    'WHERE work.%s = "%s" ' \
                    'RETURN work.%s as work, editor.%s as editor, edition.%s as edition' % (
                    c.get_label('F1'), c.star_subject, c.get_assertion_for_predicate('R3'), c.star_object,
                    c.get_label('F2'), c.star_auth, c.star_source,
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
                    c.get_label('F2'), c.star_object, c.get_assertion_for_predicate('R17'), c.star_subject,
                    c.get_label('F28'), c.star_subject, c.get_assertion_for_predicate('P14'), c.star_object,
                    c.star_auth, c.star_auth, c.star_source, c.star_source,
                    p3, sinfo.get('edition'), p3, p3
                )
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
                    c.get_label('F2'), p3, sinfo.get('edition'), c.star_subject, c.get_assertion_for_predicate('R15'),
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
        p70 = c.get_label('P70') # the documents predicate
        f2 = c.get_label('F2')   # the DB record per assertion
        r17 = c.get_label('R17') # linking the record to its creation
        f28 = c.get_label('F28') # the data creation record
        p4 = c.get_label('P4')   # created at
        e52 = c.get_label('E52') # a particular time
        p80 = c.get_label('P80') # with this timestamp
        p14 = c.get_label('P14') # carried out by...
        e21 = c.get_label('E21') # ...me.
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

    def tearDown(self):
        self.graphdriver.close()


if __name__ == '__main__':
    unittest.main()
