import config
import re
import unittest
from collections import Counter, defaultdict
from functools import reduce
from rdflib import RDF, Literal
from rdflib.exceptions import UniquenessError
from rdflib.plugins.stores import sparqlstore
from RELEVEN import PBWstarConstants, graphimportSTAR
from sqlalchemy.exc import DatabaseError
from tempfile import NamedTemporaryFile


def pburi(x):
    return f"https://pbw2016.kdl.kcl.ac.uk/person/{x}/"


def count_result(res):
    return reduce(lambda x, y: x + 1, res, 0)


# noinspection PyUnresolvedReferences
class GraphImportTests(unittest.TestCase):
    graphdriver = None
    constants = None
    # Data keys are gender, identifier (appellation), second-name appellation, alternate-name appellation,
    # death, ethnicity, religion, societyrole, legalrole, language, kinship, possession
    td_people = {
        'Anna 62': {'gender': ['Female'], 'identifier': 'Ἄννα Κομνηνή',
                    'descriptor': 'Anna Komnene, daughter of Alexios I and historian',
                    'secondname': {'Κομνηνοῦ': {'count': 2}},
                    'death': {'count': 1, 'dated': 0},
                    'religion': {'Christian': ['Georgios 25002']},
                    'legalrole': {'Basilis': 1, 'Basilissa': 1, 'Kaisarissa': 4},
                    'occupation': {'Pansebastos sebaste': 1, 'Porphyrogennetos': 6},
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
                    'descriptor': 'Anna the kouropalatissa, mother of Nikephoros Bryennios the usurper',
                    'occupation': {'Kouropalatissa': 1},
                    'kinship': {'grandmother': ['Anonymus 61'],
                                'mother': ['Ioannes 61', 'Nikephoros 62']}},
        'Anna 101': {'gender': ['Female'], 'identifier': 'Ἄννα',
                     'descriptor': 'Anna, daughter of Konstantinos X',
                     'altname': {'Ἀρετῆς': {'count': 1, 'source': 'Michel Psellos, Chronographie, 2 vols., Paris 1967'}},
                     'occupation': {'Nun': 1},
                     'kinship': {'daughter': ['Eudokia 1', 'Konstantinos 10']}},
        'Anna 102': {'gender': ['Female'], 'identifier': ' Ἄννῃ',
                     'descriptor': 'Anna, wife of Eustathios Boilas',
                     'death': {'count': 1, 'dated': 0}, 'occupation': {'Nun': 1},
                     'kinship': {'wife': ['Eustathios 105'],
                                 'mother': ['Romanos 106']}},
        'Apospharios 101': {'gender': ['Male'], 'identifier': ' Ἀποσφάριον',
                            'descriptor': 'Apospharios, slave of Eustathios Boilas',
                            'legalrole': {'Slave': 1},
                            'kinship': {'husband': ['Selegno 101']}},
        'Bagrat 101': {'gender': ['Male'], 'identifier': 'τῷ Παγκρατίῳ بقراط بن جرجس',
                       'descriptor': 'Bagrat IV, king of Georgia',
                       'legalrole': {'King': 3, 'Kouropalates': 2, 'Sebastos': 1},
                       'kinship': {'son': ['Anonyma 6003', 'Georgios 105', 'Maria 103'],
                                   'husband': ['Helena 104'], 'father': ['Maria 61']}},
        'Balaleca 101': {'gender': ['Male'], 'identifier': 'Βαλαλεχα',
                         'descriptor': 'Balaleca, Georgian monk on Athos (Iveron?)',
                         'language': 'Georgian'},
        'Gagik 101': {'gender': ['Male'], 'identifier': 'Κακίκιος',
                      'descriptor': 'Gagik II, king of Armenia',
                      # Archon should be 1 but is 2 in production, because one of the two factoids had a
                      # geographical designation but they have the same authority and source string.
                      'legalrole': {'Archon': 1, 'King': 1, 'Magistros': 1},
                      'kinship': {'son': ['Ashot 101'],
                                  'husband': ['Anonyma 158', 'Anonyma 159'],
                                  'son (in fact, nephew)': ['Ioannes 106']},
                      'possession': {'villages yielding a high income in Cappadocia, Charsianon and Lykandos':
                                         ['Ioannes 110', '437.28-29'],
                                     'Estates much poorer than Ani and its territory':
                                         ['Aristakes 101', '63.8-9 (55)']}},
        'Herve 101': {'gender': ['Male'], 'identifier': 'Ἐρβέβιον τὸν Φραγγόπωλον',
                      'descriptor': 'Hervé Phrangopoulos/Frankopoulos',
                      'secondname': {'Φραγγόπωλον': {'count': 2}},
                      'ethnicity': {'Norman': 1},
                      'legalrole': {'Stratelates': 1, 'Vestes': 1, 'Magistros': 1},
                      'possession': {'House at Dagarabe in Armeniakon': ['Ioannes 110', '485.52']}},
        'Ioannes 62': {'gender': ['Male'], 'identifier': 'Ἰωάννης',
                       'descriptor': 'Ioannes Doukas, kaisar',
                       'secondname': {'Δούκα': {'count': 6}},
                       'altname': {'Ἰγνάτιος': {'count': 1,
                                                'source': '“Commémoraisons des Comnènes dans le typikon liturgique du '
                                                          'monastère du Christ Philanthrope (ms. Panaghia Kamariotissa '
                                                          '29)”, Revue des études Byzantines 63 (2005), 41-69'}},
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
                       'descriptor': 'Ioannes the Orphanotrophos, brother of Michael IV',
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
                        'descriptor': 'Ioannes of Lampe, monk and archbishop of all Bulgaria',
                        'death': {'count': 1, 'dated': 0},
                        'legalrole': {'Archbishop': 3, 'Monk': 3}},
        'Ioannes 102': {'gender': ['Eunuch'], 'identifier': 'Ἰωάννην',
                        'descriptor': 'Ioannes, metropolitan of Side [1079, 1082, 1094]',
                        # Metropolitan should be 12 but is 13 currently in production, because it erroneously
                        # included an out-of-scope letter of Theophylact of Ohrid
                        'legalrole': {'Bishop': 1, 'Metropolitan': 12, 'Protoproedros': 1, 'Hypertimos': 2,
                                      'Protoproedros of the protosynkelloi': 2, 'Protosynkellos': 2}},
        'Ioannes 110': {'gender': ['Male'], 'identifier': 'Ἰωάννου...τοῦ Σκυλίτζη',
                        'descriptor': 'Ioannes Skylitzes, historian',
                        'legalrole': {'Megas droungarios of the vigla': 1, 'Kouropalates': 1}},
        'Konstantinos 62': {'gender': ['Male'], 'identifier': 'Κωνσταντίνῳ',
                            'descriptor': 'Konstantinos Doukas porphyrogennetos, son of Michael VII',
                            'secondname': {'Δούκα': {'count': 1,
                                                     'source': 'Annae Comnenae Alexias, Corpus fontium historiae '
                                                               'Byzantinae 40/1, Berlin – New York 2001'}},
                            'death': {'count': 2, 'dated': 0},
                            'legalrole': {'Basileus': 3, 'Basileus (co-emperor)': 3},
                            'occupation': {'Porphyrogennetos': 5},
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
        'Konstantinos 64': {'gender': ['Eunuch'], 'identifier': 'Κωνσταντῖνος',
                            'descriptor': 'Konstantinos, brother of Michael IV',
                            'altname': {'Θεοδώρῳ': {'count': 1, 'source': '“Βυζαντινὰ χρυσόβουλλα καὶ πιττάκια”,'
                                                                          ' Ἐκκλησιαστικὴ Ἀλήθεια 4 (1883-84) 431'}},
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
                             'descriptor': 'Konstantinos Diogenes, father of Romanos IV',
                             'secondname': {'Διογένης': {'count': 6}},
                             'death': {'count': 2, 'dated': 0},
                             'legalrole': {'Doux': 4, 'Patrikios': 2, 'Strategos': 3, 'Archon': 1, 'Monk': 1},
                             'kinship': {'husband': ['Anonyma 108'],
                                         'father': ['Romanos 4'],
                                         'husband of niece': ['Romanos 3'],
                                         'nephew (by marriage)': ['Romanos 3']}},
        'Konstantinos 102': {'gender': ['Male'], 'identifier': 'Κωνσταντίνῳ',
                             'descriptor': 'Konstantinos Bodin, king of Duklja',
                             'secondname': {'Βοδίνῳ': {'count': 3}},
                             'altname': {'Πέτρον ἀντὶ Κωνσταντίνου μετονομάσαντες':
                                             {'count': 1,
                                              'source': 'Ἡ συνέχεια τῆς Χρονογραφίας τοῦ Ἰωάννου Σκυλίτση (Ioannes '
                                                        'Skylitzes Continuatus), Ἑταιρεία Μακεδονικῶν Σπουδῶν, Ιδρυμα '
                                                        'Μελετῶν Χερσονήσου τοῦ Αἵμου 105, Thessalonike (1968) 103-186'}
                                         },
                             'legalrole': {'King': 2, 'Basileus': 1},
                             'kinship': {'son': ['Michael 101'], 'father': ['Georgios 20253']}},
        'Konstantinos 110': {'gender': ['Male'], 'identifier': 'Κωνσταντῖνος',
                             'descriptor': 'Konstantinos, nephew of Michael IV',
                             'legalrole': {'Patrikios': 1},
                             'kinship': {'nephew': ['Michael 4']}},
        'Liparites 101': {'gender': ['Male'], 'identifier': 'τοῦ Λιπαρίτου قاريط ملك الابخاز',
                          'descriptor': 'Liparit IV, duke of Trialeti',
                          'ethnicity': {'Georgian': 2}, 'legalrole': {'Lord of part of the Iberians': 1}}
    }

    td_boulloterions = {
        112: {'inscription': 'Κωνσταντῖνος πρόεδρος δομέστικος / τῶν σχολῶν τῆς ᾿Ανατολῆς καὶ δοὺξ ᾿Αντιοχείας',
              'seals': {'1008.112.8': 'Vienna, private collection of Prof. Werner Seibt'}, 'sources': {
                'Seibt, BBÖ I': 'W. Seibt, Die byzantinischen Bleisiegel in Österreich I. Teil, Kaiserhof, Vienna 1978 [reviewed by V. Šandrovskaja and I.V.Sokolova in Byzantinoslavica 41 (1980), 251-255]',
                'Wassiliou - Seibt BBÖ II ': 'A.-K. Wassiliou - W. Seibt, Die byzantinischen Bleisiegel in Österreich, 2. Teil: Zentral- und Provinzialverwaltung, Vienna 2003'}},
        114: {'inscription': 'Κύριε βοήθει τῷ σῷ δούλῳ / ᾿Ιγνατίῳ μοναχῷ τῷ καίσαρι',
              'auth': 'Jeffreys, Michael J.; Karágiṓrgou, ́Olga',
              'seals': {'21.114.398': 'Vienna, Kunsthistorisches Museum, Münzkabinett', '29.114.1084': 'Cambridge, Mass., Fogg Art Museum',
                        '14.114.260': 'Athens, Nomismatikon Mouseion, Main collection',
                        '1004.114.0': 'Private collection: Basel, G. Zacos (largely dispersed)', '20.114.1952': 'St Petersburg, Hermitage',
                        '1027.114.0': 'Unknown collection: details temporarily or permanently unavailable'}, 'sources': {
                'Seibt, BBÖ I': 'W. Seibt, Die byzantinischen Bleisiegel in Österreich I. Teil, Kaiserhof, Vienna 1978 [reviewed by V. Šandrovskaja and I.V.Sokolova in Byzantinoslavica 41 (1980), 251-255]',
                'Konstantopoulos, Nom. Mous.': 'K.M. Konstantopoulos, Byzantiaka molyvdoboulla tou en Athenais Ethnikou Nomismatikou Mouseiou, Athens 1917',
                'Laurent, Corpus V.2': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, V.2, L'Église, Paris 1965 [reviewed by V. Grumel in Byzantinische Zeitschrift 61 (1968), 129; W. Seibt in Byzantinoslavica 35 (1974), 73-84]",
                'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel',
                'Stavrakos': 'Ch. Stavrakos, Die byzantinischen Bleisiegel mit Familiennamen aus der Sammlung des Numismatischen Museums Athen, Wiesbaden 2000 [reviewed by Cl. Sode in Byzantinische Zeitschrift 95 (2002), 168-170 and J. Nesbitt in Speculum 77 (2002), 996-998]',
                'Iashvili - Seibt': 'I. Iashvili - W. Seibt, "Byzantinische Siegel aus Petra in Westgeorgien", Studies in Byzantine Sigillography 9, pp. 1-9'}},
        271: {'inscription': 'Μήτηρ Θεοῦ. / Θεοτόκε βοήθει τῷ σῷ δούλῳ ᾿Ιωάννῃ μοναχῷ καὶ ἀρχιεπισκόπῳ πάσης Βουλγαρίας',
              'auth': 'Jeffreys, Michael J.; Karágiṓrgou, ́Olga',
              'seals': {'5.271.5308': 'Washington, Dumbarton Oaks Research Library and Collection: 58 series',
                        '29.271.1035': 'Cambridge, Mass., Fogg Art Museum', '1105.271.1642': 'Sale Catalogue: Hirsch 186 (May, 1995)'},
              'sources': {
                  'Nesbitt - Oikonomides I': 'J. Nesbitt and N. Oikonomides, Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 1: Italy, North of the Balkans, North of the Black Sea, Washington D.C. 1991 [reviewed by W. Seibt in Byzantinische Zeitschrift 84/85 (1991), 548-5',
                  'Laurent, Corpus V.2': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, V.2, L'Église, Paris 1965 [reviewed by V. Grumel in Byzantinische Zeitschrift 61 (1968), 129; W. Seibt in Byzantinoslavica 35 (1974), 73-84]",
                  'Laurent, Corpus V.3': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, V.3, L'Église: Supplément, Paris 1972 [reviewed by W. Seibt in Byzantinoslavica 35 (1974), 73-84 and by N. Oikonomides in Speculum 49 (1974), 746-7]",
                  'Jordanov, Corpus I': 'I. Jordanov, Corpus of Byzantine Seals from Bulgaria, vol. 1: Byzantine Seals with Geographical Names, Sofia 2003 [reviewed by W. Seibt in Byzantinische Zeitschrift 98, 2005, pp. 129-133]'}},
        272: {
            'inscription': '[Μήτηρ Θεοῦ]. / Θεοτόκε βοήθει τῷ σῷ δούλῳ ᾿Ιωάννῃ μοναχῷ καὶ ἀρχιεπισκόπῳ πάσης Βουλγαρίας',
            'seals': {'4.272.4701': 'Washington, Dumbarton Oaks Research Library and Collection: 55 series'}, 'sources': {
                'Nesbitt - Oikonomides I': 'J. Nesbitt and N. Oikonomides, Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 1: Italy, North of the Balkans, North of the Black Sea, Washington D.C. 1991 [reviewed by W. Seibt in Byzantinische Zeitschrift 84/85 (1991), 548-5'}},
        283: {'inscription': 'Κύριε βοήθει τῷ σῷ δούλῳ Κωνσταντίνῳ / πατρικίῳ καὶ στρατηγῷ Σερβίας τῷ Διογένῃ',
              'seals': {'29.283.562': 'Cambridge, Mass., Fogg Art Museum'}, 'sources': {
                'Nesbitt - Oikonomides I': 'J. Nesbitt and N. Oikonomides, Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 1: Italy, North of the Balkans, North of the Black Sea, Washington D.C. 1991 [reviewed by W. Seibt in Byzantinische Zeitschrift 84/85 (1991), 548-5',
                'Laurent, Serbie': 'V. Laurent, "La thème byzantine de Serbie au XIe siècle", Revue des Études Byzantines 15, 1957'}},
        1406: {'inscription': '῾Ο ἅγιος Νικόλαος. / Κύριε βοήθει τῷ σῷ δούλῳ Νικήτᾳ καὶ ἀνθρώπῳ τοῦ εὐτυχεστάτου καίσαρος',
               'auth': 'Karágiṓrgou, ́Olga',
               'seals': {'1119.1406.297': 'Sale Catalogue: Spink: October 6, 1999'}, 'sources': {
                'Zacos II': 'G. Zacos, Byzantine Lead Seals II, compiled and edited by J.W. Nesbitt, Bern 1984 [reviewed by H. Hunger in Jahrbuch der Österreichischen Byzantinistik 36 (1986), 333-339 and by N. Oikonomides, "A propos d\'une nouvelle publication de sceaux byzantins", Re'}},
        2216: {
            'inscription': 'Μιχαήλ. / ῞Ορα σφραγίδα πρωτοπροέδρου Σίδης',
            'auth': 'Jeffreys, Michael J.; Karágiṓrgou, ́Olga',
            'seals': {'29.2216.1333': 'Cambridge, Mass., Fogg Art Museum',
                      '5.2216.194': 'Washington, Dumbarton Oaks Research Library and Collection: 58 series',
                      '4.2216.4993': 'Washington, Dumbarton Oaks Research Library and Collection: 55 series',
                      '14.2216.143': 'Athens, Nomismatikon Mouseion, Main collection',
                      '2.2216.213': 'Paris, Institut Français d’études byzantines', # was two?!
                      '1012.2216.0': 'Étampes, Thierry collection'},
            'sources': {
                'Nesbitt - Oikonomides II': 'J. Nesbitt and N. Oikonomides,  Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 2: South of the Balkans, the Islands, South of Asia Minor, Washington D.C. 1994 [reviewed by W. Seibt in Byzantinische Zeitschrift 90 (1997), 460-',
                'Laurent, Corpus V.1': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, V.1, L'Église, Paris 1963 [reviewed by V. Grumel in Byzantinische Zeitschrift 59 (1966), 392-396 and by W. Seibt in Byzantinoslavica 35 (1974), 73-84]",
                'Konstantopoulos, Nom. Mous.': 'K.M. Konstantopoulos, Byzantiaka molyvdoboulla tou en Athenais Ethnikou Nomismatikou Mouseiou, Athens 1917',
                'Laurent, Corpus V.3': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, V.3, L'Église: Supplément, Paris 1972 [reviewed by W. Seibt in Byzantinoslavica 35 (1974), 73-84 and by N. Oikonomides in Speculum 49 (1974), 746-7]"}},
        2217: {'inscription': 'Μιχαήλ. / Σφραγὶς προέδρου τῆς Σίδης ὑπερτίμου',
               'seals': {'5.2217.1149': 'Washington, Dumbarton Oaks Research Library and Collection: 58 series',
                         '5.2217.3647': 'Washington, Dumbarton Oaks Research Library and Collection: 58 series',
                         '21.2217.249': 'Vienna, Kunsthistorisches Museum, Münzkabinett', '20.2217.0': 'St Petersburg, Hermitage'},
               'sources': {
                   'Nesbitt - Oikonomides II': 'J. Nesbitt and N. Oikonomides,  Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 2: South of the Balkans, the Islands, South of Asia Minor, Washington D.C. 1994 [reviewed by W. Seibt in Byzantinische Zeitschrift 90 (1997), 460-',
                   'Pančenko IRAIK 8': 'B.A. Pančenko, Kollekcii Russkago Archeologičeskago Instituta v Konstantinopolě. Katalog molivdovulov, Sofia 1908 (repr. from Izvestija Russkago Archeologičeskago Instituta v Konstantinopolě 8 (1903), 199-246)',
                   'Laurent, Corpus V.1': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, V.1, L'Église, Paris 1963 [reviewed by V. Grumel in Byzantinische Zeitschrift 59 (1966), 392-396 and by W. Seibt in Byzantinoslavica 35 (1974), 73-84]"}},
        2218: {'inscription': 'Μιχαήλ. / Κύριε βοήθει ᾿Ιωάννῃ μητροπολίτῃ Σίδης καὶ πρωτοσυγκέλλῳ',
               'auth': 'Jeffreys, Michael J.; Karágiṓrgou, ́Olga',
               'seals': {'4.2218.4845': 'Washington, Dumbarton Oaks Research Library and Collection: 55 series'}, 'sources': {
                'Laurent, Corpus V.3': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, V.3, L'Église: Supplément, Paris 1972 [reviewed by W. Seibt in Byzantinoslavica 35 (1974), 73-84 and by N. Oikonomides in Speculum 49 (1974), 746-7]",
                'Nesbitt - Oikonomides II': 'J. Nesbitt and N. Oikonomides,  Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 2: South of the Balkans, the Islands, South of Asia Minor, Washington D.C. 1994 [reviewed by W. Seibt in Byzantinische Zeitschrift 90 (1997), 460-'}},
        2566: {
            'inscription': 'Μιχαὴλ ὁ ᾿Αρχάγγελος | ὁ στρατηλάτης. | ῾Ο ἅγιος Δημήτριος. / Κύριε βοήθει τῷ σῷ δούλῳ ᾿Ιωάννῃ νωβελλισσίμῳ πρωτοβεστιαρίῳ καὶ μεγάλῳ δομεστίκῳ τῶν σχολῶν τῆς ᾿Ανατολῆς',
            'seals': {'5.2566.3248': 'Washington, Dumbarton Oaks Research Library and Collection: 58 series'}, 'sources': {
                'Laurent, Corpus II': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, II, L'administration centrale, Paris 1981 [reviewed by J. Nesbitt in Speculum 58 (1983), 771-772, and by W. Seibt in Jahrbuch der Österreichischen Byzantinistik 26 (1977), 325]",
                'Nesbitt - Oikonomides III': 'J. Nesbitt and N. Oikonomides, Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 3: West, Northwest and Central Asia Minor and the Orient, Washington D.C. 1996 [reviewed by W. Seibt in Byzantinische Zeitschrift 92 (1999), 538-54',
                'Cheynet, Par St Georges': 'J.-Cl. Cheynet, Par St Georges, par St Michel, Travaux et Mémoires 14, 2002, pp. 114-134'}},
        2567: {
            'inscription': '῾Ο ᾿Αρχάγγελος Μιχαὴλ ὁ Χωνιάτης. / Κύριε βοήθει ᾿Ιωάννῃ νωβελλισσίμῳ πρωτοβεστιαρίῳ καὶ μεγάλῳ δομεστίκῳ τῶν σχολῶν τῆς ᾿Ανατολῆς',
            'seals': {'3.2567.1085': 'Washington, Dumbarton Oaks Research Library and Collection: 47 series'}, 'sources': {
                'Laurent, Corpus II': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, II, L'administration centrale, Paris 1981 [reviewed by J. Nesbitt in Speculum 58 (1983), 771-772, and by W. Seibt in Jahrbuch der Österreichischen Byzantinistik 26 (1977), 325]",
                'Nesbitt - Oikonomides III': 'J. Nesbitt and N. Oikonomides, Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 3: West, Northwest and Central Asia Minor and the Orient, Washington D.C. 1996 [reviewed by W. Seibt in Byzantinische Zeitschrift 92 (1999), 538-54'}},
        2625: {
            'inscription': 'Κύριε βοήθει τῷ σῷ δούλῳ ᾿Ανδρονίκῳ πρωτοπροέδρῳ καὶ / στρατηγῷ τῶν Θρᾳκησίων τῷ ἀνθρώπῳ καίσαρος τοῦ Δούκα',
            'seals': {'5.2625.1111': 'Washington, Dumbarton Oaks Research Library and Collection: 58 series'}, 'sources': {
                'Nesbitt - Oikonomides III': 'J. Nesbitt and N. Oikonomides, Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 3: West, Northwest and Central Asia Minor and the Orient, Washington D.C. 1996 [reviewed by W. Seibt in Byzantinische Zeitschrift 92 (1999), 538-54'}},
        2799: {
            'inscription': 'Κύριε βοήθει τῷ σῷ δούλῳ Κωνσταντίνῳ πατρικίῳ / πραιποσίτῳ βεστάρχῃ καὶ δομεστίκῳ τῆ~ ᾿Ανατολῆ~',
            'seals': {'29.2799.1498': 'Cambridge, Mass., Fogg Art Museum'}, 'sources': {
                'Nesbitt - Oikonomides III': 'J. Nesbitt and N. Oikonomides, Catalogue of Byzantine Seals at Dumbarton Oaks and in the Fogg Museum of Art 3: West, Northwest and Central Asia Minor and the Orient, Washington D.C. 1996 [reviewed by W. Seibt in Byzantinische Zeitschrift 92 (1999), 538-54',
                'Wassiliou - Seibt BBÖ II ': 'A.-K. Wassiliou - W. Seibt, Die byzantinischen Bleisiegel in Österreich, 2. Teil: Zentral- und Provinzialverwaltung, Vienna 2003'}},
        2991: {
            'inscription': '῾Ο ἅγιος Νικόλαος. / ΙΒ Κύριε βοήθει τῷ σῷ δούλῳ ᾿Ιωάννῃ μοναχῷ καὶ ὀρφανοτρόφῳ',
            'seals': {'5.2991.861': 'Washington, Dumbarton Oaks Research Library and Collection: 58 series',
                      '29.2991.2967': 'Cambridge, Mass., Fogg Art Museum'},
            'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel',
                        'Nesbitt, Orphanotrophos': 'J. Nesbitt, "The orphanotrophos: some observations on the history of the office in the light of seals", Studies in Byzantine Sigillography 8 (2003), pp. 51-61'}},
        2992: {'inscription': '῾Ο ἅγιος Νικόλαος. / Κύριε βοήθει τῷ σῷ δούλῳ ᾿Ιωάννῃ μοναχῷ καὶ ὀρφανοτρόφῳ',
               'seals': {'1004.2992.0': 'Private collection: Basel, G. Zacos (largely dispersed)',
                         '20.2992.2127': 'St Petersburg, Hermitage'},
               'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel',
                           'Schlumberger, Sigillographie': "G. Schlumberger, Sigillographie de l'empire byzantin, Paris 1884",
                           'Schlumberger, Mélanges': "G. Schlumberger, Mélanges d'archéologie byzantine, Paris 1895 [= extract from Revue des Études grecques 2 (1889), 245-59; 4 (1891), 111-42 and 7 (1894), 319-336]",
                           'Stepanova, St Nicholas': 'E. Stepanova, "The image of St Nicholas on Byzantine seals", Studies in Byzantine Sigillography 9 (2006), pp. 185-195'}},
        2993: {'inscription': '῾Ο ἅγιος Νικόλαος. / Κύριε βοήθει τῷ σῷ δούλῳ ᾿Ιωάννῃ μοναχῷ καὶ ὀρφανοτρόφῳ',
               'seals': {'1004.2993.0': 'Private collection: Basel, G. Zacos (largely dispersed)'}, # was 5 seals?!
               'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel'}},
        2996: {'inscription': 'Κύριε βοήθει τῷ σῷ δούλῳ Κωνσταντίνῳ / τῷ λαμπροτάτῳ νωβελλισίμῳ',
               'seals': {'1004.2996.0': 'Private collection: Basel, G. Zacos (largely dispersed)'},
               'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel'}},
        3000: {'inscription': 'Θεοτόκε βοήθει τῷ σῷ δούλῳ / ´᾿Ιωάννῃ τῷ εὐτυχεστάτῳ καίσαρι',
               'seals': {'1004.3000.0': 'Private collection: Basel, G. Zacos (largely dispersed)', # was 4 seals?!
                         '39.3000.859': 'Paris, Bibliotheque nationale'},
               'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel'}},
        3001: {'inscription': 'Θεοτόκε βοήθει τῷ σῷ δούλῳ / ´᾿Ιωάννῃ τῷ εὐτυχεστάτῳ καίσαρι',
               'seals': {'1004.3001.0': 'Private collection: Basel, G. Zacos (largely dispersed)'},
               'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel'}},
        3002: {'inscription': 'Θεοτόκε βοήθει τῷ σῷ δούλῳ / ´᾿Ιωάννῃ τῷ εὐτυχεστάτῳ καίσαρι',
               'seals': {'1004.3002.0': 'Private collection: Basel, G. Zacos (largely dispersed)'},
               'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel'}},
        3003: {'inscription': 'Θεοτόκε βοήθει τῷ σῷ δούλῳ / ´᾿Ιωάννῃ τῷ εὐτυχεστάτῳ καίσαρι',
               'seals': {'1004.3003.0': 'Private collection: Basel, G. Zacos (largely dispersed)'},
               'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel'}},
        3004: {'inscription': 'Κύριε βοήθει τῷ σῷ δούλῳ ᾿Ιωάννῃ / βασιλεοπάτορι τῷ Δούκα',
               'seals': {'1004.3004.0': 'Private collection: Basel, G. Zacos (largely dispersed)', # was 2 seals?!
                         '1055.3004.43': 'Khoury collection (largely purchased around Antioch and in Lebanon)'},
               'sources': {'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel',
                           'Cheynet, Collection Khoury': 'J.-Cl. Cheynet, Sceaux de la collection Khoury, Revue Numismatique 159, 2003, 419-456'}},
        4143: {'inscription': 'Δι᾿ εὐλάβειαν οὐ φέρει θείους τύπους / ῎Αννης Κομνηνῆς ἡ σφραγὶς ἀλλὰ στίχους',
               'seals': {'82.4143.0': 'Mordtmann collection'},
               'sources': {'Mordtmann, Komnenon': 'A. Mordtmann, "Molybdoboulla ton Komnenon", EPhS 13, Suppl.',
                           'Schlumberger, Sigillographie': "G. Schlumberger, Sigillographie de l'empire byzantin, Paris 1884",
                           'Schlumberger, Inédits 5': 'G. Schlumberger, "Sceaux byzantins inédits (Cinquième série)", Revue Numismatique 9 (1905), 321-354, nos. 204-295. ',
                           'Laurent, Bulles métriques': 'V. Laurent, Les bulles métriques dans la sigillographie byzantine, Athens 1932 [repr. from Hellenika 4 (1931), 191-228 (nos. 1-110) and 321-360 (nos. 111-224); Hellenika 5 (1932), 137-174 (nos. 225-331) and 389-420 (nos. 331a-423); Hellenika 6 (1933), 81-'}},
        4941: {'inscription': '῾Ο ἅγιος Πέτρος. / ῾Ερβεβίῳ μαγίστρῳ βέστῃ καὶ στρατηλάτῃ τῆς ᾿Ανατολῆς τῷ Φραγγοπώλῳ',
               'seals': {'1044.4941.0': 'USA (private collection)'}, 'sources': {
                'Braunlin - Nesbitt, Selections': 'M. Braunlin and J. Nesbitt, "Selections from a private collection of Byzantine bullae", Byzantion 68 (1998), 157-182'}},
        5253: {'inscription': '῾Ο ἅγιος Γεώργιος. / Georgius regis Bodini filius',
               'seals': {'77.5253.69': 'Institute and Museum of Archaeology, Sofia',
                         '1013.5253.0': 'Bulgaria (private collection)'},
               'sources': {
                   'Jordanov, Corpus II': 'I. Jordanov, Corpus of Byzantine Seals from Bulgaria, vol. 2: Byzantine Seals with Family Names, Sofia 2006',
                   'Gerasimov, Georges': 'Gerasimov Th., "Un sceau en plomb de Georges, fils du roi Bodine". Studia Serdicensia 1, pp. 217-218',
                   'Jouroukova, Georgi Bodin': 'J. Jouroukova, "Nov oloven pečat na Georgi Bodin", Numizmatika 2, 8-13'}},
        6463: {'inscription': 'Κύριε βοήθει τῷ σῷ δούλῳ / ᾿Ιγνατίῳ μοναχῷ τῷ Καίσαρι',
               'auth': 'Jeffreys, Michael J.; Karágiṓrgou, ́Olga',
               'seals': {'3.6463.0': 'Washington, Dumbarton Oaks Research Library and Collection: 47 series'}, 'sources': {
                'Laurent, Corpus V.2': "V. Laurent, Le Corpus des Sceaux de l'empire byzantin, V.2, L'Église, Paris 1965 [reviewed by V. Grumel in Byzantinische Zeitschrift 61 (1968), 129; W. Seibt in Byzantinoslavica 35 (1974), 73-84]",
                'Seibt, review of Laurent, Corpus V ': 'W. Seibt, review of Laurent, Corpus V, Byzantinoslavica 35 (1974), 73-84',
                'Seibt, BBÖ I': 'W. Seibt, Die byzantinischen Bleisiegel in Österreich I. Teil, Kaiserhof, Vienna 1978 [reviewed by V. Šandrovskaja and I.V.Sokolova in Byzantinoslavica 41 (1980), 251-255]',
                'Zacos - Veglery': 'G. Zacos and A. Veglery, Byzantine Lead Seals I, Basel'}},
        6798: {'inscription': '[...] | Κύριε βοήθει / Κωνσταντίνῳ πατρικίῳ καὶ στρατηγῷ τῷ Διογένῃ',
               'seals': {'50.6798.15178': 'Regional Historical Museum, Shumen ', '35.6798.0': 'Istanbul Archaological Musum'},
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
            'passages': 99,
            'apassage': {'P3': "Introduction 1-13", 'P190': "Χρονογραφία πονηθεῖσα ... ἱστοροῦσα τὰς πράξεις τῶν βασιλέων, ... καὶ ἕως τῆς ἀναρρήσεως Κωνσταντίνου τοῦ Δούκα"}
        },
        # Source with narrative factoid
        'praktikon_adam': {
            'work': 'Praktikon of Adam notary',
            'edition': 'Βυζαντινὰ ἔγγραφα τῆς μονῆς Πάτμου 1. Αὐτοκρατορικά, 2. Δημοσίων λειτουργῶν, Athens 1980, 2.7-20',
            'author': 'Adam, domestikos of the sekreton of the euageis oikoi',
            'authority': 'Papacostas, Tassos',
            'editor': 'Vranoúsīs, Léandros I.; Nystazopoúlou-Pelekídou, María',
            'pbwed': 'Papacostas, Tassos',
            'passages': 3,
            'apassage': {'P3': "2.20.320-323", 'P190': "πιστωθὲν παρ ἐμοῦ ἐπεδόθη"}
        },

        # Source with author but no factoid
        'kecharitomene_typikon': {
            'work': 'Kecharitomene typikon',
            'edition': '“Le typikon de la Théotokos Kécharitôménè", Revue des Études Byzantines, 43 (1985), 5-165',
            'author': 'Eirene Doukaina, wife of Alexios I',
            'authority': 'Gautier, Paul',
            'editor': 'Gautier, Paul',
            'pbwed': 'Jeffreys, Michael J.',
            'passages': 5,
            'apassage': {'P3': "p. 137 l. 2087"}
        },

        # Source with author outside of PBW
        'yahya': {
            'work': 'Ta’rikh Yahya ibn Said al-Antaki (The History of Yahya ibn Sa’id of Antioch)',
            'edition': 'Histoire de Yahya ibn Sa’id d’Antioche, Patrologia Orientalis 47.4 (no.212), Turnhout 1997',
            'author': 'Yaḥyā ibn Saʻīd al-Anṭākī',
            'authority': 'Kračkovskij, Ignati; Micheau, Françoise; Troupeau, Gérard',
            'editor': 'Kračkovskij, Ignati; Micheau, Françoise; Troupeau, Gérard',
            'pbwed': 'Papacostas, Tassos; Osti, Letizia; Munt, Harry',
            'passages': 5,
            'apassage': {'P3': 'Histoire de Yahya ibn Sa’id d’Antioche, Patrologia Orientalis 47.4 (no.212), Turnhout 1997'}
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
        },

        # Source that we split into multiple sub-sources: Kleinchroniken #5
        # The work has no author but it does have an edition
        'kleinchroniken_5': {
            'work': 'Short Chronicle #5',
            'edition': 'Die byzantinischen Kleinchroniken, 3 vols., Vienna 1975-1979, 54-56',
            'editor': 'Schreiner, Peter',
            'passages': 1,
            'pbwed': 'Papacostas, Tassos'
        }

    }

    # Helper functions
    def check_class(self, uri, ocl):
        """Helper to check that a URI is defined as the given class"""
        c = self.constants
        rcl = c.graph.value(uri, RDF.type)
        self.assertEqual(c.entitylabels.get(ocl, c.predicates.get(ocl)), rcl,
                        f"Class of {uri} should be {ocl}")

    def get_external_id(self, uri):
        """Return the content of the E42 identifier set via an E15 for the given entity."""
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
        return obj


    # Tests begin here
    def setUp(self):
        # Test against the GraphDB instance in config.py
        store = sparqlstore.SPARQLStore(config.graphuri, method='POST', auth=(config.graphuser, config.graphpw))
        # Make / retrieve the global nodes and self.constants
        self.constants = PBWstarConstants.PBWstarConstants(store=store, graph=config.graphname)

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
                self.td_people[p]['uri'] = puri
            except UniquenessError:
                self.fail("ID should lead to unique person")

    # TODO add extra assertions for the eunuchs and K62
    def test_gender(self):
        """Test that each person has the gender assignments we expect"""
        c = self.constants
        p_uri_list = '\n'.join([f"{d['uri'].n3()}" for d in self.td_people.values()])
        sparql = f"""
select ?p_uri ?gender where {{
    VALUES ?p_uri {{
        {p_uri_list}
    }}
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
        genders = defaultdict(list)
        for row in res:
            genders[row['p_uri']].append(row['gender'])
        # Check that they are correct
        for person, pinfo in self.td_people.items():
            p_uri = pinfo['uri']
            self.assertIsNotNone(genders.get(p_uri))
            self.assertListEqual(genders[p_uri], [Literal(x) for x in pinfo['gender']],
                                 f"Test gender for {person}")

    # The identifier is the name as PBW has it in the original language.
    def test_identifier(self):
        """Test that each person has the main appellation given in the PBW database"""
        c = self.constants
        p_uri_list = '\n'.join([f"{d['uri'].n3()}" for d in self.td_people.values()])
        sparql = f"""
select ?p_uri ?mainid where {{
    VALUES ?p_uri {{
        {p_uri_list}
    }}
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
            self.assertIsNotNone(identifiers.get(p_uri), f"Identifier found for {person}")
            self.assertEqual(Literal(pinfo['identifier']), identifiers[p_uri], f"Test identifier for {person}")
            # Check that the descriptors are correct too
            self.assertEqual(Literal(pinfo['descriptor']), self.get_object(p_uri, 'P3'))


    def test_appellation(self):
        """Test that each person has the second or alternative names assigned, as sourced assertions"""
        c = self.constants
        # Prod has wrong data; allow this for the time being
        if config.dbmode == 'prod':
            self.td_people['Konstantinos 64']['altname']['Θεοδώρῳ']['source'] = \
            '“Βυζαντινὰ χρυσόβουλλα καὶ πιττάκια”, Ἐκκλησιαστικὴ Ἀλήθεια 4 (1883-84) 405-406'
        for person, pinfo in self.td_people.items():
            names = dict()
            if 'secondname' in pinfo:
                names.update(pinfo['secondname'])
            if 'altname' in pinfo:
                names.update(pinfo['altname'])
            if len(names) > 0:
                sparql = f"""
select ?appellation ?src where {{
    ?a1 a {c.get_assertion_for_predicate('P1')} ;
        {c.star_subject} {pinfo['uri'].n3()} ;
        {c.star_object} ?appellation ;
        {c.star_auth} ?authority ;
        {c.star_based} ?passage .
    OPTIONAL {{
        ?a2 a {c.get_assertion_for_predicate('R15')} ;
            {c.star_subject} [a {c.get_label('F3P')} ; {c.get_label('P3')} ?src ] ;
            {c.star_object} ?passage .
    }}
}}"""
                res = c.graph.query(sparql)
                found_appels = dict()
                for row in res:
                    appel = row['appellation']
                    # The appellation should be an E41, have an English and an original language
                    self.check_class(appel, 'E41')
                    values = [v for v in c.graph.objects(appel, c.predicates['P190'])]
                    self.assertEqual(2, len(values))
                    langs = {x.language: x.toPython() for x in values}
                    # Fortunately for us all the original language appellations are Greek
                    self.assertTrue('en' in langs)
                    self.assertTrue('grc' in langs)
                    # Add the count and the source text to the found_appels dict. If the name is
                    # already there (i.e. there are multiple assertions) remove the source text.
                    test_name = langs['grc']
                    if test_name in found_appels:
                        found_appels[test_name]['count'] += 1
                        if 'source' in found_appels[test_name]:
                            del found_appels[test_name]['source']
                    else:
                        found_appels[test_name] = {'count': 1, 'source': row['src'].toPython()}
                # Check that we found all of them
                self.assertDictEqual(names, found_appels)

    def test_death(self):
        """Test that each person has at most one death event, since they all only died once. Also
        test that the assertions look correct"""
        c = self.constants
        # Look for all death events for the people in our test list
        deathevents = dict()
        p_uri_list = '\n'.join([f"{d['uri'].n3()}" for d in self.td_people.values()])
        sparql = f"""
select distinct ?person ?de where {{
    VALUES ?person {{
        {p_uri_list}
    }}
    ?a {c.star_object} ?person ;
        {c.star_subject} ?de ;
        a {c.get_assertion_for_predicate('P100')} .
}}"""
        res = c.graph.query(sparql)
        for row in res:
            person = row['person']
            devent = row['de']
            self.assertIsNone(deathevents.get(person), f"{self.get_external_id(person)} should not die twice")
            deathevents[person] = devent

        for person, pinfo in self.td_people.items():
            # Check if the person should have a death event.
            devent = deathevents.get(pinfo['uri'])
            ddescpred = c.get_assertion_for_predicate('P3')
            ddatepred = c.get_assertion_for_predicate('P4')
            if 'death' not in pinfo:
                # Make allowance for a different death event having been added to the production store by WissKI
                if devent is not None:
                    self.assertIsNotNone(re.search(r'/[0-9a-f]{13}$', devent.toPython()))
                continue
            else:
                self.assertIsNotNone(devent)
                # See if we have the expected info about the death event in question.
                # Each event should have N description assertions in English, each with a P3 attribute.
                sparql = f"""
SELECT ?desc WHERE {{
    ?a {c.star_subject} {devent.n3()} ;
        a {ddescpred} ;
        {c.star_object} ?desc .
    FILTER(LANGMATCHES(LANG(?desc), 'en'))
}}"""
                res = c.graph.query(sparql)
                self.assertEqual(pinfo['death']['count'], count_result(res), "Death description count for %s" % person)

                # and M date assertions.
                sparql = f"""
select ?a where {{
    ?a {c.star_subject} {devent.n3()} ;
        a {ddatepred} ;
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
        {c.star_subject} [a {c.get_label('E74E')} ; {c.get_label('P1')} ?eth ] ;
        {c.star_object} {pinfo['uri'].n3()} .
}} group by ?eth"""
                res = c.graph.query(sparql)
                rowct = 0
                for row in res:
                    rowct += 1
                    ethlabel = row['eth']
                    self.assertTrue(ethlabel in [Literal(x) for x in eths.keys()])
                    self.assertEqual(eths[ethlabel.toPython()], row['act'].toPython())
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
        {c.star_object} {pinfo['uri'].n3()} ;
        {c.star_auth} ?auth .
    ?a2 a {c.get_assertion_for_predicate('SP35')} ;
        {c.star_subject} ?relaff ;
        {c.star_object} [a {c.get_label('C24')} ; {c.get_label('P1')} ?rel ] ;
        {c.star_auth} ?auth .
}}"""
                res = c.graph.query(sparql)
                # We are cheating by knowing that no test person has more than one religion specified
                rows = [x for x in res]
                self.assertEqual(1, len(rows))
                found_rel = rows[0]['rel']
                self.assertTrue(found_rel in [Literal(x) for x in rels.keys()])
                authority = self.get_external_id(rows[0]['auth'])
                self.assertIn(authority, [Literal(x) for x in rels[found_rel.toPython()]])

    def test_occupation(self):
        """Test that occupations / non-legal designations are set correctly"""
        c = self.constants
        for person, pinfo in self.td_people.items():
            # Check that the occupation assertions were created
            if 'occupation' in pinfo:
                occs = {Literal(k): v for k, v in pinfo['occupation'].items()}
                sparql = f"""
select ?occ where {{
    ?a a {c.get_assertion_for_predicate('SP13')} ;
        {c.star_subject} ?pocc ;
        {c.star_object} {pinfo['uri'].n3()} .
    ?a2 a {c.get_assertion_for_predicate('SP14')} ;
        {c.star_subject} ?pocc ;
        {c.star_object} [a {c.get_label('C2')} ; {c.get_label('P1')} ?occ ] .
    ?pocc a {c.get_label('C1')} .
}}"""
                res = c.graph.query(sparql)
                ctr = Counter([row['occ'] for row in res])
                self.assertDictEqual(occs, ctr, "Test occupations for %s" % person)

    def test_legalrole(self):
        """Test that legal designations are set correctly"""
        c = self.constants
        # Override some of the values if we are testing prod (see comments in people hash).
        if config.dbmode == 'prod':
            self.td_people['Gagik 101']['legalrole']['Archon'] = 2
            self.td_people['Ioannes 102']['legalrole']['Metropolitan'] = 13
        for person, pinfo in self.td_people.items():
            # Check that the occupation assertions were created
            if 'legalrole' in pinfo:
                roles = {Literal(k): v for k, v in pinfo['legalrole'].items()}
                sparql = f"""
select ?role where {{
    ?a a {c.get_assertion_for_predicate('SP26')} ;
        {c.star_subject} ?prole ;
        {c.star_object} {pinfo['uri'].n3()} .
    ?a2 a {c.get_assertion_for_predicate('SP33')} ;
        {c.star_subject} ?prole ;
        {c.star_object} [a {c.get_label('C12')} ; {c.get_label('P1')} ?role ] .
    ?prole a {c.get_label('C13')} .
}}"""
                res = c.graph.query(sparql)
                ctr = Counter([row['role'] for row in res])
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
        {c.star_subject} {pinfo['uri'].n3()} ;
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
                self.assertEqual(Literal(pinfo['language']), rows[0]['kh'], "Test language for %s" % person)

    def test_kinship(self):
        """Test the kinship assertions for one of our well-connected people"""
        c = self.constants
        for person, pinfo in self.td_people.items():
            if 'kinship' in pinfo:
                sparql = f"""
select distinct ?kin ?kintype where {{
    ?a {c.star_object} {pinfo['uri'].n3()} ;
        a {c.get_assertion_for_predicate('SP17')};
        {c.star_subject} ?kg .
    ?a2 {c.star_subject} ?kg ;
        a {c.get_assertion_for_predicate('SP18')} ;
        {c.star_object} ?kin .
    ?a3 {c.star_subject} ?kg  ;
        a {c.get_assertion_for_predicate('SP16')} ;
        {c.star_object} [ a {c.get_label('C4')} ; {c.get_label('P1')} ?kintype ] .
}}"""
                res = c.graph.query(sparql)
                expectedkin = {Literal(k): [Literal(x) for x in v] for k, v in pinfo['kinship'].items()}
                foundkin = defaultdict(list)
                for row in res:
                    k = row['kintype']
                    foundkin[k].append(self.get_external_id(row['kin']))
                for k in foundkin:
                    foundkin[k] = sorted(foundkin[k])
                self.assertDictEqual(expectedkin, foundkin, "Kinship links for %s" % person)

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
        {c.star_object} {pinfo['uri'].n3()} ;
        {c.star_auth} ?author ;
        {c.star_based} ?srcuri .
    ?idass a {c.get_label('E15')} ;
        {c.star_subject} ?author ;
        {c.get_label('P37')} [ a {c.get_label('E42')} ; {c.get_label('P190')} ?authorid ] ;
        {c.star_auth} {c.pbw_agent.n3()} .
    ?a2 a {c.get_assertion_for_predicate('R15')} ;
        {c.star_subject} ?edition ;
        {c.star_object} ?srcuri .
    ?a3 a {c.get_assertion_for_predicate('R5')} ;
        {c.star_subject} ?edition ;
        {c.star_object} ?text .
    ?a4 a {c.get_assertion_for_predicate('R17')} ;
        {c.star_subject} ?creation ;
        {c.star_object} ?text .
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
                    author = row['authorid']
                    src = row['src']
                    self.assertTrue(poss in [Literal(x) for x in pinfo['possession']],
                                    "Test possession is correct for %s" % person)
                    (agent, reference) = pinfo['possession'][poss.toPython()]
                    self.assertEqual(Literal(agent), author, "Test possession authority is set for %s" % person)
                    self.assertEqual(Literal(reference), src, "Test possession source ref is set for %s" % person)
                self.assertEqual(rowct, len(pinfo['possession'].keys()),
                                 "Test %s has the right number of possessions" % person)

    def test_boulloterions(self):
        """For each boulloterion, check that it exists only once and has only one inscription."""
        c = self.constants
        found = set()
        boulloterions_to_test = '\n'.join([Literal(str(x)).n3() for x in self.td_boulloterions.keys()])
        sparql = f"""
SELECT ?boul ?inscr ?src ?auth WHERE {{
    VALUES ?boulid {{
        {boulloterions_to_test}
    }}
    ?ida {c.get_label('P37')} [ a {c.get_label('E42')} ; {c.get_label('P190')} ?boulid ] ;
        {c.star_subject} ?boul ;
        a {c.get_label('E15')} .
    ?a {c.star_subject} ?boul ;
        a {c.get_assertion_for_predicate('P128')} ;
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
            boulid = int(self.get_external_id(row['boul']))
            # We should be expecting this boulloterion
            self.assertIn(boulid, self.td_boulloterions, "Boulloterion %d should exist" % boulid)
            boulinfo = self.td_boulloterions[boulid]

            # but we should not have seen it yet
            self.assertNotIn(boulid, found, "Boulloterion %d should not be duplicated" % boulid)
            found.add(boulid)
            # The boulloterion should have a correct inscription (we only record the Greek)
            inscr = [v for v in c.graph.objects(row['inscr'], c.predicates['P190'])]
            self.assertEqual(1, len(inscr))
            self.assertEqual(Literal(boulinfo['inscription']), inscr[0],
                             f"boulloterion {boulid} inscription should match")
            # self.assertEqual('grc', inscr[0].language) ## TODO not yet
            # The boulloterion should have the correct named authority or authorities

            auth = self.get_object(row['auth'], 'P3')
            # Alphabetize the authority string we got. This also de-Literals it.
            alph_auth = '; '.join(sorted(auth.split('; ')))
            real_auth = boulinfo.get('auth', 'Jeffreys, Michael J.')
            self.assertEqual(real_auth, alph_auth, f"Authority for boulloterion {boulid} correctly set")
            # Separate query to check the boulloterion sources on the inscription assertion
            if len(boulinfo['sources']) > 1:
                # Source should be a Bibliography which contains a set of works
                self.check_class(row['src'], 'E73B')
                # Get the texts of this bibliography
                sources = c.graph.objects(row['src'], c.predicates['P165'])
            else:
                # Source should be a single text
                self.check_class(row['src'], 'F3P')
                sources = [row['src']]
            for source in sources:
                skey = self.get_object(source, 'P1')
                self.assertIn(skey, [Literal(x) for x in boulinfo['sources']],
                              f"Source {skey} should be there for boulloterion {boulid}")
                self.assertEqual(Literal(boulinfo['sources'][skey.toPython()]), self.get_object(source, 'P3'))

            # Separate query to check the boulloterion seals and their respective assertions. A seal was
            # produced by a boulloterion and belongs to a collection according to the same authority as above,
            # and these assertions have no explicit source.
            sealq = f"""
select ?seal ?coll where {{
    ?sealass {c.star_subject} {row['boul'].n3()} ;
        a {c.get_assertion_for_predicate('L1')} ;
        {c.star_object} ?seal ;
        {c.star_auth} ?auth .
    ?collass {c.star_object} ?seal ;
        {c.star_subject} ?coll;
        a {c.get_assertion_for_predicate('P46')} ;
        {c.star_auth} ?auth .
}}"""
            r3 = c.graph.query(sealq)
            sealcolls = dict()
            for row3 in r3:
                # Get the seal dict value and check its type
                self.check_class(row3['seal'], 'E22S')
                sealid = self.get_object(row3['seal'], 'P3')
                # Check that the seal hasn't been seen yet. TODO can't do this until seal IDs are unique
                # self.assertIsNone(sealcolls.get(sealid))
                # Add the seal and its collection
                collid = self.get_object(row3['coll'], 'P1')
                sealcolls[sealid] = collid
            # Check that we found all the seals
            expected_colls = {Literal(k): Literal(v) for k, v in boulinfo['seals'].items()}
            self.assertDictEqual(expected_colls, sealcolls)

    def test_text_sources(self):
        """Spot-check different textual sources and make sure they are set up correctly"""
        c = self.constants
        # Remove the source that is wrong in prod
        if config.dbmode == 'prod':
            del self.spot_sources['kleinchroniken_5']
        for s, sinfo in self.spot_sources.items():
            if 'author' in sinfo:
                # The Chronographia: work was created by author according to author based on passage
                # The typikon: work was created by author according to editor
                # The praktikon: work was created by author according to PBW editor
                # Yahya: work was created by author according to editor based on *edition*
                # All: work has edition according to editor based on edition
                sparql = f"""
select ?work ?author ?authority ?editor ?edition ?passage where {{
    ?work a {c.get_label('F2T')} ;
        {c.get_label('P3')} {Literal(sinfo.get('work')).n3()} .
    ?wc1 a {c.get_assertion_for_predicate('R17')} ;
        {c.star_subject} ?wc ;
        {c.star_object} ?work ;
        {c.star_auth} ?authority ;
        {c.star_based} ?passage .
    ?wc2 a {c.get_assertion_for_predicate('P14')} ;
        {c.star_subject} ?wc ;
        {c.star_object} ?author ;
        {c.star_auth} ?authority ;
        {c.star_based} ?passage .
    ?wed a {c.get_assertion_for_predicate('R5')} ;
        {c.star_subject} ?edition ;
        {c.star_object} ?work ;
        {c.star_auth} ?editor ;
        {c.star_based} ?edition .
}}"""
            elif 'work' in sinfo:
                # The 1094 synod: work has edition according to editor based on edition
                sparql = f"""
select ?work ?editor ?edition where {{
    ?work a {c.get_label('F2T')} ;
        {c.get_label('P3')} {Literal(sinfo.get('work')).n3()} .
    ?wed a {c.get_assertion_for_predicate('R5')} ;
        {c.star_subject} ?edition ;
        {c.star_object} ?work ;
        {c.star_auth} ?editor ;
        {c.star_based} ?edition .
        
}}"""
            else:
                # The Christos Philanthropos doc: edition was created by editors according to editors based on edition.
                # We have to match the group of editors. Also, we never saved the identifier key in the DB.
                sparql = f"""
select ?editor ?edition where {{
    ?edition a {c.get_label('F3P')} ;
        {c.get_label('P3')} {Literal(sinfo.get('edition')).n3()} .
    ?ec1 a {c.get_assertion_for_predicate('R24')} ;
        {c.star_subject} ?ec ;
        {c.star_object} ?edition ;
        {c.star_auth} ?editor ;
        {c.star_based} ?edition .
}}
"""
            result = [row for row in c.graph.query(sparql)]
            # Check that there is one result
            self.assertEqual(1, len(result))
            # Check the types of the various entities if we know them unambiguously
            data = result[0]
            self.check_class(data['edition'], 'F3P')
            # Check that the passages in support of authorship are set up correctly
            if 'passage' in data.asdict():
                self.check_class(data['passage'], 'F3P' if s == 'yahya' else 'E33')
                found_struct = {'P3': self.get_object(data['passage'], 'P3')}
                if 'P190' in sinfo['apassage']:
                    found_struct['P190'] = self.get_object(data['passage'], 'P190')
                exp_struct = {k: Literal(v) for k,v in sinfo['apassage'].items()}
                self.assertDictEqual(exp_struct, found_struct)
            if 'wc' in data.asdict():
                self.check_class(data['wc'], 'F28')
            if 'work' in data.asdict():
                self.check_class(data['work'], 'F2T')
                self.assertEqual(Literal(sinfo.get('work')), self.get_object(data['work'], 'P3'))
            else:
                self.assertIsNone(data.get('work'), f"Work should not be present for {s}")
            # Check that the information corresponds to what we expect
            if 'author' in sinfo:
                self.check_class(data['author'], 'E21')
                self.assertEqual(Literal(sinfo.get('author')), self.get_object(data['author'], 'P3'))
            if 'authority' in sinfo:
                self.assertEqual(Literal(sinfo.get('authority')), self.get_object(data['authority'], 'P3'))
            self.assertEqual(Literal(sinfo.get('edition')), self.get_object(data['edition'], 'P3'))
            self.assertEqual(Literal(sinfo.get('editor')), self.get_object(data['editor'], 'P3'))

            # Now check that the passages are present & correct and have the right authority
            spq = f"""
select ?pbwed (count(?passage) as ?pct) where {{ 
    ?psa a {c.get_assertion_for_predicate('R15')} ;
        {c.star_subject} {data['edition'].n3()} ;
        {c.star_object} ?passage ;
        {c.star_auth} ?pbwed .
}} group by ?pbwed"""
            passages = c.graph.query(spq)
            # There should only be one PBW editor / editor group per edition
            self.assertEqual(1, len(passages))
            for row in passages:
                pbwed = self.get_object(row['pbwed'], 'P3')
                self.assertEqual(Literal(sinfo.get('pbwed')), pbwed)
                # When we test against production we can't guarantee an exact number, but there should be
                # at least the number from the test database.
                self.assertGreaterEqual(row['pct'].toPython(), sinfo.get('passages'))

    def test_db_entry(self):
        """All the assertions in the database should be attached to DB records, linked to the single entry
        that created them."""
        if config.dbmode == 'prod':
            self.skipTest("skipping DB entry test for production")

        c = self.constants
        # How many assertions do we have? These are the things that have P140 subjects
        total_assertions = len([x for x in c.graph.subjects(c.predicates['P140'])])
        # No assertion should have more than one P140
        total_unique = len([x for x in c.graph.subjects(c.predicates['P140'], unique=True)])
        self.assertEqual(total_assertions, total_unique)

        # Find the assertions that are connected to a database record. There should in theory only
        # be one record.
        sparql = f"""
select (count(?a) as ?numass) ?record ?tstamp ?me where {{
    ?a {c.star_subject} ?somesubj .
    ?record a {c.get_label('D10')} ;
        {c.get_label('L11')} ?a ;
        {c.get_label('P14')} ?me ;
        {c.get_label('P4')} ?tstamp .
}} group by ?record ?tstamp ?me"""
        linked = [x for x in c.graph.query(sparql)]
        self.assertEqual(1, len(linked))
        result = linked[0]

        self.assertEqual(result['numass'].toPython(), total_assertions)
        self.assertIsNotNone(result['tstamp'])
        self.assertEqual(Literal('Andrews, Tara Lee'), self.get_object(result['me'], 'P3'))

    @unittest.skip("for now")
    def test_repeat(self):
        """If we have a DB connection and re-run the import, there should be zero new assertions
        and the graph should not change."""
        if config.dbmode == 'prod':
            self.skipTest("skipping data regeneration test for production")

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
            self.skipTest("Cannot run data regeneration test without a MySQL connection.")
        # Check the length of the resulting graph
        self.assertEqual(graph_size, len(gimport.g), "Regeneration of graph results in no change to size of graph")
        # Check that the triples are all identical
        for triple in c.graph:
            self.assertTrue(triple in gimport.g, f"Triple {triple} exists in both graphs")


if __name__ == '__main__':
    unittest.main()
