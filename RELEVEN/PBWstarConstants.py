import re
# This package contains a bunch of information curated from the PBW website about authority, authorship
# and so forth. It is a huge laundry list of data and some initialiser and accessor functions for it; the
# class requires a graph driver in order to do the initialisation.


class PBWstarConstants:
    """A class to deal with all of our constants, where the data is nicely encapsulated"""

    def __init__(self, graphdriver):
        self.graphdriver = graphdriver

        # These are the modern scholars who put the source information into PBW records
        mj = {'identifier': 'Jeffreys, Michael J.', 'viaf': '73866641'}
        # We need Michael and Tara on the outside
        self.mj = mj
        tp = {'identifier': 'Papacostas, Tassos', 'viaf': '316793603'}
        ta = {'identifier': 'Andrews, Tara Lee', 'viaf': '316505144'}
        self.ta = ta
        jr = {'identifier': 'Ryder, Judith R.', 'viaf': '51999624'}
        mw = {'identifier': 'Whitby, Mary', 'viaf': '34477027'}
        wa = {'identifier': 'Amin, Wahid M.', 'viaf': '213149617100303751518'}
        bs = {'identifier': 'Soravia, Bruna', 'viaf': '69252167'}
        hm = {'identifier': 'Munt, Harry', 'viaf': '78568758'}
        lo = {'identifier': 'Osti, Letizia', 'viaf': '236145542536996640148'}
        cr = {'identifier': 'Roueché, Charlotte', 'viaf': '44335536'}
        ok = {'identifier': 'Karágiṓrgou, ́Olga', 'viaf': '253347413'}

        self.aggregate_sources = {
            # Some of our sources are actually multiple works. Here is the key to disambiguate them: either
            # a list of starting strings or a map of regexp -> starting string.
            'Eustathios Romaios': {r'Peira': 'Peira',
                                   r'Ralles-Potles V, \d{2}\D': 'RPA',
                                   r'Ralles-Potles V, \d{3}\D': 'RPB',
                                   r'Schminck': 'Schminck'},
            'Nea Mone,': ['Gedeon', 'Miklosich-Müller'],
            'Psellos': {r'Eirene': 'Eirene',
                        r'Letters \(K - D\) (\d+)': 'K-D ##',
                        r'Letters \(Sathas\) (\d+)': 'Sathas ##',
                        r'Robert': 'Robert'}
        }

        self.sourcelist = {
            'Albert of Aachen': {'author': ['Albert', 26101], 'factoid': 432193, 'authority': [mj],
                                 'work': 'Historia Ierosolimitana',
                                 'expression': 'Historia Ierosolimitana, History of the Journey to Jerusalem, '
                                               'Oxford 2007',
                                 'editor': [{'identifier': 'Edgington, Susan B.', 'viaf': '93497875'}]},
            'Alexios Stoudites': {'author': ['Alexios', 11], 'authority': [tp],
                                  'expression': 'Erlasse des Patriarchen von Konstantinopel Alexios Studites, '
                                                'Kiel 1911',
                                  'editor': [{'identifier': 'Ficker, Gerhard', 'viaf': '37668465'}]},
            'Anna Komnene': {'author': ['Anna', 62], 'factoid': 396886, 'work': 'Alexias', 'authority': [mj],
                             'expression': 'D.R. Reinsch and A. Kambylis, Annae Comnenae Alexias, '
                                           'Corpus fontium historiae Byzantinae 40/1, Berlin – New York 2001',
                             'editor': [{'identifier': 'Reinsch, Diether Roderich', 'viaf': '46774901'},
                                        {'identifier': 'Kambylis, Athanasios', 'viaf': '51705268'}]},
            'Annales Barenses': {'authority': [mj]},
            'Anonymus Barensis': {'authority': [mj]},
            'Aristakes': {'author': ['Aristakes', 101], 'factoid': 375021, 'authority': [tp, ta],
                          'work': 'Patmutʿiwn',
                          'expression': 'Récit des malheurs de la nation arménienne, Brussels 1973',
                          'editor': [{'identifier': 'Canard, Marius', 'viaf': '41837536'},
                                     {'identifier': 'Pērpērean, Hayk', 'viaf': '34489376'}]},
            'Attaleiates: Diataxis': {'author': ['Michael', 202], 'factoid': 254236, 'authority': [tp]},
            'Attaleiates: History': {'author': ['Michael', 202], 'factoid': 235092, 'authority': [tp],
                                     'work': 'Attaliatae Historia',
                                     'expression': 'Michaelis Attaliatae Historia, Corpus scriptorum historiae '
                                                   'Byzantinae 3, Bonn 1853',
                                     'editor': [{'identifier': 'Brunet de Presle, Wladimir', 'viaf': '203410427'},
                                                {'identifier': 'Bekker, Immanuel', 'viaf': '44387193'}]},
            'Attaleiates: History (Spanish)': {'author': ['Michael', 202], 'factoid': 235092, 'authority': [tp],
                                               'work': 'Attaliatae Historia',
                                               'expression': 'Miguel Attaliates Historia, Nueva Roma. Bibliotheca '
                                                             'Graeca et Latina Aevi Posterioris 15, Madrid 2002',
                                               'editor': [
                                                   {'identifier': 'Pérez Martín, Inmaculada', 'viaf': '12526036'}]},
            'Basilakios, Orationes et epistulae': {'author': ['Nikephoros', 17003], 'factoid': 437070, 'authority': []},
            'Basileios of Calabria to Nikolaos III': {'author': ['Basileios', 254], 'authority': [mj]},
            'Boilas': {'author': ['Eustathios', 105], 'factoid': 226498, 'authority': [tp],
                       'work': 'Testament of Boilas',
                       'expression': '"Le testament d’Eustathios Boïlas (Avril 1059)", '
                                     'Cinq études sur le XIe siècle byzantin, Paris (1977) 15-63',
                       'editor': [{'identifier': 'Lemerle, Paul', 'viaf': '97996834'}]},
            'Bryennios': {'author': ['Nikephoros', 117], 'factoid': 237218, 'authority': [tp],
                          'work': 'Hyle Historias',
                          'expression': 'Nicéphore Bryennios: Histoire. Introduction, texte, traduction et notes, '
                                        'Brussels 1975',
                          'editor': [{'identifier': 'Gautier, Paul', 'viaf': '231073465'}]},
            'Cheynet, Antioche et Tarse': {'authority': [ok]},
            'Christophoros of Mitylene': {'author': ['Christophoros', 13102], 'authority': [mj], 'ref': 'p. 1',
                                          'work': 'Collected Poems',
                                          'expression': 'Die Gedichte des Christophoros Mitylenaios, Leipzig 1903',
                                          'editor': [{'identifier': 'Kurtz, Eduard', 'viaf': '59840374'}]},
            # opera
            'Christos Philanthropos, note': {'authority': [mj],
                                             'expression': '“Commémoraisons des Comnènes dans le typikon liturgique du '
                                                           'monastère du Christ Philanthrope (ms. Panaghia '
                                                           'Kamariotissa 29)”, Revue des études Byzantines 63 (2005), '
                                                           '41-69',
                                             'editor': [{'identifier': 'Rízou-Kouroúpou, Matoúla', 'viaf': '102334712'},
                                                        {'identifier': 'Vannier, Jean-François', 'viaf': '219220697'}]},
            'Chrysobull of 1079': {'author': ['Nikephoros', 3], 'factoid': 224945, 'authority': [tp],
                                   'expression': '“Un chrysobulle de Nicéphore Botaneiatès à souscription synodale”, '
                                                 'Byzantion 29 (1959) 29-41',
                                   'editor': [{'identifier': 'Gouillard, Jean', 'viaf': '88739139'}]},
            'Clement III to Basileios of Calabria': {'author': ['Klemes', 23], 'factoid': 444557, 'authority': [jr]},
            'Codice Diplomatico Barese IV': {'authority': [mw]},
            'Council of 1147': {'authority': [mj], 'work': 'Council of 1147',
                                'expression': 'Σύνταγμα τῶν θείων καὶ ἱερῶν κανόνων V, Athens 1852-1859: pp. 307ff.',
                                'editor': [{'identifier': 'Rállēs, Geṓrgios A.', 'viaf': '10200332'},
                                           {'identifier': 'Potlēs, Michaēl', 'viaf': '66864785'}]},
            'Council of 1157': {'authority': [mj]},
            'Dionysiou': {'authority': [tp]},
            'Docheiariou': {'authority': [tp]},
            'Documents d\'ecclesiologie ': {'authority': [jr]},
            'Domenico of Grado': {'author': ['Dominikos', 101], 'factoid': 434887, 'authority': [jr]},
            'Droit matrimonial': {'authority': [jr]},
            'Edict on Clergy Reform': {'author': ['Alexios', 1], 'factoid': 444531, 'authority': [mj]},
            'Edict on clergy reform': {'author': ['Alexios', 1], 'factoid': 444531, 'authority': [mj]},
            'Eleousa: Acts': {'authority': [tp]},
            'Eleousa: Typikon': {'authority': [mw]},
            'Esphigmenou': {'authority': [tp]},
            'Eustathios Romaios Peira': {'author': ['Eustathios', 61, 'Anonymus', 12144], 'factoid': 374065,
                                         'authority': [mj], 'work': 'Peira',
                                         'expression': '“Πεῖρα Εὐσταθίου τοῦ Ῥωμαίου”, Jus Graecoromanum vol.4, '
                                                       'Athens 1931',
                                         'editor': [{'identifier': 'Zepos, Panagiōtēs', 'viaf': '111939078'}]},
            'Eustathios Romaios RPA': {'author': ['Eustathios', 61], 'factoid': 374066, 'authority': [mj],
                                       'work': 'Peri disexadelphōn / '
                                               'Hypomnēma peri duō exadelphōn labontōn duo exadelphas',
                                       'expression': 'Σύνταγμα τῶν θείων καὶ ἱερῶν κανόνων, vol. 5, '
                                                     'Athens 1855, 32-36',
                                       'editor': [{'identifier': 'Rállēs, Geṓrgios A.', 'viaf': '10200332'},
                                                  {'identifier': 'Potlēs, Michaēl', 'viaf': '66864785'}]},
            'Eustathios Romaios RPB': {'author': ['Eustathios', 61], 'factoid': 374066, 'authority': [mj],
                                       'work': 'Peri disexadelphōn / '
                                               'Hypomnēma peri duō exadelphōn labontōn duo exadelphas',
                                       'expression': 'Σύνταγμα τῶν θείων καὶ ἱερῶν κανόνων, vol. 5, '
                                                     'Athens 1855, 341-353',
                                       'editor': [{'identifier': 'Rállēs, Geṓrgios A.', 'viaf': '10200332'},
                                                  {'identifier': 'Potlēs, Michaēl', 'viaf': '66864785'}]},
            'Eustathios Romaios Schminck': {'author': ['Eustathios', 61], 'factoid': 374066, 'authority': [mj],
                                            'work': '',
                                            'expression': '“Vier eherechtliche Entscheidungen aus dem 11. Jahrhundert”,'
                                                          ' Fontes Minores 3 (1979) 221-279',
                                            'editor': [{'identifier': 'Schminck, Andreas', 'viaf': '62165959'}]},
            'Eustathios: Capture of Thessalonike': {'author': ['Eustathios', 20147], 'factoid': 451468,
                                                    'authority': [mj], 'work': 'De Thessalonica a Latinis capta',
                                                    'expression': 'La espugnazione di Tessalonica, Palermo 1961',
                                                    'editor': [{'identifier': 'Kyriakidēs, Stilpōn Paraskeua',
                                                                'viaf': '69687416'}]},
            'Fulcher of Chartres': {'author': ['Fulcher', 101], 'factoid': 442407, 'authority': [mj]},
            'Geonames': {'authority': [cr]},
            # http://apps.brepolis.net.uaccess.univie.ac.at/lexiema/test/Default2.aspx
            'Glykas': {'author': ['Michael', 305], 'authority': [tp],  # Authority/source is actually Schreiner!
                       'work': 'Annales',
                       'expression': 'Michaelis Glycae Annales, Corpus scriptorum historiae Byzantinae 21, Bonn 1836',
                       'editor': [{'identifier': 'Bekker, Immanuel', 'viaf': '44387193'}]},
            'Gregory VII, Epistolae vagantes': {'author': ['Gregorios', 27], 'factoid': 444761, 'authority': [jr]},
            'Gregory VII, in Caspar': {'author': ['Gregorios', 27], 'authority': [jr]},  # opera
            'Hilandar': {'authority': [tp]},
            'Humbert, Commemoratio': {'author': ['Humbert', 101], 'authority': [jr]},  # no explicit factoid
            'Humbert, Dialogus': {'author': ['Humbert', 101], 'factoid': 445015, 'authority': [jr]},
            'Humbert, Excommunication': {'author': ['Humbert', 101], 'factoid': 435209, 'authority': [jr]},
            'Hypomnema on marriage': {'authority': [mj]},
            'Hypomnesis of May, 1094': {'authority': []},
            'Ibn Shaddad': {'authority': [bs, hm]},
            'Ibn al-Athir': {'authority': [wa]},
            'Ioannes Italos': {'author': ['Ioannes', 66], 'authority': [mj]},  # opera
            'Italikos': {'author': ['Michael', 20130], 'authority': [mj],
                         'work': 'Letters and discourses',
                         'expression': 'Michel Italikos. Lettres et discours, Archives de l\'Orient Chrétien 14, '
                                       'Paris 1972',
                         'editor': [{'identifier': 'Gautier, Paul', 'viaf': '231073465'}]},  # opera
            'Italos trial': {'authority': [mj],
                             'work': '',
                             'expression': '“Le proces officiel de Jean l’Italien: les actes et leurs sous-entendus,” '
                                           'Travaux et Memoires 9 (1985), 133-69',
                             'editor': [{'identifier': 'Gouillard, Jean', 'viaf': '88739139'}]},
            'Iveron': {'authority': [tp],
                       'work': '',
                       'expression': '',
                       'editor': [{'identifier': '', 'viaf': ''}, {'identifier': '', 'viaf': ''}]},
            'Jus Graeco-Romanum, III': {'authority': [mj]},
            'Kastamonitou': {'authority': [tp]},
            'Kecharitomene': {'authority': [mj], 'author': ['Eirene', 61],
                              'work': 'Kecharitomene typikon',
                              'expression': '"Le typikon de la Théotokos Kécharitôménè", Revue des études byzantines, '
                                            '43 (1985), 5-165',
                              'editor': [{'identifier': 'Gautier, Paul', 'viaf': '231073465'}]},
            'Kekaumenos': {'author': ['Anonymus', 274], 'factoid': 228104, 'authority': [tp]},
            'Keroularios  ': {'author': ['Michael', 11], 'authority': [jr]},  # opera
            'Kinnamos': {'author': ['Ioannes', 17001], 'factoid': 356015, 'authority': [mj],
                         'work': 'Epitome',
                         'expression': 'Ioannis Cinnami Epitome, Corpus scriptorum historiae Byzantinae 10, Bonn 1836',
                         'editor': [{'identifier': 'Meineke, August', 'viaf': '69736744'}]},
            'Kleinchroniken': {'authority': [tp], 'work': 'Short Chronicles',
                               'expression': 'Die byzantinischen Kleinchroniken, 3 vols., Vienna 1975-1979',
                               'editor': [{'identifier': 'Schreiner, Peter', 'viaf': '14789545'}]},
            'Koltsida-Makri': {'authority': [ok]},
            'Kyrillos Phileotes': {'authority': [tp]},
            'Laurent, Corpus V.2': {'authority': [ok]},
            'Laurent, Corpus V.3': {'authority': [ok]},
            'Lavra': {'authority': [tp]},
            'Lazaros of Galesion': {'authority': [tp], 'author': ['Gregorios', 135], 'factoid': 229931,
                                    'work': 'Vita of Lazaros of Galesion',
                                    'expression': '“Vita S. Lazari auctore Gregorio monacho”, Acta Sanctorum Novembris '
                                                  '3, Société des Bollandistes, Brussels (1910) 508-606',
                                    'editor': [{'identifier': 'Delehaye, Hippolyte', 'viaf': '71427301'}]},
            'Leo IX  ': {'author': ['Leon', 29], 'authority': [jr]},  # opera
            'Leon of Chalcedon': {'author': ['Leon', 114], 'factoid': 444848, 'authority': [jr]},
            'Leon of Ohrid (Greek)': {'author': ['Leon', 108], 'factoid': 434954, 'authority': [jr]},
            'Leon of Ohrid (Latin)': {'author': ['Leon', 108, 'Humbert', 101], 'factoid': 434954, 'authority': [jr]},
            'Lupus protospatharius': {'authority': [mj]},
            'Manasses, Chronicle': {'author': ['Konstantinos', 302], 'factoid': 441043, 'authority': [mj],
                                    'work': '',
                                    'expression': '',
                                    'editor': [{'identifier': '', 'viaf': ''}, {'identifier': '', 'viaf': ''}]},
            'Manasses, Chronicle: Dedication': {'author': ['Konstantinos', 302], 'factoid': 440958, 'authority': [mj]},
            'Manasses, Hodoiporikon': {'authority': [mj]},
            'Matthew of Edessa': {'authority': [ta]},
            'Mauropous: Letters': {'author': ['Ioannes', 289], 'authority': [tp]},  # opera
            'Mauropous: Orations': {'author': ['Ioannes', 289], 'authority': [tp]},  # opera
            'Michael the Rhetor, Regel': {'author': ['Michael', 17004], 'factoid': 449588, 'authority': [mj]},
            'Michel, Amalfi': {'author': ['Laycus', 101], 'factoid': 445024, 'authority': [jr]},
            'Nea Mone, Gedeon': {'dbid': 'Nea Mone,', 'authority': [tp],
                                 'work': '',
                                 'expression': '“Βυζαντινὰ χρυσόβουλλα καὶ πιττάκια”, Ἐκκλησιαστικὴ Ἀλήθεια 4 (1883-84)'
                                               ' 403-406, 411-413, 428-431, 444-447',
                                 'editor': [{'identifier': 'Gedeōn, Manouēl Iō.', 'viaf': '64758841'}]},
            'Nea Mone, Miklosich-Müller': {'dbid': 'Nea Mone,', 'authority': [tp],
                                           'work': '',
                                           'expression': 'Acta et diplomata graeca medii aevi sacra et profana, '
                                                         'vol. 5, Vienna (1890) 1-10',
                                           'editor': [{'identifier': 'Miklošič, Franc', 'viaf': '78772873'},
                                                      {'identifier': 'Müller, Joseph', 'viaf': '57396472'}]},
            'Nicolas d\'Andida': {'author': ['Nikolaos', 257], 'factoid': 444805, 'authority': [jr]},
            'Nicole, Chartophylax': {'author': ['Alexios', 1], 'factoid': 444947, 'authority': [jr]},
            'Niketas Choniates, Historia': {'author': ['Niketas', 25001], 'factoid': 435679, 'authority': [mj],
                                            'work': 'Historia',
                                            'expression': 'Nicetae Choniatae Historia, 2 vols. Berlin 1975',
                                            'editor': [{'identifier': 'Dieten, Jan-Louis van', 'viaf': '61585968'}]},
            'Niketas Stethatos (Darrouzes)': {'author': ['Niketas', 105], 'authority': [jr]},  # opera
            'Niketas Stethatos, On the Holy Spirit': {'author': ['Niketas', 105], 'factoid': 445329, 'authority': [jr]},
            'Nikolaos III to Urban II': {'author': ['Nikolaos', 13], 'factoid': 444667, 'authority': [jr]},
            'Oath of Eudokia': {'author': ['Eudokia', 1], 'factoid': 380209, 'authority': [mj]},
            'Odo of Deuil': {'author': ['Odo', 102], 'factoid': 445727, 'authority': [mj]},
            'Pakourianos': {'author': ['Gregorios', 61], 'factoid': 254714, 'authority': [tp]},
            'Panteleemon': {'authority': [tp]},
            'Pantokrator (Athos)': {'authority': [tp]},
            'Pantokrator Typikon': {'authority': [tp]},
            'Parthenon inscriptions': {'authority': [mj]},
            'Paschal II, Letter to Alexios I': {'author': ['Paschales', 22], 'factoid': 444991, 'authority': [mj]},
            'Patmos: Acts': {'authority': [tp],
                             'expression': 'Βυζαντινὰ ἔγγραφα τῆς μονῆς Πάτμου 1. Αὐτοκρατορικά, '
                                           '2. Δημοσίων λειτουργῶν, Athens 1980',
                             'editor': [{'identifier': 'Vranoúsīs, Léandros I.', 'viaf': '51818933'},
                                        {'identifier': 'Nystazopoúlou-Pelekídou, María', 'viaf': '44710829'}]},
            'Patmos: Codicil': {'authority': [mw]},
            'Patmos: Testament': {'authority': [mw]},
            'Patmos: Typikon': {'authority': [mw]},
            'Peri metatheseon': {'authority': [mj]},
            'Petros of Antioch  ': {'author': ['Petros', 103], 'authority': [tp]},  # multiple
            'Petros of Antioch, ep. 2': {'author': ['Petros', 103], 'factoid': 435035, 'authority': [tp]},
            'Pleiades': {'authority': [cr]},
            'Prodromos, Historische Gedichte': {'author': ['Theodoros', 25001], 'authority': [mj],  # opera
                                                'editor': [{'identifier': 'Hörandner, Wolfram', 'viaf': '46774760'}]},
            'Protaton': {'authority': [tp]},
            'Psellos Eirene': {'author': ['Michael', 61], 'authority': [mj], 'factoid': 380098,
                               'work': 'Ἐπιτάφιος εἰς Εἰρήνην καισάρισσαν',
                               'expression': 'Michaelis Pselli Scripta minora magnam partem adhuc inedita I, '
                                             'Milan 1936, 155-189',
                               'editor': [{'identifier': 'Kurtz, Eduard', 'viaf': '59840374'},
                                          {'identifier': 'Drexl, Franz', 'viaf': '35433472'}]},
            'Psellos K-D 232': {'author': ['Michael', 61], 'authority': [mj], 'factoid': 380515,
                                'work': 'K-D 232',
                                'expression': 'Michaelis Pselli Scripta minora magnam partem adhuc inedita I, '
                                              'Milan 1936',
                                'editor': [{'identifier': 'Kurtz, Eduard', 'viaf': '59840374'},
                                           {'identifier': 'Drexl, Franz', 'viaf': '35433472'}]},  # opera
            'Psellos Sathas 72': {'author': ['Michael', 61], 'authority': [mj], 'factoid': 380172,
                                  'work': 'Sathas 72',
                                  'expression': 'Μεσαιωνικὴ Βιβλιοθήκη 5. Μιχαὴλ Ψελλοῦ ἱστορικοὶ λόγοι, '
                                                'ἐπιστολαὶ καὶ ἄλλα ἀνέκδοτα, Paris (1876) 219-523',
                                  'editor': [{'identifier': 'Sáthas, Konstantínos N.', 'viaf': '51823381'}]},  # opera
            'Psellos Sathas 83': {'author': ['Michael', 61], 'authority': [mj], 'factoid': 380184,
                                  'work': 'Sathas 83',
                                  'expression': 'Μεσαιωνικὴ Βιβλιοθήκη 5. Μιχαὴλ Ψελλοῦ ἱστορικοὶ λόγοι, '
                                                'ἐπιστολαὶ καὶ ἄλλα ἀνέκδοτα, Paris (1876) 219-523',
                                  'editor': [{'identifier': 'Sáthas, Konstantínos N.', 'viaf': '51823381'}]},  # opera
            'Psellos Sathas 151': {'author': ['Michael', 61], 'authority': [mj], 'factoid': 380249,
                                   'work': 'Sathas 151',
                                   'expression': 'Μεσαιωνικὴ Βιβλιοθήκη 5. Μιχαὴλ Ψελλοῦ ἱστορικοὶ λόγοι, '
                                                 'ἐπιστολαὶ καὶ ἄλλα ἀνέκδοτα, Paris (1876) 219-523',
                                   'editor': [{'identifier': 'Sáthas, Konstantínos N.', 'viaf': '51823381'}]},  # opera
            'Psellos Robert': {'author': ['Michael', 61], 'authority': [mj], 'factoid': 380056,
                               'work': 'Χρυσόβουλλον τοῦ αύτοῦ σταλὲν πρὸς τὸν ῾Ρόμβερτον '
                                       'παρὰ τοῦ βασιλέως κυροῦ Μιχαὴλ τοῦ Δούκα',
                               'expression': 'Michael Psellus, Orationes forenses et acta, '
                                             'Stuttgart – Leipzig 1994, 169-175',
                               'editor': [{'identifier': 'Dennis, George T.', 'viaf': '17240388'}]},
            'Psellos: Chronographia': {'author': ['Michael', 61], 'factoid': 249646, 'authority': [mw],
                                       'work': 'Chronographia',
                                       'expression': 'Michel Psellos, Chronographie, 2 vols., Paris 1967',
                                       'editor': [{'identifier': 'Renauld, Émile', 'viaf': '88747868'}]},
            'Ralph of Caen': {'author': ['Radulf', 112], 'authority': [mj]},  # no explicit factoid
            'Sakkos (1166)': {'authority': [mj]},
            'Sakkos (1170)': {'authority': [mj]},
            'Seals': {},  # This is a placeholder dummy entry
            'Seibt – Zarnitz': {'authority': [ok]},
            'Semeioma on Leon of Chalcedon': {'author': ['Alexios', 1], 'factoid': 444854, 'authority': [jr],
                                              'work': 'Semeioma on Leon of Chalcedon',
                                              'expression': '“Documents inédits tirés de la bibliothèque de Patmos. '
                                                            'Décret d’Alexis Comnène portant déposition de Léon, '
                                                            'métropolitain de Chalcédoine”, '
                                                            'Bulletin de Correspondance Hellénique 2 (1878), 102-128',
                                              'editor': [{'identifier': 'Sakkeliōn, Iōannēs', 'viaf': '20048582'}]},
            'Skylitzes': {'author': ['Ioannes', 110], 'factoid': 223966, 'authority': [tp],
                          'work': 'Synopsis historikon',
                          'expression': 'Ioannis Scylitzae Synopsis Historiarum, Corpus fontium historiae '
                                        'Byzantinae 5, Berlin – New York 1973',
                          'editor': [{'identifier': 'Thurn, Hans', 'viaf': '9970194'}]},
            'Skylitzes Continuatus': {'author': ['Anonymus', 102], 'authority': [tp],
                                      'work': 'Skylitzes continuatus',
                                      'expression': 'Ἡ συνέχεια τῆς Χρονογραφίας τοῦ Ἰωάννου Σκυλίτση '
                                                    '(Ioannes Skylitzes Continuatus), Ἑταιρεία Μακεδονικῶν Σπουδῶν, '
                                                    'Ιδρυμα Μελετῶν Χερσονήσου τοῦ Αἵμου 105, '
                                                    'Thessalonike (1968) 103-186',
                                      'editor': [{'identifier': 'Tsolákīs, Eúdoxos Th.', 'viaf': '63909505'}]},
            # placeholder person from PBW
            'Sode, Berlin': {'authority': [ok]},
            'Speck, Berlin': {'authority': [ok]},
            'Stavrakos': {'authority': [ok]},
            'Synod of 1072': {'authority': [mj]},
            'Synod of 1094': {'authority': [tp],
                              'work': '',
                              'expression': '“Le synode des Blachernes (fin 1094). Étude prosopographique”, '
                                            'Revue des études byzantines 29 (1971) 213-284',
                              'editor': [{'identifier': 'Gautier, Paul', 'viaf': '231073465'}]},
            'Synodal edict (1054)': {'authority': [jr]},
            'Synodal protocol (1089)': {'authority': [jr]},
            'Synopsis Chronike': {'authority': []},
            'Thebes: Cadaster': {'authority': [mj]},
            'Thebes: Confraternity': {'authority': [mj]},
            'Theophylact of Ohrid, Speech to Alexios I': {'author': ['Theophylaktos', 105], 'factoid': 444549,
                                                          'authority': [mj]},
            'Theophylaktos of Ohrid, Letters': {'author': ['Theophylaktos', 105], 'authority': [mj],
                                                'work': '',
                                                'expression': '',
                                                'editor': [{'identifier': '', 'viaf': ''},
                                                           {'identifier': '', 'viaf': ''}]},  # opera
            'Tornikes, Georgios': {'author': ['Georgios', 25002], 'authority': [mj],
                                   'work': 'Epistulae',
                                   'expression': 'George et Dèmetrios Tornikès: lettres et discours, Paris 1970',
                                   'editor': [{'identifier': 'Darrouzès, Jean', 'viaf': '19676388'}]},  # opera
            'Tzetzes, Exegesis of Homer': {'authority': [mj]},
            'Tzetzes, Historiai': {'author': ['Ioannes', 459], 'factoid': 449306, 'authority': [mj]},
            'Tzetzes, Homerica': {'authority': [mj]},
            'Tzetzes, Letters': {'author': ['Ioannes', 459], 'authority': [mj]},  # opera
            'Tzetzes, Posthomerica': {'authority': [mj]},
            'Usama': {'author': ['Usama', 101], 'authority': [lo, hm]},  # no explicit factoid
            'Vatopedi': {'authority': [tp]},
            'Victor (pope)': {'author': ['Victor', 23], 'factoid': 444676, 'authority': [jr]},
            'Walter the Chancellor': {'author': ['Walter', 101], 'factoid': 441713, 'authority': [mj]},
            'Wassiliou, Hexamilites': {'authority': [ok]},
            'William of Tyre': {'author': ['William', 4001], 'factoid': 450027, 'authority': [mj]},
            'Xenophontos': {'authority': [tp]},
            'Xeropotamou': {'authority': [tp]},
            'Yahya al-Antaki': {'authority': [tp, lo, hm]},
            'Zacos II': {'authority': [ok]},
            'Zetounion': {'author': ['Nikolaos', 13], 'factoid': 445037, 'authority': [jr]},
            'Zonaras': {'author': ['Ioannes', 6007], 'authority': [mw],
                        'work': 'Epitome historiōn',
                        'expression': 'Ioannis Zonarae Epitome Historiarum libri XIII-XVIII, '
                                      'Corpus scriptorum historiae Byzantinae 49, Leipzig 1897',
                        'editor': [{'identifier': 'Büttner-Wobst, Theodor', 'viaf': '56683038'}]}
        }

        self.entitylabels = {
            'C1': 'Resource:sdhss__C1',    # Social Quality of an Actor (Embodiment)
            'C3': 'Resource:sdhss__C3',    # Social Relationship
            'C4': 'Resource:sdhss__C4',    # Social Relationship Type
            'C5': 'Resource:sdhss__C5',    # Membership
            'C7': 'Resource:sdhss__C7',    # Occupation
            'C11': 'Resource:sdhss__C11',  # Gender
            'C12': 'Resource:sdhss__C12',  # Social Role
            'C13': 'Resource:sdhss__C13',  # Social Role Embodiment
            'C21': 'Resource:sdhss__C21',  # Skill
            'C23': 'Resource:sdhss__C23',  # Religious Identity
            'C24': 'Resource:sdhss__C24',  # Religion or Religious Denomination
            'C29': 'Resource:sdhss__C29',  # Know-How
            'C99': 'Resource:r11__C99',    # Kinship
            'E13': 'Resource:crm__E13_Attribute_Assignment',
            'E15': 'Resource:crm__E15_Identifier_Assignment',
            'E17': 'Resource:crm__E17_Type_Assignment',
            'E18': 'Resource:crm__E18_Physical_Thing',
            'E21': 'Resource:crm__E21_Person',
            'E22': 'Resource:`crm__E22_Human-Made_Object`',
            'E31': 'Resource:crm__E31_Document',
            'E33': 'Resource:crm__E33_Linguistic_Object',
            'E34': 'Resource:crm__E34_Inscription',
            'E39': 'Resource:crm__E39_Actor',
            'E41': 'Resource:crm__E41_Appellation',
            'E42': 'Resource:crm__E42_Identifier',
            'E52': 'Resource:`crm__E52_Time-Span`',
            'E55': 'Resource:crm__E55_Type',
            'E56': 'Resource:crm__E56_Language',
            'E62': 'Resource:crm__E62_String',
            'E69': 'Resource:crm__E69_Death',
            'E73': 'Resource:crm__E73_Information_Object',
            'E74': 'Resource:crm__E74_Group',
            'E78': 'Resource:crm__E78_Curated_Holding',
            'E87': 'Resource:crm__E87_Curation_Activity',
            'F1': 'Resource:lrmoo__F1',    # Work
            'F2': 'Resource:lrmoo__F2',    # Expression
            'F27': 'Resource:lrmoo__F27',  # Work Creation
            'F28': 'Resource:lrmoo__F28'   # Expression Creation
        }

        self.predicates = {
            'P1': 'crm__P1_is_identified_by',
            'P2': 'crm__P2_has_type',
            'P3': 'crm__P3_has_note',
            'P4': 'crm__P4_has_time_span',
            'P14': 'crm__P14_carried_out_by',
            'P16': 'crm__P16_used_specific_object',
            'P17': 'crm__P17_was_motivated_by',
            'P37': 'crm__P37_assigned',
            'P41': 'crm__P41_classified',
            'P42': 'crm__P42_assigned',
            'P48': 'crm__P48_has_preferred_identifier',
            'P51': 'crm__P51_has_former_or_current_owner',
            'P70': 'crm__P70_documents',
            'P94': 'crm__P94_has_created',
            'P100': 'crm__P100_was_death_of',
            'P102': 'crm__P102_has_title',
            'P107': 'crm__P107_has_current_or_former_member',
            'P108': 'crm__P108_has_produced',
            'P127': 'crm__P127_has_broader_term',
            'P128': 'crm__P128_carries',
            'P140': 'crm__P140_assigned_attribute_to',
            'P141': 'crm__P141_assigned',
            'P147': 'crm__P147_curated',
            'P148': 'crm__P148_has_component',
            'P165': 'crm__P165_incorporates',
            'P177': 'crm__P177_assigned_property_type',
            'R3': 'lrmoo__R3',     # is realised in
            'R5': 'lrmoo__R5',     # has component
            'R15': 'lrmoo__R15',   # has fragment
            'R16': 'lrmoo__R16',   # created [work]
            'R17': 'lrmoo__R17',   # created [expression]
            'R76': 'lrmoo__R76',   # is derivative of
            'SP13': 'sdhss__P13',  # pertains to [person, social quality]
            'SP14': 'sdhss__P14',  # has social quality
            'SP16': 'sdhss__P16',  # has relationship type
            'SP17': 'sdhss__P17',  # has relationship source
            'SP18': 'sdhss__P18',  # has relationship target
            'SP26': 'sdhss__P26',  # is embodiment by [person, social role]
            'SP33': 'sdhss__P33',  # is embodiment of [social role]
            'SP35': 'sdhss__P35',  # is defined by [person, religious identity]
            'SP36': 'sdhss__P36',  # pertains to [religious identity]
            'SP37': 'sdhss__P37',  # concerns [know-how]
            'SP38': 'sdhss__P38'   # has skill
        }

        self.prednodes = dict()

        self.allowed = {
            'E / M XI',
            'L XI',
            'M XI',
            'L XI / E XII',
            'M / L XI',
            'XI',
            'E / L XI',
            'M XI / E XII',
            'E  / M XI',
            'L X / E XI',
            'E XI',
            'L XI / M XII',
            'L  XI',
            'M XI / L XI',
            'M XI/L XI',
            'L X / M XI',
            'M / L  XI',
            'E /M XI',
            'E XI / M XI',
            'E XI/M XI',
            'E / M  XI',
            'XI-XII',
            'M  / L XI',
            'mid XI',
            '1041/2',
            'mid-XI',
            'late XI',
            'L XI?',
            '1088',
            '1060',
            'M-E XI',
            'M/L XI',
            'E-L XI',
            'E XI-',
            'L XI-E XII',
            'M-L XI',
            'L XI/E XII',
            'E XI-1071',
            'c. 1006-1067',
            'c. 1050-1103',
            'c. 1007-1061',
            'E XI-c. 1088',
            'M XI/LXI',
            '1066-1123?',
            '1083-c. 1154',
            '1057-1118',
            'E/M XI',
            'XI / XII',
            'X / XI',
            '1050',
            '1070',
            '1075',
            'L XI /  E XII',
            'E X1',
            '??',
            '(XI)',
            'E  / L XI',
            '1090',
            'L XI / L XII',
            'X / XII',
            'LXI / E XII',
            'L XI /  M XII',
            '1084',
            'L XI - E XII',
            '? / M XI',
            'L X/ E XI',
            '1025',
            '11th c.',
            ' L XI',
            'L XI / XII',
            '?  XI?',
        }

        # Define our STAR model predicates
        self.star_subject = self.predicates['P140']
        self.star_predicate = self.predicates['P177']
        self.star_object = self.predicates['P141']
        self.star_source = self.predicates['P17']
        self.star_auth = self.predicates['P14']

        # Initialise our group agents and the data structures we need to start
        print("Setting up PBW constants...")
        # Make our anonymous agent PBW for the un-sourced information
        pbwcmd = "COMMAND (a:%s {descname:'Prosopography of the Byzantine World', " \
                 "prefix:'https://pbw2016.kdl.kcl.ac.uk/person/', " \
                 "constant:TRUE}) RETURN a" % self.entitylabels.get('E39')
        self.pbw_agent = self._fetch_uuid_from_query(pbwcmd)
        # and our VIAF agent for identifying PBW contributors
        viafcmd = "COMMAND (a:%s {descname:'Virtual Internet Authority File', prefix:'https://viaf.org/viaf/', " \
                  "constant:TRUE}) RETURN a" % self.entitylabels.get('E39')
        self.viaf_agent = self._fetch_uuid_from_query(viafcmd)

        # Some of these factoid types have their own controlled vocabularies.
        # Set up our structure for retaining these; we will define them when we encounter them
        # through the accessor functions.
        self.cv = {
            'Gender': dict(),
            'Ethnicity': dict(),
            'Religion': dict(),
            'Language': dict(),
            'SocietyRole': dict(),
            'Dignity': dict(),
            'Kinship': dict()
        }
        # Special-case 'slave' and ordained/consecrated roles out of 'occupations'
        self.legal_designations = ['Slave', 'Monk', 'Nun', 'Nun (?)', 'Bishops', 'Monk (Latin)', 'Patriarch',
                                   'Servant of Christ', 'Servant of God', 'Hieromonk', 'Servant of the Lord']

        # Make a list of boulloterions that are missing their references, with a link to the publication
        # that the seals come from or -1 if we should use the seal catalogues as sources
        self.boulloterion_sources = {
            3897: (-1, None),
            4779: (45, 'no. 345'),
            5740: (-1, None)
        }
    # END OF __init__
    # Lookup functions

    def source(self, factoid):
        """Return all the information we have for the given source ID"""
        a = factoid.source
        if a in self.aggregate_sources:
            # We have to look at the source ref to figure out which version of the source we are using
            aggregates = self.aggregate_sources[a]
            if isinstance(aggregates, dict):
                for rexp, sstr in aggregates.items():
                    result = re.match(rexp, factoid.sourceRef)
                    if result:
                        if len(result.groups()):
                            a = "%s %s" % (factoid.source, sstr.replace('##', result.group(1)))
                        else:
                            a = "%s %s" % (factoid.source, sstr)
                        break
            else:
                for sstr in aggregates:
                    if factoid.sourceRef.startswith(sstr):
                        a = "%s %s" % (factoid.source, sstr)
                        break
        return a

    def author(self, a):
        """Return the PBW person identifier for the given source author."""
        srecord = self.sourcelist.get(a, None)
        if srecord is None:
            return None
        return srecord.get('author', None)

    def editor(self, a):
        srecord = self.sourcelist.get(a, None)
        if srecord is None:
            return None
        return srecord.get('editor', None)

    def authorities(self, a):
        """Return the name(s) of the scholar(s) responsible for ingesting the info from the given source
        into the database. Information on the latter is taken from https://pbw2016.kdl.kcl.ac.uk/ref/sources/
        and https://pbw2016.kdl.kcl.ac.uk/ref/seal-editions/"""
        srecord = self.sourcelist.get(a, None)
        if srecord is None:
            return None
        return srecord.get('authority', None)

    def sourceref(self, factoid):
        """Return the source reference, modified to account for our aggregate sources."""
        a = factoid.source
        aref = factoid.sourceRef
        if a in self.aggregate_sources:
            # We have to look at the source ref to figure out which version of the source we are using
            aggregates = self.aggregate_sources[a]
            if isinstance(aggregates, dict):
                for rexp, sstr in aggregates.items():
                    result = re.match(rexp, aref)
                    if result is not None:
                        aref = aref.replace(result.group(0), '')
            else:
                for rexp in aggregates:
                    aref = aref.replace(rexp, '')
        return aref




    def get_label(self, lbl):
        """Return the fully-qualified entity or predicate label given the short name.
        We want this to throw an exception if nothing is found."""
        try:
            return self.entitylabels[lbl]
        except KeyError:
            return self.predicates[lbl]

    def get_predicate(self, p):
        """Return the reified predicate UUID for the given short name. This will throw
        an exception if no predicate with this key is defined."""
        if p not in self.prednodes:
            fqname = self.predicates[p]
            predq = "COMMAND (n:Resource:%s {constant:TRUE}) RETURN n" % fqname
            npred = self._fetch_uuid_from_query(predq)
            self.prednodes[p] = npred
        return self.prednodes[p]

    # Accessors / creators for our controlled vocabularies
    def _find_or_create_cv_entry(self, category, nodeclass, label, superlabel=None):
        if label in self.cv[category]:
            return self.cv[category][label]
        # We have to create the node, possibly attaching it to a superclass
        nodeq = "(cventry:%s {constant:TRUE, value:\"%s\"})" % (nodeclass, label)
        nq = "COMMAND %s" % nodeq
        if superlabel is not None:
            nq = "MERGE (super:%s {constant:TRUE, value:\"%s\"}) WITH super COMMAND %s-[:%s]->(super) " % (
                self.get_label('E55'), superlabel, nodeq, self.get_label('P2'))
        nq += " RETURN cventry"
        uuid = self._fetch_uuid_from_query(nq)
        if uuid is None:
            raise('UUID for %s label (%s) not found' % (label, category))
        self.cv[category][label] = uuid
        return uuid

    def get_gender(self, gender):
        return self._find_or_create_cv_entry('Gender', self.get_label('C11'), gender)

    def get_religion(self, rel):
        return self._find_or_create_cv_entry('Religion', self.get_label('C24'), rel)

    def get_ethnicity(self, ethlabel):
        return self._find_or_create_cv_entry('Ethnicity', self.get_label('E74'), ethlabel, 'Ethnic Group')

    def get_language(self, lang):
        return self._find_or_create_cv_entry('Language', self.get_label('C29'), lang, 'Language Skill')

    def get_kinship(self, kinlabel):
        return self._find_or_create_cv_entry('Kinship', self.get_label('C4'), kinlabel, 'Kinship')

    def get_societyrole(self, srlabel):
        if srlabel in self.legal_designations:
            return self._find_or_create_cv_entry('SocietyRole', self.get_label('C12'), srlabel, 'Legal Status')
        else:
            return self._find_or_create_cv_entry('SocietyRole', self.get_label('C7'), srlabel)

    def get_dignity(self, dignity):
        # Dignities in PBW tend to be specific to institutions / areas;
        # make an initial selection by breaking on the 'of'
        diglabel = [dignity]
        if ' of the ' not in dignity:  # Don't split (yet) titles that probably don't refer to places
            diglabel = dignity.split(' of ')
        dig_uuid = self._find_or_create_cv_entry('Dignity', self.get_label('C12'), diglabel[0], 'Legal Status')
        # Make sure that the UUID also appears under the original label
        self.cv['Dignity'][dignity] = dig_uuid
        return dig_uuid

    def inrange(self, floruit):
        """Return true if the given floruit tag falls within RELEVEN's range"""
        return floruit in self.allowed

    def _fetch_uuid_from_query(self, q):
        """Helper function to create one node if it doesn't already exist and return the UUID that gets
        auto-generated upon commit. The query passed in should have COMMAND where the node is match/created,
        and should RETURN the node whose UUID is wanted."""
        matchq = q.replace("COMMAND", "MATCH")
        createq = q.replace("COMMAND", "CREATE")
        with self.graphdriver.session() as session:
            uuid = session.run("%s.uuid AS theid" % matchq).single()
            if uuid is None:
                session.run(createq)
                uuid = session.run("%s.uuid AS theid" % matchq).single()
            return uuid['theid']
