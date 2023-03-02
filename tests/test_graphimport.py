import unittest
import RELEVEN.PBWstarConstants
import config
from neo4j import GraphDatabase


class GraphImportTests(unittest.TestCase):

    graphdriver = None
    constants = None
    # Data keys are gender, identifier (appellation), second-name appellation, alternate-name appellation,
    # death, ethnicity, religion, societyrole, legalrole, language, kinship, possession
    testpeople = {
        'Anna 62': {'gender': ['Female'], 'identifier': 'Ἄννα Κομνηνή',
                    'secondname': {'Κομνηνοῦ': 2},
                    'death': {'count': 1, 'dated': 0},
                    'religion': {'Christian': ['Georgios 25002']},
                    'legalrole': {'Basilis': 1, 'Basilissa': 1, 'Kaisarissa': 6,
                                  'Pansebastos sebaste': 1, 'Porphyrogennetos': 7},
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
                       'possession': {'Palace in Bithynia at foot of Mount Sophon': ['Nikephoros 117', '173.7-8, 179.15']}},
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

    def setUp(self):
        self.graphdriver = GraphDatabase.driver(config.graphuri, auth=(config.graphuser, config.graphpw))
        self.constants = RELEVEN.PBWstarConstants.PBWstarConstants(self.graphdriver)
        # Get the UUIDs for each of our test people
        c = self.constants
        q = 'MATCH (id:%s)<-[:%s]-(idass:%s)-[:%s]->(person:%s), (idass)-[:%s]->(agent:%s) ' \
            'WHERE id.value IN %s AND agent.descname = "Prosopography of the Byzantine World" ' \
            'RETURN id.value, person.uuid' % (c.get_label('E42'), c.get_label('P37'), c.get_label('E15'),
                                              c.get_label('P140'), c.get_label('E21'), c.get_label('P14'),
                                              c.get_label('E39'), list(self.testpeople.keys()))
        with self.graphdriver.session() as session:
            result = session.run(q)
            self.assertIsNotNone(result)
            for record in result:
                self.testpeople[record['id.value']]['uuid'] = record['person.uuid']

    # TODO add extra assertions for the eunuchs and K62
    def test_gender(self):
        """Test that each person has the gender assignments we expect"""
        c = self.constants
        for person, pinfo in self.testpeople.items():
            q = "MATCH (p:%s)<-[:%s]-(a1:%s)-[:%s]->(ga:%s)<-[:%s]-(a2:%s)-[:%s]->(gender:%s) " \
                "WHERE p.uuid = '%s' RETURN p.descname, gender.value" \
                % (c.get_label('E21'), c.star_object, c.get_label('E13'), c.star_subject, c.get_label('E17'),
                   c.star_subject, c.get_label('E13'), c.star_object, c.get_label('C11'), pinfo['uuid'])
            with self.graphdriver.session() as session:
                result = session.run(q)
                self.assertIsNotNone(result)
                self.assertListEqual(pinfo['gender'], sorted(result.value('gender.value')),
                                     "Test gender for %s" % person)

    # The identifier is the name as PBW has it in the original language.
    def test_identifier(self):
        """Test that each person has the main appellation given in the PBW database"""
        c = self.constants
        for person, pinfo in self.testpeople.items():
            # We want the appellation that was assigned by the generic PBW agent, not any of
            # the sourced ones
            pbwagent = '%s {descname: "Prosopography of the Byzantine World"}' % c.get_label('E39')
            q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(id:%s), (a)-[:%s]->(pbw:%s) WHERE p.uuid = '%s' RETURN id.value" \
                % (c.get_label('E21'), c.star_subject, c.get_label('E13'), c.star_object, c.get_label('E41'),
                   c.star_auth, pbwagent, pinfo['uuid'])
            with self.graphdriver.session() as session:
                result = session.run(q).single()
                self.assertIsNotNone(result)
                self.assertEqual(pinfo['identifier'], result['id.value'], "Test identifier for %s" % person)

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
        for person, pinfo in self.testpeople.items():
            names = dict()
            if 'secondname' in pinfo:
                names.update(pinfo['secondname'])
            if 'altname' in pinfo:
                names.update(pinfo['altname'])
            if len(names) > 0:
                q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(appel:%s), (a)-[:%s]->(apred:%s), (a)-[:%s]->(src) " \
                    "WHERE p.uuid = '%s' RETURN appel" \
                    % (c.get_label('E21'), c.star_subject, c.get_label('E13'), c.star_object, c.get_label('E41'),
                       c.star_predicate, c.get_label('P1'), c.star_source, pinfo['uuid'])
                with self.graphdriver.session() as session:
                    result = session.run(q).value('appel')
                    self.assertIsNotNone(result)
                    # We can cheat here because all appellations in our test set are Greek
                    self._check_dict_equiv(names, result, 'gr', "Test appellations for %s" % person)

    def test_death(self):
        """Test that each person has at most one death event, since they all only died once. Also
        test that the assertions look correct"""
        c = self.constants
        # Look for all death events and see who they are aboupt
        deathevents = dict()
        q = "MATCH (de:%s)<-[:%s]-(a:%s)-[:%s]->(person:%s) RETURN DISTINCT person.uuid, de.uuid" \
            % (c.get_label('E69'), c.star_subject, c.get_label('E13'), c.star_object, c.get_label('E21'))
        with self.graphdriver.session() as session:
            result = session.run(q)
            for row in result:
                p = row['person.uuid']
                de = row['de.uuid']
                # Each person should have max one death event
                self.assertIsNone(deathevents.get(p))
                deathevents[p] = de

        for person, pinfo in self.testpeople.items():
            # Check if the person should have a death event.
            devent = deathevents.get(pinfo['uuid'])
            if 'death' not in pinfo:
                # Make sure that the death was found
                self.assertIsNone(devent)
                continue
            else:
                self.assertIsNotNone(devent)
                # See if we have the expected info about the death event in question.
                # Each event should have N description assertions, each with a P3 attribute.
                q = "MATCH (de:%s)<-[:%s]-(a:%s) WHERE de.uuid = '%s' AND a.crm__P3_has_note IS NOT NULL RETURN a" \
                    % (c.get_label('E69'), c.star_subject, c.get_label('E13'), devent)
                with self.graphdriver.session() as session:
                    result = session.run(q).value('a')
                    self.assertEqual(pinfo['death']['count'], len(result), "Death count for %s" % person)
                # and M date assertions.
                q = "MATCH (de:%s)<-[:%s]-(a:%s)-[:%s]->(dating:%s) WHERE de.uuid = '%s' " \
                    "RETURN COUNT(dating) AS num" \
                    % (c.get_label('E69'), c.star_subject, c.get_label('E13'), c.star_object,
                       c.get_label('E52'), devent)
                with self.graphdriver.session() as session:
                    result = session.run(q).single()['num']
                    self.assertEqual(pinfo['death']['dated'], result)
                # Each event should also have N different sources.
                q = "MATCH (de:%s)<-[:%s]-(a:%s)-[:%s]->(sref) WHERE de.uuid = '%s' " \
                    "RETURN COUNT(DISTINCT sref.uuid) AS num" \
                    % (c.get_label('E69'), c.star_subject, c.get_label('E13'), c.star_source, devent)
                with self.graphdriver.session() as session:
                    result = session.run(q).single()['num']
                    self.assertEqual(pinfo['death']['count'], result)

    def test_ethnicity(self):
        """Test that the ethnicity was created correctly for our people with listed ethnicities"""
        c = self.constants
        for person, pinfo in self.testpeople.items():
            # Find those with a declared ethnicity. This means a membership in a group of the given type.
            if 'ethnicity' in pinfo:
                eths = pinfo['ethnicity']
                q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(group:%s)-[:%s]->(type {value: 'Ethnic Group'})," \
                    "(a)-[:%s]->(pred:%s) " \
                    "WHERE p.uuid = '%s' RETURN group.value AS eth, COUNT(group.value) AS act" \
                    % (c.get_label('E21'), c.star_object, c.get_label('E13'), c.star_subject, c.get_label('E74'),
                       c.get_label('P2'), c.star_predicate, c.get_label('P107'), pinfo['uuid'])
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
        for person, pinfo in self.testpeople.items():
            # Check that the religious assertions were created, and that they have the correct authority.
            if 'religion' in pinfo:
                rels = pinfo['religion']
                q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(relaff:%s)<-[:%s]-(a2:%s)-[%s]->(rel:%s)," \
                    "(auth)<-[:%s]-(a)-[:%s]->(pred:%s) " \
                    "WHERE p.uuid = '%s' RETURN rel, auth" \
                    % (c.get_label('E21'), c.star_object, c.get_label('E13'), c.star_subject, c.get_label('C23'),
                       c.star_subject, c.get_label('E13'), c.star_object, c.get_label('C24'), c.star_auth,
                       c.star_predicate, c.get_label('SP36'), pinfo['uuid'])
                with self.graphdriver.session() as session:
                    # We are cheating by knowing that no test person has more than one religion specified
                    result = session.run(q).single()
                    self.assertIsNotNone(result)
                    self.assertTrue(result['rel']['value'] in rels)
                    self.assertIn('Georgios Tornikes', result['auth']['descname'])

    def test_occupation(self):
        """Test that occupations / non-legal designations are set correctly"""
        c = self.constants
        for person, pinfo in self.testpeople.items():
            # Check that the occupation assertions were created
            if 'occupation' in pinfo:
                occs = pinfo['occupation']
                q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(pocc:%s)<-[:%s]-(a2:%s)-[%s]->(occ:%s) " \
                    "WHERE p.uuid = '%s' RETURN occ" \
                    % (c.get_label('E21'), c.star_object, c.get_label('E13'), c.star_subject, c.get_label('C1'),
                       c.star_subject, c.get_label('E13'), c.star_object, c.get_label('C7'), pinfo['uuid'])
                with self.graphdriver.session() as session:
                    result = session.run(q).value('occ')
                    self.assertIsNotNone(result)
                    self._check_dict_equiv(occs, result, 'value', "Test occupations for %s" % person)

    def test_legalrole(self):
        """Test that legal designations are set correctly"""
        c = self.constants
        for person, pinfo in self.testpeople.items():
            # Check that the occupation assertions were created
            if 'legalrole' in pinfo:
                occs = pinfo['legalrole']
                q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(prole:%s)<-[:%s]-(a2:%s)-[%s]->(role:%s) " \
                    "WHERE p.uuid = '%s' RETURN role" \
                    % (c.get_label('E21'), c.star_object, c.get_label('E13'), c.star_subject, c.get_label('C13'),
                       c.star_subject, c.get_label('E13'), c.star_object, c.get_label('C12'), pinfo['uuid'])
                with self.graphdriver.session() as session:
                    result = session.run(q).value('role')
                    self.assertIsNotNone(result)
                    self._check_dict_equiv(occs, result, 'value', "Test legal roles for %s" % person)

    def test_languageskill(self):
        """Test that our Georgian monk has his language skill set correctly"""
        c = self.constants
        for person, pinfo in self.testpeople.items():
            # Find those with a language skill set
            if 'language' in pinfo:
                q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(skill:%s)<-[:%s]-(a2:%s)-[:%s]->(kh:%s)-[:%s]-(type:%s) " \
                    "WHERE p.uuid = '%s' RETURN kh.value, type.value" % (c.get_label('E21'), c.star_subject,
                                                                         c.get_label('E13'), c.star_object,
                                                                         c.get_label('C21'), c.star_subject,
                                                                         c.get_label('E13'), c.star_object,
                                                                         c.get_label('C29'), c.get_label('P2'),
                                                                         c.get_label('E55'), pinfo['uuid'])
                with self.graphdriver.session() as session:
                    result = session.run(q).single()  # At the moment we do only have one
                    self.assertIsNotNone(result)
                    self.assertEqual(pinfo['language'], result['kh.value'], "Test language for %s" % person)
                    self.assertEqual('Language Skill', result['type.value'])

    def test_kinship(self):
        """Test the kinship assertions for one of our well-connected people"""
        c = self.constants
        for person, pinfo in self.testpeople.items():
            if 'kinship' in pinfo:
                q = "MATCH (p:%s {uuid: '%s'})<-[:%s]-(a:%s)-[:%s]->" \
                    "(kg:%s)<-[:%s]-(a2:%s)-[:%s]->(kin:%s), " \
                    "(kg)<-[:%s]-(a3:%s)-[:%s]->(ktype:%s), " \
                    "(a)-[:%s]->(src:%s), (a2)-[:%s]->(trg:%s), " \
                    "(p)<-[:%s]-(ia1:%s)-[:%s]->(pid:%s), " \
                    "(kin)<-[:%s]-(ia2:%s)-[:%s]->(kinid:%s) " \
                    "RETURN DISTINCT pid.value as person, kinid.value as kin, ktype.value as kintype" % (
                    c.get_label('E21'), pinfo['uuid'], c.star_object, c.get_label('E13'), c.star_subject,
                    c.get_label('C3'), c.star_subject, c.get_label('E13'), c.star_object, c.get_label('E21'),
                    c.star_subject, c.get_label('E13'), c.star_object, c.get_label('C4'),
                    c.star_predicate, c.get_label('SP17'), c.star_predicate, c.get_label('SP18'),
                    c.star_subject, c.get_label('E15'), c.get_label('P37'), c.get_label('E42'),
                    c.star_subject, c.get_label('E15'), c.get_label('P37'), c.get_label('E42'))
                with self.graphdriver.session() as session:
                    result = session.run(q)
                    foundkin = dict()
                    for row in result:
                        k = row['kintype']
                        if k not in foundkin:
                            foundkin[k] = []
                        foundkin[k].append(row['kin'])
                    for k in foundkin:
                        foundkin[k] = sorted(foundkin[k])
                    self.assertDictEqual(pinfo['kinship'], foundkin, "Kinship links for %s" % person)


    def test_possession(self):
        """Check possession assertions. Test the sources and authors/authorities while we are at it."""
        c = self.constants
        a = c.get_label('E13')
        for person, pinfo in self.testpeople.items():
            # Find those who possess something. All the possessions are documented in written sources
            # whose authors are also in PBW; exploit this to test that the written sources were set up correctly.
            if 'possession' in pinfo:
                q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(poss:%s), " \
                    "(a)-[:%s]->(author)<-[:%s]-(idass:%s)-[%s]->(id:%s), " \
                    "(a)-[:%s]->(ppred:%s), " \
                    "(a)-[:%s]->(src:%s)<-[:%s]-(a2:%s)-[:%s]->(edition:%s), " \
                    "(a2)-[:%s]->(partpred:%s), " \
                    "(edition)<-[:%s]-(a3:%s)-[:%s]->(work:%s), " \
                    "(a3)-[:%s]->(edpred:%s), " \
                    "(work)<-[:%s]-(a4:%s)-[:%s]->(creation:%s), " \
                    "(a4)-[:%s]->(workpred:%s), " \
                    "(creation)<-[:%s]-(a5:%s)-[:%s]->(author), (a5)-[:%s]->(authpred:%s) " \
                    "WHERE p.uuid = '%s' RETURN poss, id, src" % (
                        # person is object property of possession
                        c.get_label('E21'), c.star_object, a, c.star_subject, c.get_label('E18'),
                        # ...according to author, who is known by an identifier
                        c.star_auth, c.star_subject, c.get_label('E15'), c.star_object, c.get_label('E42'),
                        # the possession has the person as owner
                        c.star_predicate, c.get_label('P51'),
                        # as we know from source extract, which belongs to the edition
                        c.star_source, c.get_label('E33'), c.star_object, a, c.star_subject, c.get_label('F2'),
                        # in the capacity that the edition has the src as fragment
                        c.star_predicate, c.get_label('R15'),
                        # the edition belongs to a work
                        c.star_object, a, c.star_subject, c.get_label('F1'),
                        # which work is realised in the edition
                        c.star_predicate, c.get_label('R3'),
                        # the work belongs to a creation event
                        c.star_object, a, c.star_subject, c.get_label('F27'),
                        # the creation produced the work
                        c.star_predicate, c.get_label('R16'),
                        # the creation involves our author, who carried it out
                        c.star_subject, a, c.star_object, c.star_predicate, c.get_label('P14'),
                        pinfo['uuid'])
                with self.graphdriver.session() as session:
                    result = session.run(q)  # At the moment we do only have one
                    rowct = 0
                    for row in result:
                        rowct += 1
                        poss = row['poss'][c.get_label('P1')]
                        author = row['id']['value']
                        src = row['src']['reference']
                        self.assertTrue(poss in pinfo['possession'], "Test possession is correct for %s" % person)
                        (agent, reference) = pinfo['possession'][poss]
                        self.assertEqual(author, agent, "Test possession authority is set for %s" % person)
                        self.assertEqual(reference, src, "Test possession source ref is set for %s" % person)
                    self.assertEqual(rowct, len(pinfo['possession'].keys()),
                                     "Test %s has the right number of possessions" % person)

    def test_boulloterions(self):
        pass

    def test_source_works(self):
        pass

    def test_db_entry(self):
        pass

    def tearDown(self):
        self.graphdriver.close()


if __name__ == '__main__':
    unittest.main()
