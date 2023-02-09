import unittest
import RELEVEN.PBWstarConstants
import config
from neo4j import GraphDatabase


class GraphImportTests(unittest.TestCase):

    graphdriver = None
    constants = None
    # Herve 101, Ioannes 62, 68, 101, 102, Konstantinos 62, 101, 102, Anna 62, 101, 102
    # Data keys are gender, identifier (appellation), second-name appellation, alternate-name appellation,
    # death, ethnicity, religion, societyrole, legalrole, language, kinship, possession
    testpeople = {'Anna 62': {'gender': ['Female'], 'identifier': 'Ἄννα Κομνηνή',
                              'secondname': {'Κομνηνοῦ': 2, 'Δούκα': 1},
                              'death': {'count': 1, 'dated': 0},
                              'religion': {'Christian': ['Georgios', 25002]},
                              'legalrole': {'Majesty': 4, 'Despoina': 2, 'Basilis': 1, 'Basilissa': 1, 'Kaisarissa': 8,
                                            'Pansebastos sebaste': 1, 'Porphyrogennetos': 9},
                              'kinship': {'daughter': ['Alexios 1', 'Eirene 61'],
                                          'sister': ['Ioannes 2', 'Maria 146'],
                                          'wife': ['Nikephoros 117'],
                                          'sister-in-law': ['Nikephoros 178'],
                                          'niece': ['Isaakios 61', 'Michael 121', 'Ioannes 65'],
                                          'aunt': ['Manuel 1'],
                                          'daughter (eldest)': ['Eirene 61'],
                                          'fiancée': ['Konstantinos 62'],
                                          'granddaughter': ['Anna 61', 'Ioannes 63', 'Maria 62'],
                                          'kin': ['Michael 7'],
                                          'mother': ['Alexios 17005', 'Eirene 25003', 'Andronikos 118',
                                                     'Konstantinos 285', 'Maria 171']}
                              },
                  'Anna 64': {'gender': ['Female'], 'identifier': 'τῆς κουροπαλατίσσης Ἄννης',
                              'legalrole': {'Kouropalatissa': 1},
                              'kinship': {'grandmother': ['Anonymus 61'],
                                          'mother': ['Ioannes 61', 'Nikephoros 62']}},
                  'Anna 101': {'gender': ['Female'], 'identifier': 'Ἄννα',
                               'altname': {'Ἀρετῆς': 1}, 'legalrole': {'Nun': 1},
                               'kinship': {'daughter': ['Konstantinos 10', 'Eudokia 1']}},
                  'Anna 102': {'gender': ['Female'], 'identifier': ' Ἄννῃ',
                               'death': {'count': 1, 'dated': 0}, 'legalrole': {'Nun': 1},
                               'kinship': {'wife': ['Eustathios 105'],
                                           'mother': ['Romanos 106']}},
                  'Apospharios 101': {'gender': ['Male'], 'identifier': ' Ἀποσφάριον', 'legalrole': {'Slave': 1},
                                      'kinship': {'husband': ['Selegno 101']}},
                  'Balaleca 101': {'gender': ['Male'], 'identifier': 'Βαλαλεχα', 'language': 'Georgian'},
                  'Herve 101': {'gender': ['Male'], 'identifier': 'Ἐρβέβιον τὸν Φραγγόπωλον',
                                'secondname': {'Φραγγόπωλον': 2},
                                'ethnicity': {'Norman': 1},
                                'legalrole': {'Stratelates': 1, 'Vestes': 1, 'Magistros': 1},
                                'possession': ['House at Dagarabe in Armeniakon']},
                  'Ioannes 62': {'gender': ['Male'], 'identifier': 'Ἰωάννης',
                                 'secondname': {'Δούκα': 6}, 'altname': {'Ἰγνάτιος': 1},
                                 'death': {'count': 1, 'dated': 0},
                                 'legalrole': {'Stratarches': 1, 'Kaisar': 1, 'Basileopator': 1,
                                               'Strategos autokrator': 1, 'Basileus': 1, 'Monk': 4},
                                 'kinship': {'brother': ['Konstantinos 10'],
                                             'husband': ['Eirene 20117'],
                                             'uncle': ['Michael 7', 'Andronikos 62', 'Konstantios 61'],
                                             'father': ['Konstantinos 61', 'Andronikos 61', 'Anonymi 6049'],
                                             'father-in-law': ['Maria 62'],
                                             'grandfather': ['Michael 121', 'Ioannes 65', 'Eirene 61', 'Anonymus 25018'],
                                             'nephew (son of brother)': ['Michael 7'],
                                             'relative by marriage': ['Nikephoros 101']
                                             },
                                 'possession': ['Palace in Bithynia at foot of Mount Sophon']},
                  'Ioannes 68': {'gender': ['Eunuch'], 'identifier': 'τοῦ Ὀρφανοτρόφου',
                                 'death': {'count': 4, 'dated': 1},
                                 'legalrole': {'Praipositos': 1, 'Orphanotrophos': 1, 'Synkletikos': 1, 'Monk': 7},
                                 'occupation': {'Beggar': 1, 'Servant': 1},
                                 'kinship': {'brother': ['Michael 4', 'Niketas 104', 'Konstantinos 64', 'Georgios 106',
                                                         'Anonymi 6008', 'Stephanos 101', 'Maria 104'],
                                             'uncle': ['Michael 5'], 'uncle (maternal)': ['Michael 5'],
                                             'brother (first)': ['Michael 4', 'Anonymi 6008'],
                                             'cousin of parent': ['Konstantinos 9101', 'Anonymi 7005'],
                                             'kin': ['Anonymae 102', 'Konstantinos 9101'],
                                             'kin (by blood)': ['Anonymi 6008']}
                                 },
                  'Ioannes 101': {'gender': ['Male'], 'identifier': 'Ἰωάννην',
                                  'death': {'count': 1, 'dated': 0},
                                  'legalrole': {'Archbishop': 1, 'Monk': 3}},
                  'Ioannes 102': {'gender': ['Eunuch'], 'identifier': 'Ἰωάννην',
                                  'legalrole': {'Archbishop': 1, 'Bishop': 1, 'Metropolitan': 1, 'Protoproedros': 1,
                                              'Protoproedros of the protosynkelloi': 1, 'Protosynkellos': 1,
                                              'Hypertimos': 1}},
                  'Ioannes 110': {'gender': ['Male'], 'identifier': 'Ἰωάννου...τοῦ Σκυλίτζη',
                                  'legalrole': {'Megas droungarios of the vigla': 1, 'Kouropalates': 1}},
                  'Konstantinos 62': {'gender': ['Male'], 'identifier': 'Κωνσταντίνῳ',
                                      'secondname': {'Δούκα': 1},
                                      'death': {'count': 2, 'dated': 0},
                                      'legalrole': {'Basileus': 1, 'Porphyrogennetos': 1},
                                      'kinship': {'son': ['Michael 7', 'Maria 61'],
                                                  'fiancé': ['Anna 62'],
                                                  'grandson': ['Konstantinos 10'],
                                                  'husband (betrothed)': ['Helena 101'],
                                                  'husband (proposed)': ['Helena 101'],
                                                  'son (only)': ['Maria 61'],
                                                  'son-in-law': ['Alexios 1', 'Eirene 61']},
                                      'possession': ['An estate, Pentegostis, near Serres, with excellent water and '
                                                     'buildings to house the imperial entourage']},
                  'Konstantinos 64': {'gender': ['Eunuch'], 'identifier': 'Κωνσταντῖνος', 'altname': {'Θεοδώρῳ': 1},
                                      'death': {'count': 1, 'dated': 0},
                                      'legalrole': {'Domestikos of the eastern tagmata': 1, 'Nobelissimos': 1,
                                                    'Domestikos of the scholai': 2, 'Proedros': 1, 'Vestarches': 1,
                                                    'Domestikos': 1, 'Megas domestikos': 1, 'Doux': 1, 'Monk': 2},
                                      'occupation': {'Beggar': 1},
                                      'kinship': {'brother': ['Michael 4', 'Ioannes 68'], 'uncle': ['Michael 5']},
                                      'possession': ['Estates in Opsikion where he was banished by Zoe 1',
                                                     'A house with a cistern near the Holy Apostles '
                                                     '(in Constantinople)']},
                  'Konstantinos 101': {'gender': ['Male'], 'identifier': 'Κωνσταντῖνος ὁ Διογένης',
                                       'secondname': {'Διογένης': 6},
                                       'death': {'count': 2, 'dated': 0},
                                       'legalrole': {'Doux': 1, 'Patrikios': 1, 'Strategos': 1,
                                                     'Archon': 1, 'Monk': 1},
                                       'kinship': {'husband': ['Anonyma 108'],
                                                   'father': ['Romanos 4'],
                                                   'husband of niece': ['Romanos 3'],
                                                   'nephew (by marriage)': ['Romanos 3']}},
                  'Konstantinos 102': {'gender': ['Male'], 'identifier': 'Κωνσταντίνῳ',
                                       'secondname': {'Βοδίνῳ': 3},
                                       'altname': {'Πέτρον ἀντὶ Κωνσταντίνου μετονομάσαντες': 1},
                                       'legalrole': {'King': 1, 'Basileus': 1},
                                       'kinship': {'son': ['Michael 101'], 'father': ['Georgios 20253']}},
                  'Konstantinos 110': {'gender': ['Male'], 'identifier': 'Κωνσταντῖνος',
                                       'legalrole': {'Patrikios': 1},
                                       'kinship': {'nephew': ['Michael 4']}},
                  'Liparites 101': {'gender': ['Male'], 'identifier': 'τοῦ Λιπαρίτου قاريط ملك الابخاز',
                                    'ethnicity': {'Georgian': 2}, 'legalrole': {'Lord': 1}}
                  }

    def setUp(self):
        self.graphdriver = GraphDatabase.driver(config.graphuri, auth=(config.graphuser, config.graphpw))
        self.constants = RELEVEN.PBWstarConstants.PBWstarConstants(None, self.graphdriver)
        # Get the UUIDs for each of our test people
        c = self.constants
        q = 'MATCH (id:%s)<-[:%s]-(idass:%s)-[:%s]->(person:%s), (idass)-[:%s]->(agent:%s) ' \
            'WHERE id.value IN %s AND agent.identifier = "Prosopography of the Byzantine World" ' \
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
            pbwagent = '%s {identifier: "Prosopography of the Byzantine World"}' % c.get_label('E39')
            q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(id:%s), (a)-[:%s]->(pbw:%s) WHERE p.uuid = '%s' RETURN id.value" \
                % (c.get_label('E21'), c.star_subject, c.get_label('E13'), c.star_object, c.get_label('E41'),
                   c.star_auth, pbwagent, pinfo['uuid'])
            with self.graphdriver.session() as session:
                result = session.run(q).single()
                self.assertIsNotNone(result)
                self.assertEqual(pinfo['identifier'], result['id.value'], "Test identifier for %s" % person)

    def test_appellation(self):
        """Test that each person has the second or alternative names assigned, as sourced assertions"""
        c = self.constants
        for person, pinfo in self.testpeople.items():
            # Find the death event for each person; make sure there is only one and check for the
            # correct number of assertions about it
            q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(devent:%s) WHERE p.uuid = '%s' RETURN devent" \
                % (c.get_label('E21'), c.star_object, c.get_label('E13'), c.star_subject, c.get_label('E69'),
                   pinfo['uuid'])
            with self.graphdriver.session() as session:
                result = session.run(q).single()
                self.assertIsNotNone(result)
                self.assertEqual(pinfo['identifier'], result['id.value'], "Test identifier for %s" % person)

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
                # Each event should have N description assertions
                q = "MATCH (de:%s)<-[:%s]-(a:%s)-[:%s]->(desc:%s) WHERE de.uuid = '%s' " \
                    "RETURN COUNT(desc.value) AS num" \
                    % (c.get_label('E69'), c.star_subject, c.get_label('E13'), c.star_object,
                       c.get_label('E62'), devent)
                with self.graphdriver.session() as session:
                    result = session.run(q).single()['num']
                    self.assertEquals(pinfo['death']['count'], result)
                # and M date assertions.
                q = "MATCH (de:%s)<-[:%s]-(a:%s)-[:%s]->(dating:%s) WHERE de.uuid = '%s' " \
                    "RETURN COUNT(dating.value) AS num" \
                    % (c.get_label('E69'), c.star_subject, c.get_label('E13'), c.star_object,
                       c.get_label('E52'), devent)
                with self.graphdriver.session() as session:
                    result = session.run(q).single()['num']
                    self.assertEquals(pinfo['death']['dated'], result)
                # Each event should also have N different sources.
                q = "MATCH (de:%s)<-[:%s]-(a:%s)-[:%s]->(sref) WHERE de.uuid = '%s' RETURN COUNT(sref.uuid) AS num" \
                    % (c.get_label('E69'), c.star_subject, c.get_label('E13'), c.star_source, devent)
                with self.graphdriver.session() as session:
                    result = session.run(q).single()['num']
                    self.assertEquals(pinfo['death']['count'], result)

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
                       c.star_predicate, c.get_label('P107'), c.get_label('P2'), pinfo['uuid'])
                with self.graphdriver.session() as session:
                    result = session.run(q)
                    rowct = 0
                    for row in result:
                        rowct += 1
                        self.assertTrue(row['eth'] in eths)
                        self.assertEqual(eths[row['eth']], row['act'])
                    self.assertEqual(sum(eths.values()), rowct)

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
                       c.star_predicate, c.get_label('P36'), pinfo['uuid'])
                with self.graphdriver.session() as session:
                    # We are cheating by knowing that no test person has more than one religion specified
                    result = session.run(q).single()
                    self.assertIsNotNone(result)
                    self.assertTrue(result['rel']['value'] in rels)
                    self.assertIn('Georgios Tornikes', result['auth']['descname'])

    def test_societyrole(self):
        """Test that occupations / legal designations are set correctly"""
        pass

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
        pass

    def test_possession(self):
        pass

    def test_db_entry(self):
        pass

    def tearDown(self):
        self.graphdriver.close()


if __name__ == '__main__':
    unittest.main()


output = '''
*** Making/finding node for person Anna 62 ***
Ingested 1 Death factoid(s)
Ingested 26 Dignity/Office factoid(s)
Adding second name Komnenos (gr 'Κομνηνοῦ')
Adding second name Komnenos (gr 'Κομνηνοῦ')
Adding second name Doukas (gr 'Δούκα')
Ingested 3 Second Name factoid(s)
Ingested 48 Kinship factoid(s)
Ingested 1 Occupation/Vocation factoid(s)
Ingested 1 Religion factoid(s)
*** Making/finding node for person Anna 101 ***
Ingested 5 Kinship factoid(s)
Ingested 1 Occupation/Vocation factoid(s)
Adding alternative name Arete (gr 'Ἀρετῆς')
Ingested 1 Alternative Name factoid(s)
*** Making/finding node for person Anna 102 ***
Ingested 1 Death factoid(s)
Ingested 2 Kinship factoid(s)
Ingested 1 Occupation/Vocation factoid(s)
*** Making/finding node for person Apospharios 101 ***
Ingested 1 Kinship factoid(s)
Ingested 1 Occupation/Vocation factoid(s)
*** Making/finding node for person Balaleca 101 ***
Ingested 1 Language Skill factoid(s)
*** Making/finding node for person Herve 101 ***
Ingested 3 Dignity/Office factoid(s)
Ingested 1 Ethnic label factoid(s)
Adding second name Phrangopolos, Phrangopoulos (gr 'Φραγγόπωλον')
Adding second name Phrangopolos, Phrangopoulos (gr 'Φραγγόπωλον')
Ingested 2 Second Name factoid(s)
Ingested 1 Possession factoid(s)
*** Making/finding node for person Ioannes 62 ***
Ingested 1 Death factoid(s)
Ingested 28 Dignity/Office factoid(s)
Adding second name Doukas (gr 'Δούκα')
Adding second name Doukas (gr 'Δούκα')
Adding second name Doukas (gr 'Δούκα')
Adding second name Doukas (gr 'Δούκα')
Adding second name Doukas (gr 'Δούκα')
Adding second name Doukas (gr 'Δούκα')
Ingested 6 Second Name factoid(s)
Ingested 54 Kinship factoid(s)
Ingested 6 Occupation/Vocation factoid(s)
Ingested 1 Possession factoid(s)
Adding alternative name Ignatios (gr 'Ἰγνάτιος')
Ingested 1 Alternative Name factoid(s)
*** Making/finding node for person Ioannes 68 ***
Ingested 4 Death factoid(s)
Ingested 14 Dignity/Office factoid(s)
Ingested 38 Kinship factoid(s)
Ingested 9 Occupation/Vocation factoid(s)
*** Making/finding node for person Ioannes 101 ***
Ingested 1 Death factoid(s)
Ingested 3 Dignity/Office factoid(s)
Ingested 3 Occupation/Vocation factoid(s)
*** Making/finding node for person Ioannes 102 ***
Ingested 21 Dignity/Office factoid(s)
*** Making/finding node for person Ioannes 110 ***
Ingested 2 Dignity/Office factoid(s)
Adding second name Skylitzes (gr 'τοῦ Σκυλίτζη')
Ingested 1 Second Name factoid(s)
*** Making/finding node for person Konstantinos 62 ***
Ingested 2 Death factoid(s)
Ingested 11 Dignity/Office factoid(s)
Adding second name Doukas (gr 'Δούκα')
Ingested 1 Second Name factoid(s)
Ingested 21 Kinship factoid(s)
Ingested 1 Possession factoid(s)
*** Making/finding node for person Konstantinos 101 ***
Ingested 2 Death factoid(s)
Ingested 10 Dignity/Office factoid(s)
Adding second name Diogenes (gr 'Διογένης')
Adding second name Diogenes (gr 'Διογένης')
Adding second name Diogenes (gr 'Διογένης')
Adding second name Diogenes (gr 'Διογένης')
Adding second name Diogenes (gr 'Διογένης')
Adding second name Diogenes (gr 'Διογένης')
Ingested 6 Second Name factoid(s)
Ingested 10 Kinship factoid(s)
Ingested 1 Occupation/Vocation factoid(s)
*** Making/finding node for person Konstantinos 102 ***
Ingested 3 Dignity/Office factoid(s)
Adding second name Bodin (gr 'Βοδίνῳ')
Adding second name Bodin (gr 'Βοδίνῳ')
Adding second name Bodin (gr 'Βοδίνῳ')
Ingested 3 Second Name factoid(s)
Ingested 2 Kinship factoid(s)
Adding alternative name Petros (gr 'Πέτρον ἀντὶ Κωνσταντίνου μετονομάσαντες')
Ingested 1 Alternative Name factoid(s)
*** Making/finding node for person Konstantinos 110 ***
Ingested 1 Dignity/Office factoid(s)
Ingested 1 Kinship factoid(s)
*** Making/finding node for person Liparites 101 ***
Ingested 1 Dignity/Office factoid(s)
Ingested 2 Ethnic label factoid(s)
Ingested 3 Kinship factoid(s)
*** Created 1146 new assertions *** '''