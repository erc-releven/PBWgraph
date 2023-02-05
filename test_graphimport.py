import unittest
import PBWstarConstants
import config
from neo4j import GraphDatabase


class GraphImportTests(unittest.TestCase):

    graphdriver = None
    constants = None
    # Herve 101, Ioannes 62, 68, 101, 102, Konstantinos 62, 101, 102, Anna 62, 101, 102
    testpeople = {'Herve 101': {'gender': ['Male'], 'identifier': 'Ἐρβέβιον τὸν Φραγγόπωλον'},
                  'Ioannes 62': {'gender': ['Male'], 'identifier': 'Ἰωάννης'},
                  'Ioannes 68': {'gender': ['Eunuch'], 'identifier': 'τοῦ Ὀρφανοτρόφου'},
                  'Ioannes 101': {'gender': ['Male'], 'identifier': 'Ἰωάννην'},
                  'Ioannes 102': {'gender': ['Eunuch'], 'identifier': 'Ἰωάννην'},
                  'Konstantinos 62': {'gender': ['Male'], 'identifier': 'Κωνσταντίνῳ'},
                  'Konstantinos 101': {'gender': ['Male'], 'identifier': 'Κωνσταντῖνος ὁ Διογένης'},
                  'Konstantinos 102': {'gender': ['Male'], 'identifier': 'Κωνσταντίνῳ'},
                  'Anna 62': {'gender': ['Female'], 'identifier': 'Ἄννα Κομνηνή',
                              'appellation': {'Κομνηνοῦ': 2, 'Δούκα': 1}},
                  'Anna 101': {'gender': ['Female'], 'identifier': 'Ἄννα',
                              'appellation': {'Ἀρετῆς': 1}},
                  'Anna 102': {'gender': ['Female'], 'identifier': ' Ἄννῃ'}}

    def setUp(self):
        self.graphdriver = GraphDatabase.driver(config.graphuri, auth=(config.graphuser, config.graphpw))
        self.constants = PBWstarConstants.PBWstarConstants(None, self.graphdriver)
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
                "WHERE p.uuid = '%s' RETURN p.descname, gender.value" % (c.get_label('E21'), c.star_object,
                                                                         c.get_label('E13'), c.star_subject,
                                                                         c.get_label('E17'), c.star_subject,
                                                                         c.get_label('E13'), c.star_object,
                                                                         c.get_label('C11'), pinfo['uuid'])
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
            q = "MATCH (p:%s)<-[:%s]-(a:%s)-[:%s]->(id:%s), (a)-[:%s]->(pbw:%s) " \
                "WHERE p.uuid = '%s' RETURN id.value" % (c.get_label('E21'), c.star_subject,
                                                                         c.get_label('E13'), c.star_object,
                                                                         c.get_label('E41'), c.star_auth,
                                                                         pbwagent, pinfo['uuid'])
            with self.graphdriver.session() as session:
                result = session.run(q).single()
                self.assertIsNotNone(result)
                self.assertEqual(pinfo['identifier'], result['id.value'], "Test identifier for %s" % person)

    def test_appellation(self):
        """Test that each person has the second or alternative names assigned, as sourced assertions"""
        pass

    def test_death(self):
        pass

    def test_ethnicity(self):
        pass

    def test_religion(self):
        pass

    def test_societyrole(self):
        pass

    def test_language(self):
        pass

    def test_description(self):
        pass

    def test_kinship(self):
        pass

    def test_possession(self):
        pass

    def test_db_entry(self):
        pass

    def tearDown(self):
        self.graphdriver.close()


if __name__ == '__main__':
    unittest.main()


scriptoutput = """
/Users/tla/Projects/PBWgraph/venv/bin/python /Users/tla/Projects/PBWgraph/graphimportSTAR.py 
Setting up PBW constants...
Found 10172 relevant people
*** Making/finding node for person Anna 62 ***
Ingested 1 Death factoid(s)
Ingested 12 Description factoid(s)
Adding second name Komnenos (gr 'Κομνηνοῦ')
Adding second name Komnenos (gr 'Κομνηνοῦ')
Adding second name Doukas (gr 'Δούκα')
Ingested 3 Second Name factoid(s)
Ingested 48 Kinship factoid(s)
Ingested 1 Occupation/Vocation factoid(s)
Ingested 1 Religion factoid(s)
*** Making/finding node for person Anna 101 ***
Ingested 1 Description factoid(s)
Ingested 5 Kinship factoid(s)
Ingested 1 Occupation/Vocation factoid(s)
Adding alternative name Arete (gr 'Ἀρετῆς')
Ingested 1 Alternative Name factoid(s)
*** Making/finding node for person Anna 102 ***
Ingested 1 Death factoid(s)
Ingested 1 Description factoid(s)
Ingested 2 Kinship factoid(s)
Ingested 1 Occupation/Vocation factoid(s)
*** Making/finding node for person Herve 101 ***
Ingested 4 Description factoid(s)
Ingested 1 Ethnic label factoid(s)
Adding second name Phrangopolos, Phrangopoulos (gr 'Φραγγόπωλον')
Adding second name Phrangopolos, Phrangopoulos (gr 'Φραγγόπωλον')
Ingested 2 Second Name factoid(s)
Ingested 1 Possession factoid(s)
*** Making/finding node for person Ioannes 62 ***
Ingested 1 Death factoid(s)
Ingested 26 Description factoid(s)
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
Ingested 55 Description factoid(s)
Ingested 38 Kinship factoid(s)
Ingested 9 Occupation/Vocation factoid(s)
*** Making/finding node for person Ioannes 101 ***
Ingested 1 Death factoid(s)
Ingested 1 Description factoid(s)
Ingested 3 Occupation/Vocation factoid(s)
*** Making/finding node for person Ioannes 102 ***
Ingested 4 Description factoid(s)
*** Making/finding node for person Konstantinos 62 ***
Ingested 2 Death factoid(s)
Ingested 12 Description factoid(s)
Adding second name Doukas (gr 'Δούκα')
Ingested 1 Second Name factoid(s)
Ingested 21 Kinship factoid(s)
Ingested 1 Possession factoid(s)
*** Making/finding node for person Konstantinos 101 ***
Ingested 2 Death factoid(s)
Ingested 3 Description factoid(s)
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
Ingested 3 Description factoid(s)
Adding second name Bodin (gr 'Βοδίνῳ')
Adding second name Bodin (gr 'Βοδίνῳ')
Adding second name Bodin (gr 'Βοδίνῳ')
Ingested 3 Second Name factoid(s)
Ingested 2 Kinship factoid(s)
Adding alternative name Petros (gr 'Πέτρον ἀντὶ Κωνσταντίνου μετονομάσαντες')
Ingested 1 Alternative Name factoid(s)
*** Created 1019 new assertions ***
Processed 11 person records.
Used the following sources: ['Anna Komnene', 'Aristakes', 'Attaleiates: History', 'Boilas', 'Bryennios', 'Christophoros of Mitylene', 'Christos Philanthropos, note', 'Council of 1147', 'Eustathios: Capture of Thessalonike', 'Glykas', 'Italikos', 'Kecharitomene', 'Kinnamos', 'Manasses, Chronicle', 'Nea Mone,', 'Niketas Choniates, Historia', 'Patmos: Acts', 'Prodromos, Historische Gedichte', 'Psellos', 'Psellos: Chronographia', 'Seals', 'Skylitzes', 'Skylitzes Continuatus', 'Tornikes, Georgios', 'Zonaras']
Done! Ran in 0:02:36.138840

Process finished with exit code 0
"""