import unittest
import RELEVEN.PBWstarConstants
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
