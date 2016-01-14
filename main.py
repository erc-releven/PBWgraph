# coding=utf-8
import unittest
import pbw
import config
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class TestConnections(unittest.TestCase):
    session = None

    @classmethod
    def setUpClass(cls):
        engine = create_engine('mysql+pymysql://' + config.dbstring)
        smaker = sessionmaker(bind=engine)
        cls.session = smaker()

    def lookup_person(self, name, num):
        q = self.session.query(pbw.Person).filter_by(name=name, mdbCode=num)
        return q.scalar()

    def test_persons(self):
        alexioi = self.session.query(pbw.Person).filter_by(name="Alexios").all()
        self.assertEqual(len(alexioi), 63)

    def test_person_attributes(self):
        alexios5 = self.lookup_person('Alexios', 5)
        self.assertEqual("%s" % alexios5, '<Alexios 5>')
        self.assertEqual(alexios5.sex, 'Male')
        self.assertEqual(alexios5.origLang, 'Greek')

    def test_person_factoids(self):
        alexios1 = self.lookup_person('Alexios', 1)
        self.assertEqual(len(alexios1.main_factoids()), 1990)
        self.assertEqual(len(alexios1.main_factoids('Narrative')), 1223)
        self.assertEqual(len(alexios1.main_factoids('Authorship')), 2)
        self.assertEqual(len(alexios1.main_factoids('Death')), 7)
        self.assertEqual(len(alexios1.main_factoids('Description')), 110)
        self.assertEqual(len(alexios1.main_factoids('Dignity/Office')), 87)
        self.assertEqual(len(alexios1.main_factoids('Second Name')), 34)
        self.assertEqual(len(alexios1.main_factoids('Kinship')), 138)
        self.assertEqual(len(alexios1.main_factoids('Location')), 387)
        self.assertEqual(len(alexios1.main_factoids('Possession')), 2)
        self.assertEqual(len(alexios1.ref_factoids()), 2585)
        for t in ['(Unspecified)', 'Education', 'Ethnic label', 'Language Skill', 'Occupation/Vocation',
                  'Religion', 'Eunuchs', 'Alternative Name', 'Uncertain Ident']:
            self.assertListEqual(alexios1.main_factoids(t), [])
        self.assertIsNone(alexios1.may_also_be())

    def test_alter_ego(self):
        person = self.lookup_person('Niketas', 20214)
        alters = {self.lookup_person('Niketas', 4001), self.lookup_person('Niketas', 20215)}
        self.assertSetEqual(person.may_also_be(), alters)

    def test_factoid_attributes(self):
        mourtzouphlos = self.session.query(pbw.Factoid).filter_by(factoidKey=228700).scalar()
        alexios5 = self.lookup_person('Alexios', 5)
        sourceref = "%s %s" % (mourtzouphlos.source, mourtzouphlos.sourceRef)
        self.assertEqual(sourceref, 'Kleinchroniken 63.3, 150.92, 174.1, 229.8, 298.1, 534.38')
        self.assertEqual(mourtzouphlos.origLang, '(Unspecified)')
        self.assertEqual(mourtzouphlos.origLDesc, "τὸν λεγ\xe1\xbd\xb9μενον Μο\xe1\xbd\xbbρτζουφλον")
        self.assertEqual(mourtzouphlos.factoidType, 'Alternative Name')
        self.assertEqual(len(mourtzouphlos.persons), 1)
        self.assertEqual(mourtzouphlos.persons[0], alexios5)

    def test_variant_name(self):
        alexios5 = self.lookup_person('Alexios', 5)
        vnamefact = [x for x in alexios5.factoids if x.factoidType == 'Alternative Name']
        self.assertEqual(len(vnamefact), 1)
        self.assertEqual(vnamefact[0].vnameInfo.name, 'Mourtzouphlos')

    def test_language_skill(self):
        person = self.lookup_person('Paulos', 125)
        count = 0
        for factoid in person.factoids:
            if factoid.languageSkill is not None:
                count += 1
                self.assertEqual(factoid.languageSkill, 'Latin')
        self.assertEqual(count, 2)

    def test_possession(self):
        person = self.lookup_person('Merotas', 101)
        count = 0
        for factoid in person.factoids:
            if factoid.possession is not None:
                count += 1
                self.assertEqual(factoid.possession, 'Bequeathed by his parents')
        self.assertEqual(count, 1)

    def test_occupation(self):
        person = self.lookup_person('Klemes', 102)
        occupation_factoids = person.main_factoids('Occupation/Vocation')
        self.assertEqual(len(occupation_factoids), 2)
        expected_occupations = {'Monk', 'Presbyter'}
        self.assertSetEqual(set([x.occupation for x in occupation_factoids]), expected_occupations)

    def test_religion(self):
        person = self.lookup_person('Niphon', 17001)
        religion_factoids = person.main_factoids('Religion')
        self.assertEqual(len(religion_factoids), 2)
        expected_religions = {None, 'Bogomil'}
        self.assertSetEqual(set([x.religion for x in religion_factoids]), expected_religions)

    def test_death(self):
        person = self.lookup_person('Alexios', 1)
        death_factoids = person.main_factoids('Death')
        self.assertEqual(len(death_factoids), 7)
        for df in death_factoids:
            if len(df.deathRecord.sourceDate) > 0:
                self.assertRegexpMatches(df.deathRecord.sourceDate, 'August 15')

    def test_dignity(self):
        person = self.lookup_person('Nikephoros', 117)
        dignity_factoids = person.main_factoids('Dignity/Office')
        self.assertEqual(len(dignity_factoids), 17)
        expected_dignities = set(self.session.query(pbw.DignityOffice).filter(
            pbw.DignityOffice.stdName.in_(['Despotes', 'Kaisar', 'Majesty', 'Panhypersebastos'])).all())
        self.assertSetEqual(set([x.dignityOffice for x in dignity_factoids]), expected_dignities)

    def test_ethnicity(self):
        person = self.lookup_person('Toros', 101)
        ethnicity_factoids = person.main_factoids('Ethnic label')
        self.assertEqual(len(ethnicity_factoids), 1)
        ef = ethnicity_factoids[0]
        self.assertEqual(ef.ethnicityInfo.isDoubtful, 0)
        self.assertEqual(ef.ethnicityInfo.ethnicity.ethName, 'Armenian')

    def test_location(self):
        person = self.lookup_person('Isaakios', 1)
        location_factoids = person.main_factoids('Location')
        self.assertEqual(len(location_factoids), 83)
        expected_locations = {'Almeas (?)', 'Asia', 'Athos: Amalphenon', 'Bosporos', 'Chrysopolis (Bithynia)',
                              'Constantinople', 'Constantinople: Blachernai',
                              'Constantinople: Blachernai: Hagia Thekla', 'Constantinople: Great Palace',
                              'Constantinople: Hagia Sophia', 'Constantinople: Mangana', 'Constantinople: Stoudios',
                              'Damatrys', 'Gounaria', 'Honoratos', 'Iberia (theme)', 'Kastamon (Paphlagonia)',
                              'Lobitzos (Bulgaria)', 'Marmara', 'Nea Mone, Chios', 'Neapolis (on Bosporos)', 'Nicaea',
                              'Nikomedeia', 'Orient', 'Pemolissa  (Asia Minor)', 'Reai (Bithynia)', 'Sangarios (river)',
                              'Serdica (Bulgaria)', 'Theotokos Dekapolitissa (Mitylene?)',
                              'Triaditza (Serdica, Bulgaria)', 'Vaspurakan'}
        self.assertSetEqual(set([x.locationInfo.location.locName for x in location_factoids]), expected_locations)

    def test_kinship(self):
        person = self.lookup_person('Michael', 11)
        kinship_factoids = person.main_factoids('Kinship')
        self.assertEqual(len(kinship_factoids), 17)
        expected_relatives = set(self.session.query(pbw.Person).filter(
            pbw.Person.personKey.in_([157545, 157472, 157544, 107051, 107553, 107962])
        ))
        actual_relatives = set()
        for kf in kinship_factoids:
            for rf in kf.referents():
                actual_relatives.add(rf)
        self.assertSetEqual(expected_relatives, actual_relatives)

    def test_narrative_unit(self):
        nunit = self.session.query(pbw.NarrativeUnit).filter_by(narrativeUnitID=21904).scalar()
        self.assertEqual(len(nunit.unitFactoids), 3)
        self.assertEqual(nunit.dateType, 'Internal date - approximate')
        yearUnit = nunit.yearUnit
        reignUnit = nunit.reignUnit
        self.assertEqual(yearUnit.description, '1045')
        self.assertEqual(reignUnit.description, 'KONSTANTINOS IX (1042-1055)')
        self.assertEqual(len(nunit.parentUnits), 2)
        self.assertEqual(len(nunit.childUnits), 0)
        self.assertEqual(len(yearUnit.childUnits), 29)
        self.assertIn(yearUnit, nunit.parentUnits)
        self.assertIn(reignUnit, nunit.parentUnits)

    # Suspect cursus is not really used...
    def test_cursus(self):
        person = self.lookup_person('Alp Arslan', 51)
        cursus_items = person.cursusItems
        self.assertEqual(len(cursus_items), 6)
        for ci in cursus_items:
            self.assertIsNotNone(ci.scDate)

    def test_activity(self):
        factoidWith = self.session.query(pbw.Factoid).filter_by(factoidKey=222270).scalar()
        self.assertIsNotNone(factoidWith.activityRecord)
        factoidEmpty = self.session.query(pbw.Factoid).filter_by(factoidKey=221949).scalar()
        self.assertIsNone(factoidEmpty.activityRecord)
        factoidWithout = self.session.query(pbw.Factoid).filter_by(factoidKey=220079).scalar()
        self.assertIsNone(factoidWithout.activityRecord)

    def test_boulloterion(self):
        boull = self.session.query(pbw.Boulloterion).filter_by(boulloterionKey=4087).scalar()
        self.assertEqual(boull.origLang, 'Greek')
        self.assertEqual(boull.scDate.year, 1000)
        self.assertEqual(boull.scDate.accuracy, 'reliable')
        self.assertEqual(boull.dateWords, '10th-11th c.')
        self.assertEqual(boull.obvType, 'Bust')
        self.assertIsNone(boull.revType)
        self.assertEqual(len(boull.seals), 27)
        expected_collections = ['Preslav, Museum of Archaeology ', 'Preslav, Archaeology Museum ',
                                'Munich, Zarnitz collection ', 'Shumen, Regional Historical Museum ',
                                'Bulgaria (private collection)']
        for s in boull.seals:
            self.assertIn(s.collection.shortName, expected_collections)
        self.assertEqual(boull.found.foundDesc, 'Preslav')
        self.assertEqual(boull.found.country, 'Bulgaria')
        self.assertEqual(len(boull.publication), 4)
        expected_pubs = {'Jordanov, Preslav': 'no. 32- 54', 'Seibt, review of Jordanov, Preslav': 'p. 135',
                         'Jordanov, Corpus II': 'no. 482- 511', 'Jordanov - Zhekova, Shumen': 'no. 132'}
        for p in boull.publication:
            self.assertEqual(p.publicationRef, expected_pubs[p.bibSource.shortName])


if __name__ == '__main__':
    unittest.main()
