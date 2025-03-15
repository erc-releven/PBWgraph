import re
from sqlalchemy import Column, ForeignKey, Table  # DB components
from sqlalchemy import DateTime, Integer, SmallInteger, String, Text  # Column types
from sqlalchemy.orm import declarative_base, relationship, backref
from sqlalchemy.ext.associationproxy import association_proxy

Base = declarative_base()


# ## Simple key-value lookup tables
class Accuracy(Base):
    __tablename__ = 'Accuracy'
    accuracyName = Column(String)
    accKey = Column(Integer, primary_key=True)


class Bibliography(Base):
    __tablename__ = 'Bibliography'
    date = Column(SmallInteger)
    bibKey = Column(Integer, primary_key=True)
    greekBib = Column(Text)
    reference = Column(Text)
    latinBib = Column(Text)
    red = Column(Integer)
    shortName = Column(Text)


# N.B. there are several identical collection records for each ID! Weird.
class Collection(Base):
    __tablename__ = 'Collection'
    collectionName = Column(Text)
    red = Column(Integer)
    collectionKey = Column(Integer, primary_key=True)
    shortName = Column(Text)
    baseURL = Column(Text)


class Country(Base):
    __tablename__ = 'Country'
    countryName = Column(Text)
    countryKey = Column(Integer, primary_key=True)


class DateTypes(Base):
    __tablename__ = 'DateTypes'
    dateTypeKey = Column(SmallInteger, primary_key=True)
    dateType = Column(String)


class DignityOfficeType(Base):
    __tablename__ = 'DignityOfficeType'
    dotName = Column(String)
    dotKey = Column(Integer, primary_key=True)


class FactoidPersonType(Base):
    __tablename__ = 'FactoidPersonType'
    fpTypeName = Column(String)
    fpTypeKey = Column(Integer, primary_key=True)


class FactoidType(Base):
    __tablename__ = 'FactoidType'
    typeName = Column(String)
    factoidTypeKey = Column(Integer, primary_key=True)


class Figure(Base):
    __tablename__ = 'Figure'
    figureKey = Column(Integer, primary_key=True)
    figureName = Column(Text)


class KinshipType(Base):
    __tablename__ = 'KinshipType'
    tstamp = Column(DateTime)
    kinKey = Column(SmallInteger, primary_key=True)
    gspecRelat = Column(String)
    creationDate = Column(DateTime)
    gunspecRelat = Column(String)


class LanguageSkill(Base):
    __tablename__ = 'LanguageSkill'
    languageName = Column(String)
    langKey = Column(SmallInteger, primary_key=True)


class Occupation(Base):
    __tablename__ = 'Occupation'
    occupationKey = Column(SmallInteger, primary_key=True)
    occupationName = Column(String)


class OrigLangAuth(Base):
    __tablename__ = 'OrigLangAuth'
    oLangKey = Column(Integer, primary_key=True)
    oLanguage = Column(String)


class Religion(Base):
    __tablename__ = 'Religion'
    religionName = Column(String)
    religionKey = Column(Integer, primary_key=True)


class SexAuth(Base):
    __tablename__ = 'SexAuth'
    sexValue = Column(String)
    sexKey = Column(Integer, primary_key=True)


class Source(Base):
    __tablename__ = 'Source'
    sourceBib = Column(Text)
    sourceID = Column(String)
    sourceKey = Column(SmallInteger, primary_key=True)


class Type(Base):
    __tablename__ = 'Type'
    typeKey = Column(Integer, primary_key=True)
    typeName = Column(Text)


# ## Slightly less simple key/value lookup tables
class ActivityFactoid(Base):
    __tablename__ = 'ActivityFactoid'
    oLangKey = Column(Integer, ForeignKey('OrigLangAuth.oLangKey'))
    SourceDateOL = Column(Text)
    sourceDate = Column(Text)
    factoidKey = Column(Integer, ForeignKey('Factoid.factoidKey'), primary_key=True)
    tstanp = Column(DateTime)
    # Direct foreign key associations
    _oLangVal = relationship('OrigLangAuth')
    origLang = association_proxy('_oLangVal', 'oLanguage')
    # Only put the activity record on the Factoid object if there is any content.
    factoid = relationship('Factoid', backref=backref('activityRecord', uselist=False,
                                                      primaryjoin='and_(ActivityFactoid.factoidKey==Factoid.factoidKey,'
                                                                  ' ActivityFactoid.sourceDate!="")'))


class CollDB(Base):
    __tablename__ = 'CollDB'
    tstamp = Column(DateTime)
    corrector = Column(String)
    collDBKey = Column(SmallInteger, primary_key=True)
    collDBID = Column(String)
    cdbCreationDate = Column(DateTime)
    researcher = Column(String)
    creationDate = Column(DateTime)
    notes = Column(Text)
    cdbImportDate = Column(DateTime)
    sourceKey = Column(SmallInteger, ForeignKey('Source.sourceKey'))
    # Direct foreign key associations
    source = relationship('Source')


class DeathFactoid(Base):
    __tablename__ = 'DeathFactoid'
    oLangKey = Column(Integer, ForeignKey('OrigLangAuth.oLangKey'))
    tStamp = Column(DateTime)
    SourceDateOL = Column(Text)
    sourceDate = Column(Text)
    factoidKey = Column(Integer, ForeignKey('Factoid.factoidKey'), primary_key=True)
    # Direct foreign key associations
    _oLangVal = relationship('OrigLangAuth')
    origLang = association_proxy('_oLangVal', 'oLanguage')
    factoid = relationship('Factoid', backref=backref('deathRecord', uselist=False))


class Ethnicity(Base):
    __tablename__ = 'Ethnicity'
    tstamp = Column(DateTime)
    ethName = Column(String)
    ethnicityKey = Column(SmallInteger, primary_key=True)
    oLangKey = Column(Integer, ForeignKey('OrigLangAuth.oLangKey'))
    creationDate = Column(DateTime)
    ethNameOL = Column(String)
    # Direct foreign key associations
    _oLangVal = relationship('OrigLangAuth')
    origLang = association_proxy('_oLangVal', 'oLanguage')


class Found(Base):
    __tablename__ = 'Found'
    bibRef = Column(Text)
    # bibKey is unused
    # bibKey = Column(Text, ForeignKey('Bibliography.bibKey'))
    foundKey = Column(Integer, primary_key=True)
    boulloterionKey = Column(Integer, ForeignKey('Boulloterion.boulloterionKey'))
    countryKey = Column(Integer, ForeignKey('Country.countryKey'))
    foundDesc = Column(Text)
    # Direct foreign key associations
    boulloterion = relationship('Boulloterion', backref=backref('found', uselist=False))
    _countryInfo = relationship('Country')
    country = association_proxy('_countryInfo', 'countryName')


class Location(Base):
    __tablename__ = 'Location'
    tstamp = Column(DateTime)
    extraInfo = Column(String)
    locName = Column(String)
    oLangKey = Column(Integer, ForeignKey('OrigLangAuth.oLangKey'))
    locNameOL = Column(String)
    creationDate = Column(DateTime)
    notes = Column(Text)
    locationKey = Column(Integer, primary_key=True)
    ## latitude = Column(String)
    ## longitude = Column(String)
    ## radius = Column(String)
    ## geosource = Column(String)
    geonames_id = Column(Integer)
    pleiades_id = Column(Integer)
    # Direct foreign key associations
    _oLangVal = relationship('OrigLangAuth')
    origLang = association_proxy('_oLangVal', 'oLanguage')


class LocationSelector(Base):
    __tablename__ = 'LocationSelector'
    locSelKey = Column(SmallInteger, primary_key=True)
    locSelName = Column(String)
    # Direct foreign key associations
    locationKey = Column(Integer, ForeignKey('Location.locationKey'))

    locations = relationship('Location')


class HierarchyUnit(Base):
    __tablename__ = 'HierarchyUnit'
    narrativeUnitID = Column(Integer, ForeignKey('NarrativeUnit.narrativeUnitID'))
    hierarchyUnitID = Column(Integer, primary_key=True)
    parentID = Column(Integer, ForeignKey('HierarchyUnit.hierarchyUnitID'))
    Chronorder = Column(Integer)
    lft = Column(Integer)  # TODO What is this??
    rgt = Column(Integer)  # TODO What is this??
    treeID = Column(Integer)  # Unused
    # Direct foreign key associations
    narrativeUnit = relationship('NarrativeUnit', backref='hierarchyRecords')
    # Proxy foreign key setup
    _parent_hierarchy = relationship('HierarchyUnit', remote_side=[hierarchyUnitID], backref='_child_hierarchy')
    parentUnit = association_proxy('_parent_hierarchy', 'narrativeUnit')
    childUnits = association_proxy('_child_hierarchy', 'narrativeUnit')


class PossessionFactoid(Base):
    __tablename__ = 'PossessionFactoid'
    tstamp = Column(DateTime)
    possessionName = Column(String)
    factoidKey = Column(Integer, ForeignKey('Factoid.factoidKey'), primary_key=True)
    factoid = relationship('Factoid', backref=backref('possessionRecord', uselist=False))


class Published(Base):
    __tablename__ = 'Published'
    publishedOrder = Column(Integer)
    bibKey = Column(Integer, ForeignKey('Bibliography.bibKey'))
    boulloterionKey = Column(Integer, ForeignKey('Boulloterion.boulloterionKey'))
    publicationRef = Column(String)
    publishedKey = Column(Integer, primary_key=True)
    publicationPage = Column(String)
    # Direct foreign key associations
    bibSource = relationship('Bibliography')
    boulloterion = relationship('Boulloterion', backref='publication')


class ScDate(Base):
    __tablename__ = 'ScDate'
    priority = Column(Integer)
    tstamp = Column(DateTime)
    yearGivenForm = Column(String)
    yrOrder = Column(SmallInteger)
    ssRef = Column(Text)
    scDateKey = Column(Integer, primary_key=True)
    creationDate = Column(DateTime)
    notes = Column(Text)
    accKey = Column(Integer, ForeignKey('Accuracy.accKey'))
    factoidKey = Column(Integer, ForeignKey('Factoid.factoidKey'))
    year = Column(SmallInteger)
    sSourceKey = Column(Integer, ForeignKey('ScSource.sSourceKey'))

    _acc_info = relationship('Accuracy')
    accuracy = association_proxy('_acc_info', 'accuracyName')
    scSource = relationship('ScSource')
    factoid = relationship('Factoid', backref='scDate')


class ScSource(Base):
    __tablename__ = 'ScSource'
    tstamp = Column(DateTime)
    sSourceFullRef = Column(Text)
    sSourceID = Column(String)
    creationDate = Column(DateTime)
    notes = Column(Text)
    sSourceKey = Column(Integer, primary_key=True)


class Seal(Base):
    __tablename__ = 'Seal'
    sealOrder = Column(Integer)
    boulloterionKey = Column(Integer, ForeignKey('Boulloterion.boulloterionKey'))
    collectionKey = Column(Integer, ForeignKey('Collection.collectionKey'))
    sealKey = Column(Integer, primary_key=True)
    collectionRef = Column(Integer)
    # Direct foreign key refs
    collection = relationship('Collection', backref='seals')
    boulloterion = relationship('Boulloterion', backref='seals')


# Association tables
vname_association = Table('VNameFactoid', Base.metadata,
                          Column('vNameKey', Integer, ForeignKey('VariantName.vnameKey')),
                          Column('factoidKey', Integer, ForeignKey('Factoid.factoidKey')))

cursus_association = Table('FactoidCursus', Base.metadata,
                           Column('cursusKey', Integer, ForeignKey('Cursus.cursusKey')),
                           Column('factoidKey', Integer, ForeignKey('Factoid.factoidKey')))

family_association = Table('FamNameFactoid', Base.metadata,
                           Column('famNameKey', SmallInteger, ForeignKey('FamilyName.famNameKey')),
                           Column('factoidKey', Integer, ForeignKey('Factoid.factoidKey')))

kinship_association = Table('KinFactoid', Base.metadata,
                            Column('kinKey', SmallInteger, ForeignKey('KinshipType.kinKey')),
                            Column('factoidKey', Integer, ForeignKey('Factoid.factoidKey')))

boulloterion_figures = Table('BoulloterionFigure', Base.metadata,
                             Column('figureKey', Integer, ForeignKey('Figure.figureKey')),
                             Column('boulloterionKey', Integer, ForeignKey('Boulloterion.boulloterionKey')))

language_association = Table('LangFactoid', Base.metadata,
                             Column('factoidKey', Integer, ForeignKey('Factoid.factoidKey')),
                             Column('langKey', SmallInteger, ForeignKey('LanguageSkill.langKey')))

religion_association = Table('ReligionFactoid', Base.metadata,
                             Column('religionKey', SmallInteger, ForeignKey('Religion.religionKey')),
                             Column('factoidKey', Integer, ForeignKey('Factoid.factoidKey')))

occupation_association = Table('OccupationFactoid', Base.metadata,
                               Column('OccupationKey', SmallInteger, ForeignKey('Occupation.occupationKey')),
                               Column('factoidKey', Integer, ForeignKey('Factoid.factoidKey')))


# Association classes
class DignityFactoid(Base):
    __tablename__ = 'DignityFactoid'
    AppointedBy = Column(Text)
    cursusOrder = Column(SmallInteger)
    tstamp = Column(DateTime)
    dotKey = Column(Integer, ForeignKey('DignityOfficeType.dotKey'))
    stdName = Column(String)
    factoidKey = Column(Integer, ForeignKey('Factoid.factoidKey'), primary_key=True)
    doKey = Column(SmallInteger, ForeignKey('DignityOffice.doKey'))

    _doType = relationship('DignityOfficeType')
    doType = association_proxy('_doType', 'dotName')
    dignity = relationship('DignityOffice', backref='factoids')
    factoid = relationship('Factoid', backref=backref('_assoc_dignity', uselist=False), uselist=False)


class EthnicityFactoid(Base):
    __tablename__ = 'EthnicityFactoid'
    tstamp = Column(DateTime)
    ethnicityKey = Column(SmallInteger, ForeignKey('Ethnicity.ethnicityKey'))
    isDoubtful = Column(Integer)
    factoidKey = Column(Integer, ForeignKey('Factoid.factoidKey'), primary_key=True)

    ethnicity = relationship('Ethnicity')
    factoid = relationship('Factoid', backref=backref('ethnicityInfo', uselist=False), uselist=False)


class FactoidLocation(Base):
    __tablename__ = 'FactoidLocation'
    tstamp = Column(DateTime)
    oLangKey = Column(Integer, ForeignKey('OrigLangAuth.oLangKey'))
    srcName = Column(String)
    sigInfo = Column(Text)
    notes = Column(Text)
    factoidKey = Column(Integer, ForeignKey('Factoid.factoidKey'), primary_key=True)
    locationKey = Column(Integer, ForeignKey('Location.locationKey'))

    # Foreign key associations
    _oLangVal = relationship('OrigLangAuth')
    origLang = association_proxy('_oLangVal', 'oLanguage')
    location = relationship('Location')
    factoid = relationship('Factoid', backref=backref('locationInfo', uselist=False), uselist=False)


class FactoidPerson(Base):
    __tablename__ = 'FactoidPerson'
    fpTypeKey = Column(Integer, ForeignKey('FactoidPersonType.fpTypeKey'))
    fpKey = Column(Integer, primary_key=True)
    personKey = Column(Integer, ForeignKey('Person.personKey'))
    factoidKey = Column(Integer, ForeignKey('Factoid.factoidKey'))

    _fpType = relationship('FactoidPersonType')
    fpType = association_proxy('_fpType', 'fpTypeName')
    person = relationship('Person', lazy='joined', innerjoin=True, backref='_person_factoids')
    factoid = relationship('Factoid', lazy='joined', innerjoin=True, backref='_assoc_persons')


class NarrativeFactoid(Base):
    __tablename__ = 'NarrativeFactoid'
    fmKey = Column(Integer)  # TODO What is this?
    narrativeUnitID = Column(Integer, ForeignKey('NarrativeUnit.narrativeUnitID'), primary_key=True)
    hide = Column(Integer)
    factoidKey = Column(Integer, ForeignKey('Factoid.factoidKey'), primary_key=True)
    chronOrder = Column(SmallInteger)

    # Foreign key associations
    narrativeUnit = relationship('NarrativeUnit', lazy='joined', innerjoin=True, backref='_assoc_factoids')
    factoid = relationship('Factoid', lazy='joined', innerjoin=True, backref=backref('_assoc_nunit', uselist=False))


class PersonCollDB(Base):
    __tablename__ = 'PersonCollDB'
    tstamp = Column(DateTime)
    personCollDBKey = Column(Integer, primary_key=True)
    nameOL = Column(String)
    cdbCode = Column(SmallInteger)
    collDBKey = Column(SmallInteger, ForeignKey('CollDB.collDBKey'))
    oLangKey = Column(Integer, ForeignKey('OrigLangAuth.oLangKey'))
    name = Column(String)
    notes = Column(Text)
    personKey = Column(Integer, ForeignKey('Person.personKey'))
    # Foreign key associations
    _oLangVal = relationship('OrigLangAuth')
    origLang = association_proxy('_oLangVal', 'oLanguage')
    # Association class stuff
    person = relationship('Person', lazy='joined', innerjoin=True, backref='collDBRef')
    collDB = relationship('CollDB', lazy='joined', innerjoin=True, backref='_assoc_persons')


# Primary entity tables
class Boulloterion(Base):
    __tablename__ = 'Boulloterion'
    scDateKey = Column(Integer, ForeignKey('ScDate.scDateKey'))
    obvTypeKey = Column(Integer, ForeignKey('Type.typeKey'))
    revIcon = Column(Text)
    hasImage = Column(Integer)
    revTypeKey = Column(String, ForeignKey('Type.typeKey'))
    dateWords = Column(Text)
    oLangKey = Column(Text, ForeignKey('OrigLangAuth.oLangKey'))
    diameter = Column(Text)
    boulloterionKey = Column(Integer, primary_key=True)
    obvIcon = Column(Text)
    text = Column(Text)
    title = Column(Text)
    origLText = Column(Text)

    # Direct foreign key associations
    _oLangVal = relationship('OrigLangAuth')
    origLang = association_proxy('_oLangVal', 'oLanguage')
    _obvType = relationship(Type, foreign_keys=[obvTypeKey])
    obvType = association_proxy('_obvType', 'typeName')
    _revType = relationship(Type, foreign_keys=[revTypeKey])
    revType = association_proxy('_revType', 'typeName')
    scDate = relationship('ScDate')

    # Associations through table
    _figRecords = relationship('Figure', secondary=boulloterion_figures)
    figures = association_proxy('_figRecords', 'figureName')


class Cursus(Base):
    __tablename__ = 'Cursus'
    cursusKey = Column(Integer, primary_key=True)
    cursusOrder = Column(SmallInteger)
    scDateKey = Column(Integer, ForeignKey('ScDate.scDateKey'))
    personKey = Column(Integer, ForeignKey('Person.personKey'))
    # Direct foreign key associations
    person = relationship('Person', backref='cursusItems')
    scDate = relationship('ScDate')
    # Association class backreferences
    cursusFactoids = relationship('Factoid', secondary=cursus_association)


class DignityOffice(Base):
    __tablename__ = 'DignityOffice'
    tstamp = Column(DateTime)
    parentKey = Column(SmallInteger, ForeignKey('DignityOffice.doKey'))
    oLangKey = Column(Integer, ForeignKey(OrigLangAuth.oLangKey))
    stdNameOL = Column(String)
    creationDate = Column(DateTime)
    lft = Column(SmallInteger)  # TODO what is this??
    dotKey = Column(Integer, ForeignKey(DignityOfficeType.dotKey))
    stdShortOL = Column(String)
    stdName = Column(String)
    rgt = Column(SmallInteger)  # TODO what is this??
    doKey = Column(SmallInteger, primary_key=True)

    # Direct foreign key associations
    _oLangVal = relationship('OrigLangAuth')
    origLang = association_proxy('_oLangVal', 'oLanguage')
    _dotVal = relationship('DignityOfficeType')
    type = association_proxy('_dotVal', 'dotName')
    parent = relationship('DignityOffice', remote_side=[doKey])


class Factoid(Base):
    __tablename__ = 'Factoid'
    tstamp = Column(DateTime)
    engDesc = Column(Text)
    sourceRef = Column(String)
    collDBKey = Column(SmallInteger, ForeignKey('CollDB.collDBKey'))
    oLangKey = Column(Integer, ForeignKey('OrigLangAuth.oLangKey'))
    needsAttn = Column(Integer)
    factoidTypeKey = Column(Integer, ForeignKey('FactoidType.factoidTypeKey'))
    boulloterionKey = Column(String, ForeignKey('Boulloterion.boulloterionKey'))
    creationDate = Column(DateTime)
    notes = Column(Text)
    factoidKey = Column(Integer, primary_key=True)
    origLDesc = Column(Text)
    sourceKey = Column(SmallInteger, ForeignKey(Source.sourceKey))

    # Direct foreign key associations
    _sourceVal = relationship('Source')
    source = association_proxy('_sourceVal', 'sourceID')
    _oLangVal = relationship('OrigLangAuth')
    origLang = association_proxy('_oLangVal', 'oLanguage')
    _fTypeVal = relationship('FactoidType')
    factoidType = association_proxy('_fTypeVal', 'typeName')
    boulloterion = relationship('Boulloterion')
    collDB = relationship('CollDB')

    # Indirect foreign key associations
    persons = association_proxy('_assoc_persons', 'person')

    # Subtype foreign key associations...
    # These are the types with no extra information.
    # - Authorship
    # - Description
    # - Education
    # - Eunuchs
    # - Uncertain Ident

    # These return a full record.
    vnameInfo = relationship('VariantName', secondary=vname_association, backref="factoidData", uselist=False)
    secondName = relationship('FamilyName', secondary=family_association, uselist=False)
    kinshipType = relationship('KinshipType', secondary=kinship_association, uselist=False)
    cursusRecord = relationship('Cursus', secondary=cursus_association, uselist=False, overlaps="cursusFactoids")

    # From association classes that return a record, we have:
    # - deathRecord
    # - ethnicityInfo
    # - locationInfo
    # - narrativeUnitInfo

    # These come from simple association tables and return a value.
    _langSkillInfo = relationship('LanguageSkill', secondary=language_association, uselist=False)
    languageSkill = association_proxy('_langSkillInfo', 'languageName')
    _occInfo = relationship('Occupation', secondary=occupation_association, uselist=False)
    occupation = association_proxy('_occInfo', 'occupationName')
    _relInfo = relationship('Religion', secondary=religion_association, uselist=False)
    religion = association_proxy('_relInfo', 'religionName')

    # These come from association classes and return a value or another class.
    dignityOffice = association_proxy('_assoc_dignity', 'dignity')
    possession = association_proxy('possessionRecord', 'possessionName')
    narrativeUnit = association_proxy('_assoc_nunit', 'narrativeUnit')

    def associated_person(self, refKey):
        """Return the Person object for the given key, which comes out of a PERSREF
        in the factoid text.
        :param refKey: the associated key"""
        for pf in self._assoc_persons:
            if pf.fpKey == refKey:
                return pf.person
        return None

    def main_person(self):
        """Return the Person objects for the primary person of this factoid.
        There will almost, but not quite, always be one, so we return a list."""
        main_persons = []
        for pf in self._assoc_persons:
            if pf.fpType == 'Primary':
                main_persons.append(pf.person)
        return main_persons

    def referents(self, check_persref=True):
        referent_objects = []
        for pf in self._assoc_persons:
            if pf.fpType == 'DescRef':
                referent_objects.append(pf.person)
        if check_persref:
            pattern = re.compile(r'<PERSREF ID="(\d+)"/>')
            persref_persons = set()
            for fpid in pattern.findall(self.engDesc):
                this_person = self.associated_person(int(fpid))
                if this_person is not None:
                    persref_persons.add(this_person)
            return [x for x in referent_objects if x in persref_persons]
        return referent_objects

    def _persref_as_str(self, m):
        p = self.associated_person(int(m.group(2)))
        return str(p)

    def replace_referents(self):
        desc = self.engDesc
        pattern = re.compile(r'(<PERSREF ID="(\d+)"/>)')
        return re.sub(pattern, self._persref_as_str, desc)


class FamilyName(Base):
    __tablename__ = 'FamilyName'
    tstamp = Column(DateTime)
    oLangKey = Column(Integer, ForeignKey('OrigLangAuth.oLangKey'))
    famNameKey = Column(SmallInteger, primary_key=True)
    famNameOL = Column(String)
    creationDate = Column(DateTime)
    famName = Column(String)

    # Direct foreign key associations
    _oLangVal = relationship('OrigLangAuth')
    origLang = association_proxy('_oLangVal', 'oLanguage')


class NarrativeUnit(Base):
    __tablename__ = 'NarrativeUnit'
    dateTypeKey = Column(Integer, ForeignKey('DateTypes.dateTypeKey'))
    number = Column(Integer)
    secondaryAttestation = Column(String)
    fmKey = Column(Integer)
    dates = Column(String)
    heading = Column(Integer)
    narrativeUnitID = Column(Integer, primary_key=True)
    event = Column(Integer, ForeignKey('NarrativeUnit.narrativeUnitID'))
    description = Column(Text)
    problem = Column(Integer)
    reign = Column(Integer, ForeignKey('NarrativeUnit.narrativeUnitID'))
    summary = Column(Text)
    notes = Column(Text)
    primaryAttestation = Column(String)
    year = Column(Integer, ForeignKey('NarrativeUnit.narrativeUnitID'))

    # Direct foreign key associations
    _dTypeVal = relationship('DateTypes')
    dateType = association_proxy('_dTypeVal', 'dateType')
    yearUnit = relationship('NarrativeUnit', foreign_keys=[year], remote_side=[narrativeUnitID])
    reignUnit = relationship('NarrativeUnit', foreign_keys=[reign], remote_side=[narrativeUnitID])
    eventUnit = relationship('NarrativeUnit', foreign_keys=[event], remote_side=[narrativeUnitID])
    # Indirect foreign key associations
    unitFactoids = association_proxy('_assoc_factoids', 'factoid')
    parentUnits = association_proxy('hierarchyRecords', 'parentUnit')
    _childUnitGroups = association_proxy('hierarchyRecords', 'childUnits')

    @property
    def childUnits(self):
        childSet = set()
        for c in self._childUnitGroups:
            childSet.update(c)
        return childSet


class Person(Base):
    __tablename__ = 'Person'
    floruit = Column(String)
    tstamp = Column(DateTime)
    firstDate = Column(SmallInteger)
    nameOL = Column(String)
    bibliography = Column(Text)
    mdbCode = Column(Integer)
    name = Column(String)
    oLangKey = Column(Integer, ForeignKey('OrigLangAuth.oLangKey'))
    lastDateType = Column(Integer)
    descName = Column(String)
    firstDateType = Column(Integer)
    lastDate = Column(SmallInteger)
    sexKey = Column(Integer, ForeignKey('SexAuth.sexKey'))
    creationDate = Column(DateTime)
    personKey = Column(Integer, primary_key=True)
    notes = Column(Text)

    # Direct foreign key associations
    _sexValue = relationship('SexAuth')
    sex = association_proxy('_sexValue', 'sexValue')
    _oLangVal = relationship('OrigLangAuth')
    origLang = association_proxy('_oLangVal', 'oLanguage')

    # Indirect foreign key associations
    factoids = association_proxy('_person_factoids', 'factoid')
    collDBEntries = association_proxy('_assoc_colldb', 'collDB')

    def main_factoids(self, ftype=None):
        """Return the list of factoids for which this person is the primary referent.
        :param ftype: the factoid type
        """
        main_set = [x.factoid for x in self._person_factoids if x.fpType == 'Primary']
        if ftype is not None:
            return [x for x in main_set if x.factoidType == ftype]
        else:
            return main_set

    def ref_factoids(self):
        """Return the list of factoids for which this person is a secondary referent."""
        return [x.factoid for x in self._person_factoids if x.fpType == 'DescRef']

    def may_also_be(self):
        """Return a set of Person objects who may or may not be the same person.
        Depends on the "Uncertain Ident" factoids for this."""
        uncertain = self.main_factoids('Uncertain Ident')
        if len(uncertain) == 0:
            return None
        alter_egos = set()
        for f in uncertain:
            alter_egos.update(f.referents())
        return alter_egos

    def __repr__(self):
        return "<%s %d>" % (self.name, self.mdbCode)


class VariantName(Base):
    __tablename__ = 'VariantName'
    tstamp = Column(DateTime)
    vnameKey = Column(Integer, primary_key=True)
    name = Column(String)
    oLangKey = Column(Integer, ForeignKey('OrigLangAuth.oLangKey'))
    creationDate = Column(DateTime)

    # Direct foreign key associations
    _oLangVal = relationship('OrigLangAuth')
    origLang = association_proxy('_oLangVal', 'oLanguage')
