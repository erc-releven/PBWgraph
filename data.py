from sqlalchemy import Column, ForeignKey, Table                   ## DB components
from sqlalchemy import DateTime, Integer, SmallInteger, String, Text   ## Column types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref

Base = declarative_base()


class AttrDateType(Base):
    __tablename__ = 'AttrDateType'
    attrDTKey = Column(Integer, primary_key=True)
    aDTName = Column(String)


class Audit(Base):
    __tablename__ = 'Audit'
    CollDBKey = Column(Integer)
    personCount = Column(SmallInteger)
    factoidTypeKey = Column(SmallInteger)
    DCDCount = Column(SmallInteger)
    MDBcount = Column(SmallInteger)
    problem = Column(Integer)
    auditKey = Column(SmallInteger, primary_key=True)
    subCount = Column(SmallInteger)


