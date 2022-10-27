from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, TIMESTAMP, ARRAY
from pydantic import BaseModel
from datetime import datetime
from typing import List

Base = declarative_base()


class MetricRegistry(Base):
    __tablename__ = "metricregistry"
    metricid = Column(String, primary_key=True)
    name = Column(String)
    description = Column(String)
    target_table = Column(String)
    versionid = Column(String)
    version = Column(String)
    update_time = Column(TIMESTAMP)


class MetricVersions(Base):
    __tablename__ = "metricversions"
    versionid = Column(String, primary_key=True)
    version = Column(String)
    metricid = Column(String)
    model_path = Column(String)
    count = Column(Integer)
    metrics = Column(ARRAY(String))
    dimensions = Column(ARRAY(String))
    measures = Column(ARRAY(String))
    identifiers = Column(ARRAY(String))
    depends_on = Column(ARRAY(String))
    tables_used = Column(ARRAY(String))
    update_time = Column(TIMESTAMP)


class PydanticMetricregistry(BaseModel):
    metricid: str
    name: str
    description: str = None
    target_table: str = None
    versionid: str
    version: str
    update_time: datetime


class PydanticMetricVersions(BaseModel):
    versionid: str
    version: str
    metricid: str
    model_path: str
    count: int = 0
    metrics: List[str] = []
    dimensions: List[str] = []
    measures: List[str] = []
    identifiers: List[str] = []
    depends_on: List[str] = []
    tables_used: List[str] = []
    update_time: datetime
