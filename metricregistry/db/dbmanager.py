from metricregistry.db.utils import *
from metricregistry.utils.exception import MetricRegistryException
from metricregistry.db.models import *
from typing import Dict
from hashlib import sha256
import uuid
from datetime import datetime
from sqlalchemy.sql import exists
from sqlalchemy.orm import close_all_sessions


class DataBase:
    def __init__(self, db_uri, model_path):
        self.db_uri = db_uri
        self.model_path = model_path
        self.engine = create_sqlalchemy_engine(db_uri)
        Base.metadata.create_all(self.engine, checkfirst=True)
        Base.metadata.bind = self.engine
        self.Session = sqlalchemy.orm.sessionmaker(bind=self.engine)
        self.ManagedSessionMaker = get_managed_session_maker(self.Session)
        if not verify_table_exists(self.engine):
            MetricRegistry.__table__.create(bind=self.engine, checkfirst=True)
            MetricVersions.__table__.create(bind=self.engine, checkfirst=True)

    def close(self):
        close_all_sessions()
        self.engine.dispose()

    def _save_to_db(self, session, objs):
        if type(objs) is list:
            session.add_all(objs)
        else:
            session.add(objs)

    def getMetricIds(self, name: str) -> Dict:
        return {
            "metricid": sha256(str(name).lower().encode("utf-8")).hexdigest(),
            "versionid": str(uuid.uuid4()),
        }

    def checkIfMetricExists(self, session, metricid: str) -> bool:
        try:
            return session.query(
                exists().where(MetricRegistry.metricid == metricid)
            ).scalar()
        except Exception as e:
            raise MetricRegistryException(
                "failed while checking if metric exists : {}".format(e)
            )

    def updateMetric(self, session, metricid: str, versionid: str, version: str):
        try:
            (
                session.query(MetricRegistry)
                .filter(MetricRegistry.metricid == metricid)
                .update(
                    {
                        "versionid": versionid,
                        "version": version,
                        "update_time": datetime.now(),
                    }
                )
            )
            session.commit()
        except Exception as e:
            raise MetricRegistryException(
                "failed while updating metric with latest version :  {}".format(e)
            )

    def getPreviousVersion(self, session, metricid: str) -> str:
        try:
            return (
                session.query(MetricRegistry.version)
                .filter(MetricRegistry.metricid == metricid)
                .scalar()
            )
        except Exception as e:
            raise MetricRegistryException(
                "failed while getting previous version :  {}".format(e)
            )

    def createVersion(self, previousVersion: str = None) -> str:
        if previousVersion:
            major, minor = tuple(map(lambda x: int(x), previousVersion.split(".")))
            if minor < 9:
                minor += 1
                return "{0}.{1}".format(major, minor)
            elif minor == 9:
                major += 1
                return "{0}.{1}".format(major, 0)
            else:
                pass
        else:
            return "1.0"

    def add_model(self, data: Dict):
        with self.ManagedSessionMaker() as session:
            try:
                ids = self.getMetricIds(data.get("name"))
                existFlag = self.checkIfMetricExists(session, ids.get("metricid"))
                versionsDict = dict(
                    (d, data[d])
                    for d in [
                        "count",
                        "metrics",
                        "dimensions",
                        "measures",
                        "identifiers",
                        "depends_on",
                        "tables_used",
                    ]
                    if d in data
                )
                metricDict = dict(
                    (d, data[d])
                    for d in [
                        "name",
                        "description",
                        "target_table",
                    ]
                    if d in data
                )
                if existFlag:
                    print(
                        "Model Metric {} exists, creating a new version".format(
                            data.get("name")
                        )
                    )
                    previousVersion = self.getPreviousVersion(
                        session, ids.get("metricid")
                    )
                    newVersion = self.createVersion(previousVersion)
                    modelPath = "{0}/{1}/version={2}/{3}".format(
                        self.model_path,
                        data.get("name"),
                        newVersion,
                        data.get("filename"),
                    )
                    version = MetricVersions(
                        **PydanticMetricVersions(
                            **{
                                **versionsDict,
                                **ids,
                                **{
                                    "version": newVersion,
                                    "model_path": modelPath,
                                    "update_time": datetime.now(),
                                },
                            }
                        ).dict()
                    )
                    self._save_to_db(session, version)
                    self.updateMetric(
                        session, ids.get("metricid"), ids.get("versionid"), newVersion
                    )
                    session.commit()
                    session.flush()
                    return {
                        "status": True,
                        "data": {"model_path": modelPath, **ids},
                        "message": "Metric Model {0} updated to version {1}".format(
                            data.get("name"), newVersion
                        ),
                    }
                else:
                    newVersion = "1.0"
                    print(
                        "Metric registry for model {} is being regsitered".format(
                            data.get("name")
                        )
                    )
                    modelPath = "{0}/{1}/version={2}/{3}".format(
                        self.model_path,
                        data.get("name"),
                        newVersion,
                        data.get("filename"),
                    )
                    metric = MetricRegistry(
                        **PydanticMetricregistry(
                            **metricDict,
                            **ids,
                            **{
                                "version": "1.0",
                                "update_time": datetime.now(),
                            },
                        ).dict()
                    )
                    version = MetricVersions(
                        **PydanticMetricVersions(
                            **{
                                **versionsDict,
                                **ids,
                                **{
                                    "version": newVersion,
                                    "model_path": modelPath,
                                    "update_time": datetime.now(),
                                },
                            }
                        ).dict()
                    )
                    self._save_to_db(session, metric)
                    self._save_to_db(session, version)
                    session.commit()
                    session.flush()
                    return {
                        "status": True,
                        "data": {"model_path": modelPath, **ids},
                        "message": "Created a new registry for model {0}".format(
                            data.get("name")
                        ),
                    }
            except Exception as e:
                raise MetricRegistryException(
                    "Error while adding the model :  {}".format(e)
                )
