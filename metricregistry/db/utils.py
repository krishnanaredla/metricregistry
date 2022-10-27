import sqlalchemy
from metricregistry.db.models import MetricRegistry, MetricVersions
from metricregistry.utils.exception import MetricRegistryException
from contextlib import contextmanager


def create_sqlalchemy_engine(db_uri):
    try:
        engine = sqlalchemy.create_engine(db_uri)
        sqlalchemy.inspect(engine)
        return engine
    except Exception as e:
        raise MetricRegistryException(
            "SQLAlchemy engine could not be created. The following exception is caught.\n {0} ".format(
                e
            )
        )


def verify_table_exists(engine) -> bool:
    inspected_tables = set(sqlalchemy.inspect(engine).get_table_names())
    expected_tables = [MetricRegistry.__tablename__, MetricVersions.__tablename__]
    if [table not in inspected_tables for table in expected_tables]:
        return False
    else:
        return True


def get_managed_session_maker(SessionMaker):
    @contextmanager
    def make_managed_session():
        session = SessionMaker()
        try:
            yield session
            session.commit()
        except MetricRegistryException:
            session.rollback()
            raise
        except Exception as e:
            session.rollback()
            raise MetricRegistryException(e)
        finally:
            session.close()

    return make_managed_session
