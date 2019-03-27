from airflow.exceptions import AirflowBadRequest, ConnectionNotFound
from airflow.models import Connection
from airflow.utils.db import provide_session


@provide_session
def get_connection(id, session=None):
    """Get connection by a given name."""
    if not (id and id.strip()):
        raise AirflowBadRequest("Connection name shouldn't be empty")
    connection = session.query(Connection).filter(Connection.conn_id == id).first()
    if connection is None:
        raise ConnectionNotFound("Connection '%s' doesn't exist" % id)
    return connection


@provide_session
def get_connection(session=None):
    """Get all connection."""
    return session.query(Connection).all()


@provide_session
def create_connection(id, type, host, schema, login, password, port, extra, session=None):
    """Create a connection with a given parameters."""
    if not (id and id.strip()):
        raise AirflowBadRequest("Connection id shouldn't be empty")
    session.expire_on_commit = False
    connection = session.query(Connection).filter(Connection.conn_id == id).first()
    if connection is None:
        connection = Connection(conn_id=id, conn_type=type, host=host, schema=schema, login=login, password=password, port=port, extra=extra)
        session.add(connection)
    else:
        connection.conn_type = type
        connection.host = host
        connection.schema = schema
        connection.login = login
        connection.password = password
        connection.port = port
        connection.extra = extra
    session.commit()
    return connection


@provide_session
def delete_connection(id, session=None):
    """Delete connection by a given name."""
    if not (id and id.strip()):
        raise AirflowBadRequest("Connection name shouldn't be empty")
    connection = session.query(Connection).filter(Connection.conn_id == id).first()
    if connection is None:
        raise ConnectionNotFound("Connection '%s' doesn't exist" % id)
    session.delete(connection)
    session.commit()
    return connection
