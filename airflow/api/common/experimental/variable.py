from airflow.exceptions import AirflowBadRequest, VariableNotFound
from airflow.models import Variable
from airflow.utils.db import provide_session


@provide_session
def get_variable(key, session=None):
    """Get pool by a given name."""
    if not (key and key.strip()):
        raise AirflowBadRequest("Pool name shouldn't be empty")
    variable = session.query(Variable).filter(Variable.key == key).first()
    if variable is None:
        raise VariableNotFound("Variable '%s' doesn't exist" % key)
    return variable


@provide_session
def get_variables(session=None):
    """Get all variables."""
    return session.query(Variable).all()


@provide_session
def create_variable(key, value, session=None):
    """Create a pool with a given parameters."""
    if not (key and key.strip()):
        raise AirflowBadRequest("Variable key shouldn't be empty")
    session.expire_on_commit = False
    variable = session.query(Variable).filter(Variable.key == key).first()
    if variable is None:
        variable = Variable(key=key, val=value)
        session.add(variable)
    else:
        variable.set_val(value)
    session.commit()
    return variable


@provide_session
def delete_variable(key, session=None):
    """Delete pool by a given name."""
    if not (key and key.strip()):
        raise AirflowBadRequest("Pool name shouldn't be empty")
    variable = session.query(Variable).filter(Variable.key == key).first()
    if variable is None:
        raise VariableNotFound("Pool '%s' doesn't exist" % key)
    session.delete(variable)
    session.commit()
    return variable
