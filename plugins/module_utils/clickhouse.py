# This code is part of Ansible, but is an independent component.
# This particular file snippet, and this file snippet only, is BSD licensed.
# Modules you write using this snippet, which is embedded dynamically by Ansible
# still belong to the author of the module, and may assign their own license
# to the complete work.
#
# Simplified BSD License (see simplified_bsd.txt or https://opensource.org/licenses/BSD-2-Clause)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

from ansible.module_utils.basic import missing_required_lib
from ansible.module_utils._text import to_native

Client = None
try:
    from clickhouse_driver import Client
    from clickhouse_driver import __version__ as driver_version
    HAS_DB_DRIVER = True
except ImportError:
    HAS_DB_DRIVER = False

PRIV_ERR_CODE = 497


def client_common_argument_spec():
    """
    Return a dictionary with connection options.

    The options are commonly used by many modules.
    """
    return dict(
        login_host=dict(type='str', default='localhost'),
        login_port=dict(type='int', default=None),
        login_db=dict(type='str', default=None),
        login_user=dict(type='str', default=None),
        login_password=dict(type='str', default=None, no_log=True),
        client_kwargs=dict(type='dict', default={}),
    )


def get_main_conn_kwargs(module):
    """Retrieves main connection arguments values and translates
    them into corresponding clickhouse_driver.Client() arguments.

    Returns a dictionary of arguments with values to pass to Client().
    """
    main_conn_kwargs = {}
    main_conn_kwargs['host'] = module.params['login_host']  # Has a default value
    if module.params['login_port']:
        main_conn_kwargs['port'] = module.params['login_port']
    if module.params['login_db']:
        main_conn_kwargs['database'] = module.params['login_db']
    if module.params['login_user']:
        main_conn_kwargs['user'] = module.params['login_user']
    if module.params['login_password']:
        main_conn_kwargs['password'] = module.params['login_password']
    return main_conn_kwargs


def check_clickhouse_driver(module):
    """Checks if the driver is present.

    Informs user if no driver and fails.
    """
    if not HAS_DB_DRIVER:
        module.fail_json(msg=missing_required_lib('clickhouse_driver'))


def version_clickhouse_driver():
    """
    Returns the current version of clickhouse_driver.
    """
    return driver_version


def connect_to_db_via_client(module, main_conn_kwargs, client_kwargs):
    """Connects to DB using the Client() class.

    Returns Client() object.
    """
    try:
        # Merge the kwargs as Python 2 would through an error
        # when unpaking them separately to Client()
        client_kwargs.update(main_conn_kwargs)
        client = Client(**client_kwargs)
    except Exception as e:
        module.fail_json(msg="Failed to connect to database: %s" % to_native(e))

    return client


def execute_query(module, client, query, execute_kwargs=None, set_settings=None):
    """Execute query.

    Returns rows returned in response.

    set_settings - The list of settings that need to be set before executing the request.
    """
    # Some modules do not pass this argument
    if execute_kwargs is None:
        execute_kwargs = {}

    if set_settings is None:
        set_settings = {}

    try:
        if len(set_settings) != 0:
            for setting in set_settings:
                client.execute("SET %s = '%s'" % (setting, set_settings[setting]))
        result = client.execute(query, **execute_kwargs)
    except Exception as e:
        if "Not enough privileges" in to_native(e):
            return PRIV_ERR_CODE
        module.fail_json(msg="Failed to execute query: %s" % to_native(e))

    return result


def get_server_version(module, client):
    """Get server version.

    Returns a dictionary with server version.
    """
    result = execute_query(module, client, "SELECT version()")

    if result == PRIV_ERR_CODE:
        return {PRIV_ERR_CODE: "Not enough privileges"}

    raw = result[0][0]
    split_raw = raw.split('.')

    version = {}
    version["raw"] = raw

    version["year"] = int(split_raw[0])
    version["feature"] = int(split_raw[1])
    version["maintenance"] = int(split_raw[2])

    if '-' in split_raw[3]:
        tmp = split_raw[3].split('-')
        version["build"] = int(tmp[0])
        version["type"] = tmp[1]
    else:
        version["build"] = int(split_raw[3])
        version["type"] = None

    return version


class ClickHouseCreateAlterDropType:
    _type = None

    def __init__(self, module, client, name):
        self.module = module
        self.client = client
        self.name = name
        self.executed_statements = []
        self.exists = self._check_exists()

    def _check_exists(self):
        query = f"SELECT 1 FROM system.{self._type.lower()}s WHERE name = '{self.name}' LIMIT 1"
        result = execute_query(self.module, self.client, query)
        return bool(result)

    def _get_create_statement(self):
        """Get current definition using SHOW CREATE X"""
        if not self.exists:
            return None

        query = f"SHOW CREATE {self._type} {self.name}"
        result = execute_query(self.module, self.client, query)
        if result:
            return result[0][0]  # SHOW CREATE X returns single row with CREATE statement
        return None

    def _params_match(self, create_statement):
        """Parse CREATE X statement"""
        raise NotImplementedError

    def _needs_altering(self):
        """Check if we need to alter to reach desired"""
        create_statement = self._get_create_statement()
        return create_statement is None or not self._params_match(create_statement)

    def _create_sql_clauses(self, action):
        clauses = [f"{action} {self._type} {self.name}"]

        cluster = self.module.params['cluster']
        if cluster:
            clauses.append(f"ON CLUSTER {cluster}")

        return clauses

    def _do(self, action):
        if action not in ("CREATE", "ALTER"):
            raise ValueError(f"Expected action to be create or alter but got '{action}'")

        query = " ".join(self._create_sql_clauses(action))

        self.executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

    def create(self):
        """
        Create entity using CREATE X
        Returns whether the entity was created or not
        """
        if self.exists:
            return False

        self._do("CREATE")
        self.exists = True
        return True

    def alter(self):
        """
        Update entity using ALTER X if it needs it
        Returns whether the entity was altered or not
        """
        if not self.exists or not self._needs_altering():
            return False

        self._do("ALTER")
        return True

    def drop(self):
        """Drop entity using DROP X"""
        if not self.exists:
            return False

        query = f"DROP {self._type} {self.name}"
        self.executed_statements.append(query)

        if not self.module.check_mode:
            execute_query(self.module, self.client, query)

        self.exists = False
        return True

class ClickHousePresentAbsentType(ClickHouseCreateAlterDropType):
    def ensure_state(self):
        state = self.module.params['state']
        if state not in ("present", "absent"):
            raise ValueError(f"Unexpeced state '{state}'")
        
        if state == "present":
            # create or alter role
            # will do nothing is nothing needs to be done
            changed = self.create() or self.alter()
        else:
            # drop if exists
            changed = self.drop()

        return changed
