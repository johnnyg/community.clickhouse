#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright: (c) 2025, John Garland (@johnnyg) <johnnybg@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

DOCUMENTATION = r"""
---
module: clickhouse_quota

short_description: Creates or removes a ClickHouse quota.

description:
  - Creates or removes a ClickHouse quota.

attributes:
  check_mode:
    description: Supports check_mode.
    support: full

version_added: '1.0.0'

author:
  - John Garland (@johnnyg)

extends_documentation_fragment:
  - community.clickhouse.client_inst_opts

options:
  state:
    description:
      - Quota state.
      - C(present) creates the quota if it does not exist.
      - C(absent) deletes the quota if it exists.
    type: str
    choices: ['present', 'absent']
    default: 'present'
  name:
    description:
      - Quota name to add or remove.
    type: str
    required: true
  cluster:
    description:
      - Run the command on all cluster hosts.
      - If the cluster is not configured, the command will crash with an error.
    type: str
  keyed_by:
    description:
      - Keys the quota by the specified key (default is to not key)
    type: str
    choices:
      - user_name
      - ip_address
      - client_key
      - client_key,user_name
      - client_key,ip_address
  limits:
    description:
      - The limits that this quota should enforce.
    type: list
    elements: dict
    suboptions:
      randomized_start:
        description:
          - Whether this interval's start should be randomized.
          - Intervals always start at the same time if not randomized.
        type: bool
        default: false
      interval:
        description:
          - The interval to apply the following quotas on
          - This is in the format <number> <unit>
          - Where unit is one of second, minute, hour, day, week, month, quarter or year
        type: str
      max:
        description:
          - Maximum values to apply to this interval in this quota
        type: dict
        suboptions:
          queries:
            description:
              - Maximum number of queries to enforce in this interval
            type: int
          query_selects:
            description:
              - Maximum number of query selects to enforce in this interval
            type: int
          query_inserts:
            description:
              - Maximum number of query inserts to enforce in this interval
            type: int
          errors:
            description:
              - Maximum number of errors to enforce in this interval
            type: int
          result_rows:
            description:
              - Maximum number of result rows to enforce in this interval
            type: int
          result_bytes:
            description:
              - Maximum number of result bytes to enforce in this interval
            type: int
          read_rows:
            description:
              - Maximum number of rows read to enforce in this interval
            type: int
          read_bytes:
            description:
              - Maximum number of bytes read to enforce in this interval
            type: int
          written_bytes:
            description:
              - Maximum number of bytes written to enforce in this interval
            type: int
          execution_time:
            description:
              - Maximum number of execution time to enforce in this interval
            type: float
          failed_sequential_authentications:
            description:
              - Maximum number of failed sequential authentications to enforce in this interval
            type: int
        required_one_of:
          - ("queries", "query_selects", "query_inserts", "errors", "result_rows", "result_bytes", "read_rows", "read_bytes", "written_bytes", "execution_time", "failed_sequential_authentications")
      no_limits:
        description:
          - Don't apply any limits
        type: bool
        choices: [true]
      tracking_only:
        description:
          - Just track usage instead of enforcing
        type: bool
        choices: [true]
      mutually_exclusive:
        - ("max", "no_limits", "tracking_only")
      required_one_of:
        - ("max", "no_limits", "tracking_only")
  apply_to:
    description:
      - Apply this quota to the following list of users/roles dependent on I(apply_to_mode)
      - Can include special keywords of default and current_user or the name of an actual user or role
      - Is an error to specify this if I(apply_to_mode="all")
    type: list
    elements: str
  apply_to_mode:
    description:
      - When C(listed_only) (default), the quota will only apply to the users/roles specified in I(apply_to)
      - When C(all), the quota will only apply to _all_ users/roles
      - When C(all_except_listed), the quota will only apply to _all_ the users/roles except those specified in I(apply_to)
    type: str
    choices: ['listed_only', 'all', 'all_except_listed']
    default: 'listed_only'
"""

EXAMPLES = r"""
- name: Create quota
  community.clickhouse.clickhouse_quota:
    name: test_quota
    state: present

- name: Create a quota with settings
  community.clickhouse.clickhouse_quota:
    name: test_quota
    state: present
    settings:
      - max_memory_usage = 15000 MIN 15000 MAX 16000 READONLY
      - PROFILE restricted
    cluster: test_cluster

- name: Remove quota
  community.clickhouse.clickhouse_quota:
    name: test_quota
    state: absent
"""

RETURN = r"""
executed_statements:
  description:
  - Data-modifying executed statements.
  returned: on success
  type: list
  sample: ['CREATE QUOTA test_quota']
"""

from ansible.module_utils.basic import AnsibleModule

from ansible_collections.community.clickhouse.plugins.module_utils.clickhouse import (
    check_clickhouse_driver,
    client_common_argument_spec,
    connect_to_db_via_client,
    get_main_conn_kwargs,
    ClickHousePresentAbsentType,
)


class ClickHouseQuota(ClickHousePresentAbsentType):
    _type = "QUOTA"

    def _params_match(self, create_statement):
        return False

    def _create_sql_clauses(self, action):
        sql_clauses = super()._create_sql_clauses(action)

        keyed_by = self.module.params.get("keyed_by")
        if keyed_by:
           sql_clauses.append(f"KEYED BY {keyed_by}")

        limits_sql_clauses = []
        for limit in self.module.params["limits"] or []:
            sql_clause = ["FOR"]
            if limit.get("randomized_start", False):
                sql_clause.append("RANDOMIZED")
            sql_clause.append(f"INTERVAL {limit["interval"]}")
            max_limits = limit.get("max")
            if max_limits:
              sql_clause.append("MAX")
              sql_clause.append(", ".join(f"{key} = {value}" for key, value in max_limits.items()))
            elif limit.get("no_limits") is not None:
              sql_clause.append("NO LIMITS")
            elif limit.get("tracking_only") is not None:
              sql_clause.append("TRACKING ONLY")
            else:
               raise ValueError("One of max or no_limits or tracking_only needs to specified")
            limits_sql_clauses.append(" ".join(sql_clause))
        if limits_sql_clauses:
          sql_clauses.append(", ".join(limits_sql_clauses))

        apply_to = self.module.params.get("apply_to", [])
        apply_to_mode = self.module.params["apply_to_mode"]
        if apply_to_mode == "all_except_listed" and not apply_to:
           apply_to_mode = "all"
        if apply_to and apply_to_mode == "all":
           raise ValueError("Cannot specify list of user/roles to apply to when apply_to_mode == all")
        if apply_to_mode == "all":
          sql_clauses.append("TO ALL")
        elif apply_to:
          sql_clauses.append("TO")
          if apply_to_mode == "all_except_listed":
             sql_clauses.append("ALL EXCEPT")
          sql_clauses.append(", ".join(apply_to))

        return sql_clauses


def main():
    # Set up arguments.
    # If there are common arguments shared across several modules,
    # create the common_argument_spec() function under plugins/module_utils/*
    # and invoke here to return a dict with those arguments
    argument_spec = client_common_argument_spec()
    argument_spec.update(
        state=dict(type="str", choices=["present", "absent"], default="present"),
        name=dict(type="str", required=True),
        cluster=dict(type='str', default=None),
        keyed_by=dict(type='str', choices=[
          "user_name",
          "ip_address",
          "client_key",
          "client_key,user_name",
          "client_key,ip_address",
        ]),
        limits=dict(type='list', elements='dict'),
        apply_to=dict(type='list', elements='str'),
        apply_to_mode=dict(type='str', choices=["listed_only", "all", "all_except_listed"], default="listed_only"),
    )

    # Instantiate an object of module class
    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
    )

    # Assign passed options to variables
    client_kwargs = module.params["client_kwargs"]
    # The reason why these arguments are separate from client_kwargs
    # is that we need to protect some sensitive data like passwords passed
    # to the module from logging (see the arguments above with no_log=True);
    # Such data must be passed as module arguments (not nested deep in values).
    main_conn_kwargs = get_main_conn_kwargs(module)
    name = module.params["name"]

    # Will fail if no driver informing the user
    check_clickhouse_driver(module)

    # Connect to DB
    client = connect_to_db_via_client(module, main_conn_kwargs, client_kwargs)

    # Do the job
    quota = ClickHouseQuota(module, client, name)
    changed = quota.ensure_state()

    # Close connection
    client.disconnect_connection()

    # Users will get this in JSON output after execution
    module.exit_json(changed=changed, executed_statements=quota.executed_statements)


if __name__ == "__main__":
    main()
