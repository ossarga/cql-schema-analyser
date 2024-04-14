import copy
import re


class SchemaParser:
    field_statement_to_state_mapping = {
        '(': 'START_FIELD_DEF',
        'PRIMARY': 'PRIMARY_STATEMENT',
        'KEY': 'KEY_STATEMENT',
        ')': 'END_FIELD_DEF'
    }

    properties_statement_to_state_mapping = {
        'COMPACT': 'COMPACT_STATEMENT',
        'STORAGE': 'STORAGE_STATEMENT',
        'CLUSTERING': 'CLUSTERING_STATEMENT',
        'ORDER': 'ORDER_STATEMENT',
        'BY': 'BY_STATEMENT',
        'DESC': 'DESC_STATEMENT',
        'ASC': 'ASC_STATEMENT',
        'AND': 'AND_STATEMENT'
    }

    def __init__(self):
        self.keyspace = ''
        self.table = ''
        self.statement_raw = ''
        self.statement_terms = []
        self.dom = []

        self.parse_statement_callback = {
            'CREATE': self.__parse_create_statement,
            'USE': self.__parse_use_statement
        }

        self.parse_create_statement_callback = {
            'KEYSPACE': self.__parse_create_keyspace_statement,
            'TABLE': self.__parse_create_table_statement
        }

    def __reset_parser_state(self):
        self.keyspace = ''
        self.table = ''
        self.statement_raw = ''
        self.statement_terms = []
        self.dom = []

    def __set_keyspace(self, keyspace_str):
        print('Setting keyspace to "{}".'.format(keyspace_str))
        keyspace = keyspace_str
        if keyspace[-1] == ';':
            keyspace = keyspace[0:-1]

        self.keyspace = keyspace

    def __parse_create_statement(self):
        if len(self.statement_terms) < 2:
            print('ERROR: Incomplete "CREATE" statement found. Skipping.')
            return

        create_statement = self.statement_terms[1]

        try:
            self.parse_create_statement_callback[create_statement]()
        except KeyError:
            print('ERROR: Unsupported CREATE statement "{}" found. Skipping.'.format(create_statement))

    def __parse_use_statement(self):
        if len(self.statement_terms) < 2:
            print('ERROR: Incomplete "USE" statement found. Skipping.')
            return

        self.__set_keyspace(self.statement_terms[1])

    def __parse_create_keyspace_statement(self):
        if len(self.statement_terms) < 3:
            print('ERROR: Incomplete "CREATE KEYSPACE" statement found. Skipping.')
            return

        self.__set_keyspace(self.statement_terms[2])

    def __parse_create_table_statement_properties(self, dom_object):
        # Parse the fields
        current_state = ''
        previous_state = ''
        order_by_stack = 0
        option_key_buffer = ''
        option_value_buffer = ''
        while len(self.statement_terms) > 0:
            table_options_statement = self.statement_terms.pop()
            clean_statement = re.sub(r'\(|\)|;', '', table_options_statement)

            # Work out what state we should be in
            try:
                current_state = SchemaParser.properties_statement_to_state_mapping[clean_statement]
            except KeyError:
                if previous_state == 'AND_STATEMENT':
                    current_state = 'TABLE_OPTION_DEF'
                elif previous_state == 'TABLE_OPTION_DEF':
                    current_state = 'TABLE_OPTION_DEF'
                elif previous_state == 'BY_STATEMENT':
                    current_state = 'ORDER_BY_FIELD'
                elif previous_state == 'DESC_STATEMENT' or previous_state == 'ASC_STATEMENT':
                    if order_by_stack > 0:
                        current_state = 'ORDER_BY_FIELD'
                    else:
                        print(
                            'ERROR: Malformed CQL detected. Expecting "AND" statement, but found "{}".'.format(
                                table_options_statement
                            )
                        )
                else:
                    current_state = 'TABLE_OPTION_DEF'

            # Take action based on the state we are in
            if current_state == 'COMPACT_STATEMENT':
                previous_state = 'COMPACT_STATEMENT'
            elif current_state == 'STORAGE_STATEMENT':
                previous_state = 'STORAGE_STATEMENT'
            elif current_state == 'CLUSTERING_STATEMENT':
                previous_state = 'CLUSTERING_STATEMENT'
            elif current_state == 'ORDER_STATEMENT':
                previous_state = 'ORDER_STATEMENT'
            elif current_state == 'BY_STATEMENT':
                previous_state = 'BY_STATEMENT'
            elif current_state == 'ORDER_BY_FIELD':
                order_by_stack += table_options_statement.count('(')
                previous_state = 'ORDER_BY_FIELD'
            elif current_state == 'DESC_STATEMENT':
                order_by_stack -= table_options_statement.count(')')
                previous_state = 'DESC_STATEMENT'
            elif current_state == 'ASC_STATEMENT':
                order_by_stack -= table_options_statement.count(')')
                previous_state = 'ASC_STATEMENT'
            elif current_state == 'AND_STATEMENT':
                if previous_state == 'TABLE_OPTION_DEF':
                    dom_object['attributes']['properties'][option_key_buffer] = option_value_buffer
                    option_key_buffer = ''
                    option_value_buffer = ''
                previous_state = 'AND_STATEMENT'
            elif current_state == 'TABLE_OPTION_DEF':
                key_value_pair = clean_statement.split('=')
                if len(key_value_pair) > 1:
                    option_key_buffer = key_value_pair[0]
                    option_value_buffer += key_value_pair[1]
                else:
                    option_value_buffer += ' {}'.format(key_value_pair[0])

                if table_options_statement[-1] == ';':
                    dom_object['attributes']['properties'][option_key_buffer] = option_value_buffer
                    break
                else:
                    previous_state = 'TABLE_OPTION_DEF'

    def __parse_create_table_statement_fields(self, dom_object):
        # Parse the fields
        current_state = ''
        previous_state = ''
        table_def_scope_stack = -1
        primary_key_scope_stack = 0
        previous_value = ''
        field_def = {}
        while len(self.statement_terms) > 0 and not table_def_scope_stack == 0:
            table_def_statement = self.statement_terms.pop()

            # Work out what state we should be in
            try:
                current_state = SchemaParser.field_statement_to_state_mapping[table_def_statement]
            except KeyError:
                if previous_state == 'START_FIELD_DEF':
                    current_state = 'FIELD_DEF_NAME'
                elif previous_state == 'FIELD_DEF_NAME':
                    current_state = 'FIELD_DEF_TYPE'
                elif previous_state == 'FIELD_DEF_TYPE':
                    current_state = 'FIELD_DEF_NAME'
                elif previous_state == 'KEY_STATEMENT':
                    current_state = 'PRIMARY_KEY_DEF'
                elif previous_state == 'PRIMARY_KEY_DEF':
                    if primary_key_scope_stack == 0:
                        current_state = 'FIELD_DEF_NAME'
                    else:
                        current_state = 'PRIMARY_KEY_DEF'
                else:
                    print(
                        'ERROR: Unexpected previous state "{}" found due to statement "{}".'.format(
                            previous_state, table_def_statement
                        )
                    )

            # Take action based on the state we are in
            if current_state == 'START_FIELD_DEF':
                if table_def_scope_stack == -1:
                    table_def_scope_stack = 0
                table_def_scope_stack += table_def_statement.count('(')
                previous_state = 'START_FIELD_DEF'
            elif current_state == 'FIELD_DEF_NAME':
                previous_state = 'FIELD_DEF_NAME'
            elif current_state == 'FIELD_DEF_TYPE':
                table_type = table_def_statement.rstrip(',')
                field_def[previous_value] = table_type
                dom_object['attributes']['fields'].append(table_type)
                previous_state = 'FIELD_DEF_TYPE'
            elif current_state == 'PRIMARY_STATEMENT':
                previous_state = 'PRIMARY_STATEMENT'
            elif current_state == 'KEY_STATEMENT':
                previous_state = 'KEY_STATEMENT'
            elif current_state == 'PRIMARY_KEY_DEF':
                primary_key_scope_stack += table_def_statement.count('(')
                primary_key_scope_stack -= table_def_statement.count(')')
                field_name = table_def_statement.strip('(').strip(',').strip(')')
                field_type = field_def[field_name]
                if primary_key_scope_stack >= 1:
                    dom_object['attributes']['key']['partition'].append(field_type)
                else:
                    dom_object['attributes']['key']['clustering'].append(field_type)

                previous_state = 'PRIMARY_KEY_DEF'
            elif current_state == 'END_FIELD_DEF':
                table_def_scope_stack -= table_def_statement.count(')')

            previous_value = table_def_statement

    # ['CREATE', 'TABLE', '"RAM"', '(', '"RAM_ID"', 'text,', '"RAM_DOC"', 'text,', 'PRIMARY', 'KEY', '(("RAM_ID"))', ')', 'WITH', 'bloom_filter_fp_chance=0.010000', 'AND', "caching='KEYS_ONLY'", 'AND', "comment=''", 'AND', 'dclocal_read_repair_chance=0.100000', 'AND', 'gc_grace_seconds=864000', 'AND', 'index_interval=128', 'AND', 'read_repair_chance=0.000000', 'AND', "replicate_on_write='true'", 'AND', "populate_io_cache_on_flush='false'", 'AND', 'default_time_to_live=0', 'AND', "speculative_retry='99.0PERCENTILE'", 'AND', 'memtable_flush_period_in_ms=0', 'AND', "compaction={'class':", "'SizeTieredCompactionStrategy'}", 'AND', "compression={'sstable_compression':", "'LZ4Compressor'};"]
    # Parse table based on the assumption that schema was dumped using the "DESCRIBE SCHEMA;" command
    def __parse_create_table_statement(self):
        self.statement_terms.reverse()

        # Remove "CREATE" and "TABLE" statements
        assert self.statement_terms.pop() == 'CREATE'
        assert self.statement_terms.pop() == 'TABLE'

        self.table = self.statement_terms.pop()

        if 'system' in self.keyspace or '"OpsCenter"' in self.keyspace:
            print('Table "{}" belongs to keyspace "{}". Skipping.'.format(
                self.table, self.keyspace.strip('"'))
            )
            return

        dom_object = {
            'name': '{}.{}'.format(self.keyspace, self.table),
            'attributes': {
                'fields': [],
                'key': {
                    'partition': [],
                    'clustering': []
                },
                'properties': {},
            },
            'statement': self.statement_raw
        }

        self.__parse_create_table_statement_fields(dom_object)

        assert self.statement_terms.pop() == 'WITH'

        self.__parse_create_table_statement_properties(dom_object)
        self.dom.append(dom_object)

    def parse_schema(self, cql_file_path):
        with open(cql_file_path, 'r') as in_cql:
            for raw_line in in_cql:
                statement_part = raw_line.strip()

                if len(statement_part) == 0:
                    continue

                self.statement_raw += raw_line
                self.statement_terms += statement_part.split(' ')

                if len(self.statement_terms) > 0 and self.statement_terms[-1][-1] == ';':
                    cql_operation = self.statement_terms[0].upper()

                    try:
                        self.parse_statement_callback[cql_operation]()
                    except KeyError:
                        print('ERROR: Unsupported CQL operation "{}" found. Skipping.'.format(cql_operation))

                    self.statement_raw = ''
                    self.statement_terms = []

        rtn_dom = copy.deepcopy(self.dom)
        self.__reset_parser_state()

        return rtn_dom
