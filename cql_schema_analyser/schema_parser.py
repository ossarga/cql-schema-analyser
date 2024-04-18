import copy
import re


class SchemaParser:
    column_grammar_to_state_mapping = {
        'ascii': 'COLUMN_TYPE_DEF',
        'bigint': 'COLUMN_TYPE_DEF',
        'blob': 'COLUMN_TYPE_DEF',
        'boolean': 'COLUMN_TYPE_DEF',
        'counter': 'COLUMN_TYPE_DEF',
        'date': 'COLUMN_TYPE_DEF',
        'decimal': 'COLUMN_TYPE_DEF',
        'double': 'COLUMN_TYPE_DEF',
        'duration': 'COLUMN_TYPE_DEF',
        'float': 'COLUMN_TYPE_DEF',
        'inet': 'COLUMN_TYPE_DEF',
        'int': 'COLUMN_TYPE_DEF',
        'smallint': 'COLUMN_TYPE_DEF',
        'text': 'COLUMN_TYPE_DEF',
        'time': 'COLUMN_TYPE_DEF',
        'timestamp': 'COLUMN_TYPE_DEF',
        'timeuuid': 'COLUMN_TYPE_DEF',
        'tinyint': 'COLUMN_TYPE_DEF',
        'uuid': 'COLUMN_TYPE_DEF',
        'varchar': 'COLUMN_TYPE_DEF',
        'varint': 'COLUMN_TYPE_DEF',
        'list': 'COLUMN_TYPE_DEF',
        'frozen': 'COLUMN_TYPE_DEF',
        'map': 'COLUMN_TYPE_DEF',
        'set': 'COLUMN_TYPE_DEF',
        'tuple': 'COLUMN_TYPE_DEF',
        'PRIMARY': 'PRIMARY_CLAUSE',
        'KEY': 'KEY_CLAUSE',
        '(': 'START_SCOPE_TABLE_DEF',
        ')': 'END_SCOPE_TABLE_DEF',
        '<': 'START_SCOPE_COLUMN_TYPE_DEF',
        '>': 'END_SCOPE_COLUMN_TYPE_DEF',
        ',': 'TERM_SEPARATOR',
        ';': 'END_STATEMENT'
    }

    def __init__(self):
        self.current_keyspace = ''
        self.current_table_parts = ()
        self.keyspaces = set()
        self.statement_raw = ''
        self.statement_terms = []
        self.dom = []

        self.parse_statement_callback = {
            'create': self.__parse_create_statement,
            'use': self.__parse_use_statement,
        }

        self.parse_create_statement_callback = {
            'keyspace': self.__parse_create_keyspace_statement,
            'table': self.__parse_create_table_statement,
        }

        self.scope = ''
        self.scope_stack = 0
        self.column_def_stack = 0
        self.column_name = ''
        self.column_type = ''
        self.column_type_lookup = {}

    def __reset_parser_state(self):
        self.current_keyspace = ''
        self.keyspaces = set()
        self.table = ''
        self.statement_raw = ''
        self.statement_terms = []
        self.dom = []

    def __set_keyspace(self):
        term_item = self.statement_terms.pop()
        if term_item[0].isalnum() and term_item.lower() != 'with':
            print('Setting keyspace to "{}".'.format(term_item))
            self.keyspaces.add(term_item)
            self.current_keyspace = term_item
        else:
            raise ValueError('ERROR: Malformed keyspace definition. Skipping.')

    def __set_table(self):
        term_item = self.statement_terms.pop()

        # Check if the table name is in the format <keyspace>.<table> and <keyspace> matches the current keyspace
        if term_item in self.keyspaces:
            separator = self.statement_terms.pop()
            if separator == '.':
                self.current_table_parts = (term_item, self.statement_terms.pop())
            else:
                raise ValueError('ERROR: Malformed "CREATE TABLE" statement found. Expecting table name to be in '
                      'format <keyspace>.<table>. Dot separator is missing. Skipping.')
        # Assume we just have a table name in the current keyspace
        else:
            if self.statement_terms[-1] == 'START_SCOPE_TABLE_DEF':
                self.current_table_parts = (self.current_keyspace, term_item)
            elif self.statement_terms[-1] == '.':
                self.statement_terms.pop()
                raise ValueError('ERROR: Unexpected keyspace "{}" found for table "{}". Skipping'.format(
                    term_item,
                    self.statement_terms.pop())
                )

    def __parse_create_statement(self):
        if len(self.statement_terms) < 2:
            print('ERROR: Incomplete "CREATE" statement found. Skipping.')
            return

        create_statement = self.statement_terms.pop().lower()

        try:
            self.parse_create_statement_callback[create_statement]()
        except KeyError:
            print('ERROR: Unsupported CREATE statement "{}" found. Skipping.'.format(create_statement))

    def __parse_use_statement(self):
        if len(self.statement_terms) < 2:
            print('ERROR: Incomplete "USE" statement found. Skipping.')
            return

        try:
            self.__set_keyspace()
        except ValueError as e:
            print(e)

    def __parse_create_keyspace_statement(self):
        if len(self.statement_terms) < 3:
            print('ERROR: Incomplete "CREATE KEYSPACE" statement found. Skipping.')
            return

        try:
            self.__set_keyspace()
        except ValueError as e:
            print(e)

    def __parse_create_table_statement_column_def(self, current_state, previous_state, term_item, dom_object):
        if current_state == 'START_SCOPE_TABLE_DEF':
            self.scope_stack += 1
            if previous_state == 'KEY_CLAUSE':
                if len(dom_object['attributes']['key']['partition']):
                    raise ValueError('ERROR: Multiple primary keys found. Skipping.')
                else:
                    self.scope = 'PARTITION_KEY_DEF'
        elif current_state == 'COLUMN_NAME_DEF':
            self.column_name = term_item
        elif current_state == 'COLUMN_TYPE_DEF':
            if previous_state == 'COLUMN_NAME_DEF':
                self.column_type = term_item
            elif previous_state in ['TERM_SEPARATOR', 'START_SCOPE_COLUMN_TYPE_DEF']:
                self.column_type += term_item
        elif current_state == 'START_SCOPE_COLUMN_TYPE_DEF':
            self.column_def_stack += 1
            self.column_type += term_item
        elif current_state == 'END_SCOPE_COLUMN_TYPE_DEF':
            self.column_def_stack -= 1
            self.column_type += term_item
        elif current_state == 'TERM_SEPARATOR':
            if previous_state == 'COLUMN_TYPE_DEF' and self.column_def_stack > 0:
                self.column_type += term_item
            else:
                self.column_type_lookup[self.column_name] = self.column_type
                dom_object['attributes']['columns'].append(self.column_type)
                if previous_state == 'KEY_CLAUSE':
                    if len(dom_object['attributes']['key']['partition']):
                        raise ValueError('ERROR: Multiple primary keys found. Skipping.')
                    else:
                        dom_object['attributes']['key']['partition'].append(self.column_type)
        elif current_state == 'PRIMARY_CLAUSE':
            pass
        elif current_state == 'KEY_CLAUSE':
            if previous_state != 'PRIMARY_CLAUSE':
                raise ValueError('ERROR: Unexpected "KEY" clause found. Skipping.')
        elif current_state == 'END_SCOPE_TABLE_DEF':
            self.scope_stack -= 1
            dom_object['attributes']['columns'].append(self.column_type)
            if self.scope_stack == 0:
                if not len(dom_object['attributes']['key']['partition']):
                    raise ValueError('ERROR: No primary key found. Skipping.')


    def __parse_create_table_statement_partition_def(self, current_state, previous_state, term_item, dom_object):
        if current_state == 'COLUMN_NAME_DEF':
            self.column_name = term_item
        elif current_state == 'START_SCOPE_TABLE_DEF':
            self.scope_stack += 1
            if self.scope_stack > 3:
                raise ValueError('ERROR: Unexpected "(" found. Skipping.')
        elif current_state == 'TERM_SEPARATOR':
            dom_object['attributes']['key']['partition'].append(self.column_type_lookup[self.column_name])
            if self.scope_stack == 2:
                self.scope = 'CLUSTERING_KEY_DEF'
        elif current_state == 'END_SCOPE_TABLE_DEF':
            dom_object['attributes']['key']['partition'].append(self.column_type_lookup[self.column_name])
            self.scope_stack -= 1
            if self.scope_stack == 1:
                self.scope = 'TABLE_COLUMN_DEF'
            elif self.scope_stack == 2:
                self.scope = 'CLUSTERING_KEY_DEF'

    def __parse_create_table_statement_clustering_def(self, current_state, previous_state, term_item, dom_object):
        if current_state == 'COLUMN_NAME_DEF':
            self.column_name = term_item
        elif current_state == 'TERM_SEPARATOR':
            if previous_state == 'COLUMN_NAME_DEF':
                dom_object['attributes']['key']['clustering'].append(self.column_type_lookup[self.column_name])
        elif current_state == 'END_SCOPE_TABLE_DEF':
            self.scope_stack -= 1
            if self.scope_stack == 1:
                dom_object['attributes']['key']['clustering'].append(self.column_type_lookup[self.column_name])
                self.scope = 'TABLE_COLUMN_DEF'
            else:
                raise ValueError('ERROR: Unexpected ")" found. Skipping.')

    def __parse_create_table_statement_columns(self, dom_object):
        term_item = self.statement_terms.pop()
        if term_item == '(':
            current_state = None
            previous_state = None
            self.scope = 'TABLE_COLUMN_DEF'
            self.scope_stack = 1
        else:
            print('ERROR: Malformed "CREATE TABLE" statement found. Expecting table definition to be in the format '
                  '("<field_name> <field_type>, ..."). Skipping.')
            return

        while len(self.statement_terms) > 0 and self.scope_stack > 0:
            term_item = self.statement_terms.pop()

            try:
                current_state = SchemaParser.column_grammar_to_state_mapping[term_item]
            except KeyError as e:
                current_state = 'COLUMN_NAME_DEF'

            if self.scope == 'TABLE_COLUMN_DEF':
                self.__parse_create_table_statement_column_def(current_state, previous_state, term_item, dom_object)
            elif self.scope == 'PARTITION_KEY_DEF':
                self.__parse_create_table_statement_partition_def(current_state, previous_state, term_item, dom_object)
            elif self.scope == 'CLUSTERING_KEY_DEF':
                self.__parse_create_table_statement_clustering_def(current_state, previous_state, term_item, dom_object)

            previous_state = current_state

    def __parse_create_table_statement(self):
        # Resolve the table name first before we check if we should skip this table
        try:
            self.__set_table()
        except ValueError as e:
            print(e)
            return

        if self.current_table_parts[0].split('_')[0].lower() in ['dse', 'opscenter', 'solr', 'system']:
            print('Table "{}" belongs to keyspace "{}". Skipping.'.format(
                self.current_table_parts[1],
                self.current_table_parts[0])
            )
            return

        dom_object = {
            'name': '{}.{}'.format(self.current_table_parts[0], self.current_table_parts[1]),
            'attributes': {
                'columns': [],
                'key': {
                    'partition': [],
                    'clustering': []
                },
                'properties': {},
            },
            'statement': self.statement_raw
        }

        try:
            self.__parse_create_table_statement_columns(dom_object)
        except ValueError as e:
            print(e)
            return

        term_item = self.statement_terms.pop()

        if term_item in ['WITH', ';']:
            self.dom.append(dom_object)
        else:
            print('ERROR: Malformed "CREATE TABLE" statement found. Expecting table properties to be in the format '
                  'CREATE TABLE ... (...) WITH (<property>, ...); or CREATE TABLE ... (...);. Skipping.')

    def parse_schema(self, cql_file_path):
        with open(cql_file_path, 'r') as in_cql:
            for raw_line in in_cql:
                statement_part = raw_line.strip()
                if len(statement_part) == 0:
                    continue

                self.statement_raw += raw_line

                for term_item in re.split(r'(\W)', raw_line):
                    term_item = term_item.strip()
                    if term_item and term_item not in ['\'', '"']:
                        self.statement_terms.append(term_item)

                if len(self.statement_terms) > 0 and self.statement_terms[-1] == ';':
                    self.statement_terms.reverse()

                    cql_operation = self.statement_terms.pop().lower()

                    try:
                        self.parse_statement_callback[cql_operation]()
                    except KeyError:
                        print('ERROR: Unsupported CQL operation "{}" found. Skipping.'.format(cql_operation))

                    self.statement_raw = ''
                    self.statement_terms = []

        rtn_dom = copy.deepcopy(self.dom)
        self.__reset_parser_state()

        return rtn_dom
