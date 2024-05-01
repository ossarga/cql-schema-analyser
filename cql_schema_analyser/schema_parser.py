import copy
import logging
import re


class SchemaParser:
    KS_OK = 0
    KS_UNSELECTED = 1
    KS_IGNORED = 2

    keyspace_status_messages = {
        KS_OK: 'Tables without a keyspace defined will assumed to be in this keyspace when processed.',
        KS_UNSELECTED: 'Keyspace is unselected. Tables without a keyspace defined will assume to be in this '
                       'keyspace and ignored.',
        KS_IGNORED: 'Keyspace is being ignored. Tables without a keyspace defined will assume to be in this '
                    'keyspace and ignored.'
    }

    reserved_keyspaces = {
        'dse_insights',
        'dse_insights_local',
        'dse_leases',
        'dse_security',
        'dse_system',
        'dse_system_local',
        'OpsCenter',
        'solr',
        'system',
        'system_auth',
        'system_distributed',
        'system_schema',
        'system_traces'
    }

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
        self.parsed_keyspaces = {}
        self.ignore_keyspace = None
        self.select_keyspace = None
        self.parse_reserved_keyspaces = False
        self.statement_raw = ''
        self.statement_terms = []
        self.dom = []

        self.scope = ''
        self.scope_stack = 0
        self.column_def_stack = 0
        self.column_name = ''
        self.column_type = ''
        self.column_type_lookup = {}

        self.parse_statement_callback = {
            'create': self.__parse_create_statement,
            'use': self.__parse_use_statement,
        }

        self.parse_create_statement_callback = {
            'keyspace': self.__parse_create_keyspace_statement,
            'table': self.__parse_create_table_statement,
        }

        self.logger = logging.getLogger(__name__)
        logging.basicConfig(format='[%(levelname)s] %(message)s', level=logging.INFO)

    def __reset_parser_state(self):
        self.current_keyspace = ''
        self.parsed_keyspaces = set()
        self.ignore_keyspace = None
        self.select_keyspace = None
        self.statement_raw = ''
        self.statement_terms = []
        self.dom = []

        self.scope = ''
        self.scope_stack = 0
        self.column_def_stack = 0
        self.column_name = ''
        self.column_type = ''
        self.column_type_lookup = {}

    def __get_keyspace_status(self, keyspace):
        if self.select_keyspace:
            if keyspace not in self.select_keyspace:
                return SchemaParser.KS_UNSELECTED
        elif self.ignore_keyspace:
            if keyspace in self.ignore_keyspace:
                return SchemaParser.KS_IGNORED

        return SchemaParser.KS_OK

    def __resolve_table_name(self):
        term_item = self.statement_terms.pop()

        if self.statement_terms[-1] == '.':
            keyspace_name = term_item
            self.statement_terms.pop()
            table_name = self.statement_terms.pop()
        elif self.statement_terms[-1] == '(':
            keyspace_name = self.current_keyspace
            table_name = term_item
        else:
            raise ValueError(
                'Malformed "CREATE TABLE" statement found. Expecting keyspace table dot separator when statement '
                'format is CREATE TABLE [IF NOT EXISTS] <keyspace>.<table> ( ... ), or  column definition opening '
                'bracket when statement format is CREATE TABLE [IF NOT EXISTS] <table> ( ... ). Found "{}" instead. '
                'Ignoring.'.format(self.statement_terms[-1]))

        if not table_name[0].isalnum():
            raise ValueError('Malformed "CREATE TABLE" statement found. Expecting table name to begin with an'
                             'alphanumeric character. Found table name "{}". Skipping.'.format(table_name))

        if keyspace_name in self.parsed_keyspaces:
            keyspace_status = self.parsed_keyspaces[keyspace_name]
            if keyspace_status == SchemaParser.KS_OK:
                return keyspace_name, table_name
            else:
                if keyspace_status == SchemaParser.KS_IGNORED:
                    self.logger.info('Table "{}.{}" defined in CREATE statement. Table is in an ignored keyspace '
                                     'and will be ignored.'.format(keyspace_name, table_name))
                elif keyspace_status == SchemaParser.KS_UNSELECTED:
                    self.logger.info('Table "{}.{}" defined in CREATE statement. Table is in an unselected keyspace'
                                     'and will be ignored.'.format(keyspace_name, table_name))
        else:
            self.logger.warn('Table "{}.{}" is in an undefined keyspace. Ignoring CREATE statement.'.format(
                keyspace_name,
                table_name
            ))

        return None

    def __parser_lwt_statement(self):
        lwt_clauses = ['IF', 'NOT', 'EXISTS']
        for idx, val in enumerate(lwt_clauses):
            if self.statement_terms.pop() != val:
                raise ValueError(
                    'Malformed "CREATE KEYSPACE" statement found. '
                    'Expecting "{}" after "{}" Skipping.'.format(val, lwt_clauses[idx - 1])
                )

    def __parse_create_statement(self):
        if len(self.statement_terms) < 2:
            self.logger.error('Incomplete "CREATE" statement found. Ignoring.')
            return

        create_statement = self.statement_terms.pop().lower()

        try:
            self.parse_create_statement_callback[create_statement]()
        except KeyError:
            self.logger.error('Unsupported CREATE statement "{}" found. Ignoring.'.format(create_statement))

    def __parse_use_statement(self):
        term_item = self.statement_terms.pop()
        if term_item[0].isalnum():
            if term_item in self.parsed_keyspaces:
                self.current_keyspace = term_item
                self.logger.info('Keyspace "{}" selected in USE statement. {}'.format(
                    term_item,
                    self.keyspace_status_messages[self.parsed_keyspaces[term_item]]
                ))
            else:
                self.logger.warning('Keyspace "{}" is undefined. Ignoring USE statement.'.format(term_item))
        else:
            raise ValueError('Malformed USE KEYSPACE statement. Expecting keyspace name; found "{}".'.format(term_item))

    def __parse_create_keyspace_statement(self):
        if self.statement_terms[-1] == 'IF':
            try:
                self.__parser_lwt_statement()
            except ValueError as e:
                self.logger.error(e)
                return

        term_item = self.statement_terms.pop()
        if term_item[0].isalnum() and term_item.lower() != 'with':
            if term_item in self.parsed_keyspaces:
                raise ValueError('Keyspace "{}" already defined. Skipping.'.format(term_item))

            process_rtn_val = self.__get_keyspace_status(term_item)
            self.parsed_keyspaces[term_item] = process_rtn_val
            self.current_keyspace = term_item
            self.logger.info('Keyspace "{}" defined in CREATE statement. {}'.format(
                term_item,
                self.keyspace_status_messages[process_rtn_val]
            ))
        else:
            raise ValueError('Malformed CREATE KEYSPACE statement. Expecting keyspace name; found "{}".'.format(
                term_item
            ))

    def __parse_create_table_statement_column_def(self, current_state, previous_state, term_item, dom_object):
        if current_state == 'START_SCOPE_TABLE_DEF':
            self.scope_stack += 1
            if previous_state == 'KEY_CLAUSE':
                if len(dom_object['attributes']['key']['partition']):
                    raise ValueError('Multiple primary keys found. Skipping.')
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
                        raise ValueError('Multiple primary keys found. Skipping.')
                    else:
                        dom_object['attributes']['key']['partition'].append(self.column_type)
        elif current_state == 'PRIMARY_CLAUSE':
            pass
        elif current_state == 'KEY_CLAUSE':
            if previous_state != 'PRIMARY_CLAUSE':
                raise ValueError('Unexpected "KEY" clause found. Skipping.')
        elif current_state == 'END_SCOPE_TABLE_DEF':
            self.scope_stack -= 1
            dom_object['attributes']['columns'].append(self.column_type)
            if self.scope_stack == 0:
                if not len(dom_object['attributes']['key']['partition']):
                    raise ValueError('No primary key found. Skipping.')

    def __parse_create_table_statement_partition_def(self, current_state, previous_state, term_item, dom_object):
        if current_state == 'COLUMN_NAME_DEF':
            self.column_name = term_item
        elif current_state == 'START_SCOPE_TABLE_DEF':
            self.scope_stack += 1
            if self.scope_stack > 3:
                raise ValueError('Unexpected "(" found. Skipping.')
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
                raise ValueError('Unexpected ")" found. Skipping.')

    def __parse_create_table_statement_columns(self, dom_object):
        term_item = self.statement_terms.pop()
        if term_item == '(':
            current_state = None
            previous_state = None
            self.scope = 'TABLE_COLUMN_DEF'
            self.scope_stack = 1
        else:
            print('Malformed "CREATE TABLE" statement found. Expecting table definition to be in the format '
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
        if self.statement_terms[-1] == 'IF':
            try:
                self.__parser_lwt_statement()
            except ValueError as e:
                self.logger.error(e)
                return

        try:
            table_name_parts = self.__resolve_table_name()
        except ValueError as e:
            self.logger.error(e)
            return

        if not table_name_parts:
            return

        dom_object = {
            'name': '{}.{}'.format(table_name_parts[0], table_name_parts[1]),
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
            self.logger.error(e)
            return

        term_item = self.statement_terms.pop()

        if term_item.lower() in ['with', ';']:
            self.dom.append(dom_object)
        else:
            self.logger.error('Malformed "CREATE TABLE" statement found. Expecting table properties to be in the '
                              'format CREATE TABLE ... (...) WITH (<property>, ...); or CREATE TABLE ... (...);. '
                              'Ignoring.')

    def parse_schema(self, cql_file_path, ignore_keyspace=None, select_keyspace=None, parse_reserved_keyspaces=False):
        if ignore_keyspace:
            self.ignore_keyspace = set(ignore_keyspace)
            if not parse_reserved_keyspaces:
                self.ignore_keyspace = self.ignore_keyspace.union(SchemaParser.reserved_keyspaces)
        if select_keyspace:
            self.select_keyspace = set(select_keyspace)
            if parse_reserved_keyspaces:
                self.select_keyspace = self.select_keyspace.union(SchemaParser.reserved_keyspaces)

        self.logger.info('Parsing CQL schema file "{}"'.format(cql_file_path))
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
                        self.logger.error('Unsupported CQL operation "{}" found. Skipping.'.format(cql_operation))

                    self.statement_raw = ''
                    self.statement_terms = []

        rtn_dom = copy.deepcopy(self.dom)
        self.__reset_parser_state()

        return rtn_dom
