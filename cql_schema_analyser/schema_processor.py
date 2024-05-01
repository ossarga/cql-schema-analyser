#!/usr/bin/env python

import argparse
import os
import sys

try:
    import cql_schema_analyser.schema_parser as schema_parser
    import cql_schema_analyser.table_template_analyser as table_template_analyser
except ModuleNotFoundError:
    # Catch the case where we are calling the process.py directly from the parent directory.
    # In this case add the parent directory to the system path so that we can import the redactor modules.
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    import cql_schema_analyser.schema_parser as schema_parser
    import cql_schema_analyser.table_template_analyser as table_template_analyser


class SchemaProcessor:
    def __init__(self):
        self.cql_schema_paser = schema_parser.SchemaParser()
        self.cql_table_template_analyser = table_template_analyser.TableTemplateAnalyser()

    def process_schema(self, schema_file, ignore_keyspace=None, select_keyspace=None, parse_reserved_keyspaces=False):
        cql_schema_dom = self.cql_schema_paser.parse_schema(
            schema_file,
            ignore_keyspace=ignore_keyspace,
            select_keyspace=select_keyspace,
            parse_reserved_keyspaces=parse_reserved_keyspaces
        )

        for dom_obj in cql_schema_dom:
            self.cql_table_template_analyser.catalog_table_definition(dom_obj)

        self.cql_table_template_analyser.print_table_definitions()

    @staticmethod
    def main_cli():
        arg_parser = argparse.ArgumentParser(description='Process a CQL schema file and output the table definitions.')
        arg_parser.add_argument('schema_file', help='The CQL schema file to process.')
        arg_parser.add_argument(
            '-i',
            '--ignore-keyspace',
            dest='ignore_keyspace',
            nargs='+',
            default=[],
            help='One or more keyspaces to ignore when processing the schema. All tables in that keyspace will be '
                 'ignored. The --select-keyspace option takes precedence over this option. That is, if a keyspace is '
                 'specified in both this and the --select-keyspace option, it will be processed rather than ignored. '
        )
        arg_parser.add_argument(
            '-s',
            '--select-keyspace',
            dest='select_keyspace',
            nargs='+',
            default=[],
            help='One or more keyspaces to exclusively select when processing the schema. All other keyspaces and '
                 'their associated tables will be ignored. This option takes precedence over the --ignore-keyspace '
                 'option. That is, if a keyspace is specified in both this and the --ignore-keyspace option, it will '
                 'be processed rather than ignored. Defaults to all keyspaces being processed except for those listed '
                 'in the --ignore_keyspace option.'
        )
        arg_parser.add_argument(
            '-r',
            '--parse-reserved-keyspaces',
            dest='parse_reserved_keyspaces',
            action='store_true',
            help='Parse the reserved dse*, OpsCenter, solr, and system* keyspaces. '
                 'By default, these keyspaces are ignored.'
        )
        schema_proc_args = arg_parser.parse_args()

        schema_processor = SchemaProcessor()
        schema_processor.process_schema(
            schema_proc_args.schema_file,
            ignore_keyspace=schema_proc_args.ignore_keyspace,
            select_keyspace=schema_proc_args.select_keyspace,
            parse_reserved_keyspaces=schema_proc_args.parse_reserved_keyspaces
        )


if __name__ == '__main__':
    sys.exit(SchemaProcessor.main_cli())