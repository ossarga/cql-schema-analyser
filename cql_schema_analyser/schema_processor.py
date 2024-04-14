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

    def process_schema(self, schema_file):
        print('Parsing schema file: {}'.format(schema_file))
        cql_schema_dom = self.cql_schema_paser.parse_schema(schema_file)

        for dom_obj in cql_schema_dom:
            self.cql_table_template_analyser.catalog_table_definition(dom_obj)

        self.cql_table_template_analyser.print_table_definitions()

    @staticmethod
    def main_cli():
        arg_parser = argparse.ArgumentParser(description='Process a CQL schema file and output the table definitions.')
        arg_parser.add_argument('schema_file', help='The CQL schema file to process.')
        schema_proc_args = arg_parser.parse_args()

        schema_processor = SchemaProcessor()
        schema_processor.process_schema(schema_proc_args.schema_file)


if __name__ == '__main__':
    sys.exit(SchemaProcessor.main_cli())