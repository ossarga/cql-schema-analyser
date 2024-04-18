import copy
import hashlib
import json


class TableTemplateAnalyser:
    def __init__(self):
        # Stores the various table templates we find as we parse the CQL. Store entries in dict in the following format:
        #
        #   '<field_hash>': {
        #       <properties_hash> : {
        #           columns: [<field_type>, ...],
        #           key: {
        #               'partition': [<field_type>, ...],
        #               'clustering': [<field_type>, ...]
        #           },
        #           properties: {
        #               'property_name': 'property_value',
        #               ...
        #           },
        #           statement: '<formatted_cql>',
        #           occurrences: [{'name': '<keyspace.table>', 'match': <match_percentage>},  ...]
        #       }
        #   }
        #
        self.template_definitions = {}

    def __str__(self):
        return json.dumps(self.template_definitions, indent=4)

    @staticmethod
    def __get_field_hash(columns_list, key_dict):
        columns_hash = hashlib.md5()
        columns_list.sort()
        encoded = json.dumps(columns_list).encode()
        columns_hash.update(encoded)

        key_dict['partition'].sort()
        key_dict['clustering'].sort()
        encoded = json.dumps(key_dict, sort_keys=True).encode()
        columns_hash.update(encoded)
        return columns_hash.hexdigest()

    @staticmethod
    def __get_properties_hash(options_dict):
        options_hash = hashlib.md5()
        encoded = json.dumps(options_dict, sort_keys=True).encode()
        options_hash.update(encoded)
        return options_hash.hexdigest()

    def __add_table_template_definition(self, dom_object, columns_hash, props_hash, match_percent):
        self.template_definitions[columns_hash]['variants'].append(props_hash)
        self.template_definitions[columns_hash][props_hash] = {
            'columns': dom_object['attributes']['columns'],
            'key': dom_object['attributes']['key'],
            'properties': dom_object['attributes']['properties'],
            'statement': dom_object['statement'],
            'occurrences': [{'name': dom_object['name'], 'match': match_percent}]
        }

    # Hash columns first
    #  - if match hash options
    #    - if match append with 100% match
    #    - if mismatch compare option values and calculate percentage match
    #      - if percentage match over 90% append with exact percentage match
    def catalog_table_definition(self, dom_object):
        columns_hash = self.__get_field_hash(dom_object['attributes']['columns'], dom_object['attributes']['key'])
        props_hash = self.__get_properties_hash(dom_object['attributes']['properties'])

        if columns_hash in self.template_definitions:
            if props_hash in self.template_definitions[columns_hash]:
                self.template_definitions[columns_hash][props_hash]['occurrences'].append({
                    'name': dom_object['name'],
                    'match': 1
                })
            else:
                ref_props_hash = self.template_definitions[columns_hash]['variants'][0]
                ref_props = copy.deepcopy(self.template_definitions[columns_hash][ref_props_hash]['properties'])
                new_props = copy.deepcopy(dom_object['attributes']['properties'])

                matches = 0
                differences = 0

                for new_key, new_val in new_props.items():
                    if ref_props.pop(new_key, None) == new_val:
                        matches += 1
                    else:
                        differences += 1

                differences += len(ref_props)

                assert (differences + matches) > 0

                match_percent = round(float(matches) / float(differences + matches), 3)

                if match_percent > 0.5:
                    self.template_definitions[columns_hash][ref_props_hash]['occurrences'].append({
                        'name': dom_object['name'],
                        'match': match_percent
                    })
                else:
                    self.__add_table_template_definition(dom_object, columns_hash, props_hash, match_percent)
        else:
            self.template_definitions[columns_hash] = {
                'variants': [],
            }
            self.__add_table_template_definition(dom_object, columns_hash, props_hash, 1)

    def print_table_definitions(self):
        for tbl_key, tbl_value in self.template_definitions.items():
            properties_hash = tbl_value['variants'][0]

            print('\n')
            print('hash: {}'.format(tbl_key))
            print('variants: [{}] - {}'.format(len(tbl_value['variants']), ', '.join(tbl_value['variants'])))
            template_occurrences = 0
            template_occurrences_list = []
            for prop_key in tbl_value['variants']:
                template_occurrences += len(tbl_value[prop_key]['occurrences'])
                for table_inst in tbl_value[prop_key]['occurrences']:
                    template_occurrences_list.append('{} ({}%)'.format(table_inst['name'], table_inst['match'] * 100))
            print('occurrences: [{}] - {}'.format(template_occurrences, ', '.join(template_occurrences_list)))
            print('example cql:\n{}'.format(tbl_value[properties_hash]['statement']))
