import textwrap
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re


table_names = []
seen_table_names = set()
all_tables_table = []
all_fields_table = []
all_fk_table = []
all_pk_table = []
all_enum_table = []


def unique_list(list):
    list_unique = np.array(list)
    return list_unique


def load_tables(csv_path):
    tables = pd.read_csv(csv_path)
    tables['filename'] = tables['filename'].str.split('.').str[0].str.split('_').str[0]
    tables_list = tables['filename'].values.tolist()

    return tables_list


def append_to_table_names(new_table_names):
    global init_table_names

    for new_table_name in new_table_names:
        if new_table_name not in seen_table_names and new_table_name in init_table_names:
            seen_table_names.add(new_table_name)
            table_names.append(new_table_name)


def process_fk_table(fk_table, all_fk_table):
    table_rows = []
    new_table_names = []

    for tr in fk_table.find_all("tr", recursive=False):  # , limit=5
        tds = tr.find_all("td", recursive=False)
        cell_contents = []

        for td in tds:
            if tr.index(td) == 9:
                if td.text.strip() != '':
                    new_table_names.append(td.text.strip())

            cell_contents.append(td.text.strip())
        table_rows.append(cell_contents)

    all_fk_table = all_fk_table + table_rows
    append_to_table_names(new_table_names)


def process_fields_table(table_name, fields_table, all_enum_table):
    new_table_names = []

    for tr in fields_table.find_all("tr", recursive=False):  # , limit=5
        cell_contents = [table_name]
        is_pk = 0
        current_field_name = ''

        tds = tr.find_all("td", recursive=False)
        for td in tds:
            # 1 is the field name
            if tr.index(td) == 1:
                current_field_name = td.text.strip()

            if tr.index(td) == 7:
                if td.text.strip() != '':
                    new_table_names.append(td.text.strip())

            if 'Possible values' in td.text:
                cell_contents.append('1')
                process_enumerated_values(table_name, current_field_name, td, all_enum_table)
            else:
                cell_contents.append(td.text.strip())

        global all_fields_table
        all_fields_table.append(cell_contents)

    append_to_table_names(new_table_names)


def process_enumerated_values(table_name, current_field_name, incoming_element, all_enum_table):
    for tr in incoming_element.find_all("tr"):  # , limit=5
        cell_contents = [table_name, current_field_name]
        for td in tr.find_all("td", recursive=False):
            cell_contents.append(td.text.strip())

        all_enum_table.append(cell_contents)


def dump_scraped_data():
    tables_description = pd.DataFrame(all_tables_table)
    tables_description.columns = ['Table Name', 'SAP Table Info', 'Table Description', 'LEANX Link']
    tables_description = tables_description.drop(['SAP Table Info', 'LEANX Link'], axis=1)
    columns_description = pd.DataFrame(all_fields_table)
    columns_description.columns = ['Table Name','Column Name','Column Description','Data Element','Checktable','Datatype', 'Empty_1', 'Length', 'Decimals', 'Empty_2']
    columns_description = columns_description.drop(['Empty_1', 'Empty_2'], axis=1)

    output = pd.merge(columns_description, tables_description, on="Table Name")

    output = output.loc[:,['Table Name', 'Table Description', 'Column Name', 'Column Description', 'Data Element', 'Checktable', 'Datatype', 'Length', 'Decimals']]
    output['Table Description Short'] = output['Table Description']
    output['Column Description Short'] = output['Column Description']

    output.to_csv('SAP_Tables_Columns.csv', index=None)


def main(init_table_names, all_fk_table, all_enum_table):
    append_to_table_names(init_table_names)

    try:
        breaker = 0
        for table_name in table_names:
            if breaker == 500: break

            URL = "https://www.leanx.eu/en/sap/table/" + table_name + ".html"

            print('breaker:' + str(breaker))

            page = requests.get(URL)
            cell_contents = []
            cell_contents.append(table_name)

            soup = BeautifulSoup(page.content, "html.parser")
            main_content = soup.find(class_="main-container")

            try:
                table_long_name = main_content.find("h1").text
                print(table_long_name)
                cell_contents.append(table_long_name)
            except AttributeError:
                cell_contents.append('table_long_name not found')

            try:
                table_short_description = main_content.find("h2").text
                print(table_short_description)
                cell_contents.append(table_short_description)
            except AttributeError:
                cell_contents.append('table_short_description not found')

            cell_contents.append(URL)

            all_tables_table.append(cell_contents)

            content_table_headers = main_content.find_all("h3")

            try:
                fields_table = content_table_headers[0].findNext("tbody")
                process_fields_table(table_name, fields_table, all_enum_table)
            except AttributeError:
                pass
            except IndexError:
                pass

            breaker = breaker + 1
    except:
        dump_scraped_data()
        raise

    dump_scraped_data()


def load_tables(csv_path):
    tables = pd.read_csv(csv_path)
    tables['filename'] = tables['filename'].str.split('.').str[0].str.split('_').str[0]
    tables_list = tables['filename'].values.tolist()

    return tables_list


def urlify(s):

    # Remove all non-word characters (everything except numbers and letters)
    s = re.sub(r"[^\w\s\-]", '_', s)

    # Replace all runs of whitespace with a single dash
    s = re.sub(r"\s+", '_', s)
    s = re.sub(r"[-]", '_', s)
    s = re.sub(r"[-*]", '_', s)
    s = re.sub(r"[_*]", '_', s)
    s = re.sub(r'(_)(?=_*\1)', '', s)
    s = s.strip('_')

    return s


def prepare_short_strings(csv_path):
    mapping_table = pd.read_csv(csv_path)

    mapping_table['Column Description Short'] = mapping_table['Column Description']
    mapping_table['Column Description Short'] = mapping_table['Column Description Short'].apply(urlify)
    mapping_table['Column Description Short'] = mapping_table['Column Description Short'].str.lower()

    mapping_table['Table Description Short'] = mapping_table['Table Description']
    mapping_table['Table Description Short'] = mapping_table['Table Description Short'].apply(urlify)
    mapping_table['Table Description Short'] = mapping_table['Table Description Short'].str.lower()

    return mapping_table


def write_in_file(destination_file_path, content):
    with open(destination_file_path, 'w+', newline='', encoding='utf-8') as f:
        for line in content:
            f.write(f"{line}\n")


# init_table_names = ['EBAN', 'NAST', 'EBUB']
init_table_names = unique_list(load_tables('tables.csv'))
target_database_name = 'prd_internal_cfb_cbi_supply_chain'

create_statements = []

# main(init_table_names, all_fk_table, all_enum_table)
croda_mapping = prepare_short_strings('SAP_Tables_Columns.csv')

croda_mapping = croda_mapping.rename(columns={'Table Name': 'SAP Table Name',
                                              'Column Name': 'SAP Column Name',
                                              'Table Description': 'LEANX Table Description',
                                              'Column Description': 'LEANX Column Description',
                                              'Column Description Short': 'CDP Column translated'
                                              })

croda_mapping['CDP Table Name raw'] = 'croda_' + croda_mapping['SAP Table Name'].str.lower() + '_raw'
croda_mapping['CDP Table Name translated'] = 'croda_' + croda_mapping['SAP Table Name'].str.lower() + '_' + croda_mapping['Table Description Short'].str.lower()

croda_mapping['CDP New Table Name'] = ''
croda_mapping['CDP New Table Column'] = ''

croda_mapping = croda_mapping[['SAP Table Name', 'SAP Column Name',
                               'LEANX Table Description', 'LEANX Column Description',
                               'CDP Table Name raw', 'CDP Table Name translated', 'CDP Column translated',
                               'CDP New Table Name', 'CDP New Table Column']]

croda_mapping.to_csv('SAP_Tables_Columns_prepared.csv', index=None)

def prepare_create_translated_statements(dataframe):

    tables_df = dataframe[['SAP Table Name', 'LEANX Table Description', 'CDP Table Name translated']]
    tables_df.drop_duplicates(inplace=True)
    # Index(['SAP Table Name', 'SAP Column Name', 'LEANX Table Description',
    #        'LEANX Column Description', 'CDP Table Name raw',
    #        'CDP Table Name translated', 'CDP Column translated',
    #        'CDP New Table Name', 'CDP New Table Column'],
    #       dtype='object')

    for index, row in tables_df.iterrows():
        process_table = dataframe[dataframe['SAP Table Name'].str.contains(row['SAP Table Name'])]
        database_name = target_database_name
        table_name = process_table['CDP Table Name translated'].drop_duplicates().values[0]
        table_description = process_table['LEANX Table Description'].drop_duplicates().values[0]
        col_defs = []
        for index_col, row_col in process_table.iterrows():
            col_defs.append(f"\r\n\t{row_col['CDP Column translated']} STRING COMMENT '{row_col['LEANX Column Description']}'")

        col_defs_str = ", ".join(col_defs)
        create_stmt = f'''CREATE EXTERNAL TABLE {database_name}.{table_name}
({col_defs_str}
)
COMMENT '{table_description}'
STORED AS PARQUET LOCATION 'hdfs://nameservice1/prd/internal/cfb_cbi_supply_chain/croda/';
'''
        create_statements.append(create_stmt)



prepare_create_translated_statements(dataframe=croda_mapping)

write_in_file(destination_file_path='statements.sql', content=create_statements)
