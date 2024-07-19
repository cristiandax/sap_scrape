import abbreviate
import textwrap
import requests
from bs4 import BeautifulSoup
import pandas as pd

table_names = []
seen_table_names = set()
all_tables_table = []
all_fields_table = []
all_fk_table = []
all_pk_table = []
all_enum_table = []


def append_to_table_names(new_table_names):
    # global table_names
    # global seen_table_names
    global init_table_names

    # using 'not in' gets very slow on lists,
    # 'set()' does not maintain order :(
    for new_table_name in new_table_names:
        if new_table_name not in seen_table_names and new_table_name in init_table_names:
            seen_table_names.add(new_table_name)
            table_names.append(new_table_name)


def process_fk_table(fk_table, all_fk_table):
    table_rows = []
    new_table_names = []

    for tr in fk_table.find_all("tr", recursive=False):  # , limit=5
        # print('8888888888888888888')
        tds = tr.find_all("td", recursive=False)
        cell_contents = []

        for td in tds:
            # print('index:' + str(tr.index(td)))
            # print(td.text.strip())

            # 9 is the checktable
            if tr.index(td) == 9:
                if td.text.strip() != '':
                    new_table_names.append(td.text.strip())

            cell_contents.append(td.text.strip())

        # print(cell_contents)
        table_rows.append(cell_contents)

    # global all_fk_table
    all_fk_table = all_fk_table + table_rows

    # print(table_rows)
    # print(new_table_names)
    append_to_table_names(new_table_names)
    # print(table_names)


def process_fields_table(table_name, fields_table, all_enum_table):
    new_table_names = []

    for tr in fields_table.find_all("tr", recursive=False):  # , limit=5
        # print('8888888888888888888')
        cell_contents = [table_name]
        is_pk = 0
        current_field_name = ''

        # # flag PK if it's a PK field
        # if tr.has_attr('class'):
        #     if tr['class'][
        #         0] == 'info':  # tr['class'] is a list, info should be the only one there, and means it's a PK
        #         is_pk = 1

        tds = tr.find_all("td", recursive=False)
        for td in tds:
            # 1 is the field name
            if tr.index(td) == 1:
                current_field_name = td.text.strip()
                # if is_pk == 1:
                #     global all_pk_table
                #     all_pk_table.append([table_name, td.text.strip()])

            # print('index:' + str(tr.index(td)))

            # 7 is the checktable
            if tr.index(td) == 7:
                if td.text.strip() != '':
                    new_table_names.append(td.text.strip())

            if 'Possible values' in td.text:
                # print ('bonus content!')
                cell_contents.append('1')
                process_enumerated_values(table_name, current_field_name, td, all_enum_table)
            else:
                cell_contents.append(td.text.strip())

        global all_fields_table
        # print(cell_contents)
        all_fields_table.append(cell_contents)

    # print(new_table_names)
    append_to_table_names(new_table_names)

    # df = pd.DataFrame(tableRows)
    # df


def process_enumerated_values(table_name, current_field_name, incoming_element, all_enum_table):
    for tr in incoming_element.find_all("tr"):  # , limit=5
        cell_contents = [table_name, current_field_name]
        for td in tr.find_all("td", recursive=False):
            cell_contents.append(td.text.strip())

        all_enum_table.append(cell_contents)


def dump_scraped_data(write_in_excel=False):
    # print(all_tables_table)
    # print(all_fields_table)
    # print(all_fk_table)
    # print(all_pk_table)
    # print(all_enum_table)

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

    # abbr = abbreviate.Abbreviate()

    output['Column Description Short'] = output['Column Description Short'].str.lower()
    output['Column Description Short'] = output['Column Description Short'].str.replace(' ', '_')
    output['Column Description Short'] = output['Column Description Short'].str.replace('(', '_')
    output['Column Description Short'] = output['Column Description Short'].str.replace(')', '_')
    output['Column Description Short'] = output['Column Description Short'].str.replace('/', '_')
    output['Column Description Short'] = output['Column Description Short'].str.slice(0, 10)

    output.to_csv()
    # output = output.apply(lambda x: abbr(x['Column Description Short']), axis=1)
    # output.applymap(abbr).loc[:, 'Column Description Short']

    if write_in_excel is True:
        with pd.ExcelWriter('pandas_to_excel.xlsx') as writer:
            # pd.DataFrame(table_names).to_excel(writer, sheet_name='TableNames')
            tables_description.to_excel(writer, sheet_name='AllTablesTable')
            columns_description.to_excel(writer, sheet_name='AllFieldsTable')
            # pd.DataFrame(all_fk_table).to_excel(writer, sheet_name='AllFkTable')
            # pd.DataFrame(all_pk_table).to_excel(writer, sheet_name='AllPkTable')
            # pd.DataFrame(all_enum_table).to_excel(writer, sheet_name='AllEnumTable')


def main(init_table_names, all_fk_table, all_enum_table):
    append_to_table_names(init_table_names)

    try:
        breaker = 0
        for table_name in table_names:
            if breaker == 500: break

            URL = "https://www.leanx.eu/en/sap/table/" + table_name + ".html"

            print('breaker:' + str(breaker))
            # print(URL)

            page = requests.get(URL)

            cell_contents = []

            # print(tableName)
            # print(page.text)
            cell_contents.append(table_name)

            soup = BeautifulSoup(page.content, "html.parser")
            main_content = soup.find(class_="main-container")
            # print(mainContent.prettify())

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
            # print(contentTableHeaders[0].prettify())

            try:
                fields_table = content_table_headers[0].findNext("tbody")
                # print(fieldsTable.prettify())
                process_fields_table(table_name, fields_table, all_enum_table)
            except AttributeError:
                pass
            except IndexError:
                pass

            # if len(content_table_headers) == 2:
            #     fk_table = content_table_headers[1].findNext("tbody")
            #     # print(fkTable.prettify())
            #
            #     # process_fk_table(fk_table, all_fk_table)

            breaker = breaker + 1
    except:
        dump_scraped_data()
        raise

    dump_scraped_data()


# init_table_names = ['EBAN', 'NAST', 'EBUB']
init_table_names = ['EBAN', 'NAST']

main(init_table_names, all_fk_table, all_enum_table)
