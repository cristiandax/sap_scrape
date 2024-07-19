import pandas as pd
import uuid
from datetime import datetime
import json

# Set up input
input_file = 'scrape_dump.xlsx'# - Reduced
# Set up output
output_dict = {}

# Set up basic counts and elements
def get_uuid():
  global uuid
  uuid_str = str(uuid.uuid4())
  return uuid_str;
  
internal_id_int = 10
def get_internal_id():
  global internal_id_int
  internal_id_int = internal_id_int + 1
  id_str = 'o' + str(internal_id_int)
  return id_str;  
  
def get_timestamp():
  return int(datetime.timestamp(datetime.now()));

 def get_operator_name():
  return 'Mr Fish'; 
  
  
def get_column_code(name_str):
  return name_str.strip().replace(" ", "_").replace(".", "_").upper();

def convert_to_int(val):
  return int(val) if val != '' else val;

# read Domain sheet
df_domain = pd.read_excel(input_file, sheet_name = "Domain")
df_domain = df_domain.fillna('')
df_domain['Length'] = df_domain['Length'].apply(lambda val: convert_to_int(val)) # converts the int column to int values, skipping the filled NaNs.
df_domain['Id'] = df_domain.apply (lambda row: get_internal_id(), axis=1)
df_domain['ObjectID'] = df_domain.apply (lambda row: get_uuid(), axis=1)
df_domain['Code'] = df_domain.apply (lambda row: get_column_code(row['Name']), axis=1)
df_domain['CreationDate'] = df_domain.apply (lambda row: get_timestamp(), axis=1)
df_domain['Creator'] = df_domain.apply (lambda row: get_operator_name(), axis=1)
df_domain['ModificationDate'] = df_domain.loc[:, 'CreationDate']
df_domain['Modifier'] = df_domain.loc[:, 'Creator']
df_domain['Index'] = df_domain.loc[:, 'Name']

#df_domain

#domain_list = df_domain.to_dict('records')
domain_dict = df_domain.set_index('Index').T.to_dict('dict')
output_dict['Domains'] = domain_dict
#print(output_dict)  

# read Table sheet
df_table = pd.read_excel(input_file, sheet_name = "allTablesTable")
df_table.columns = ['Name', 'Annotation', 'Comment', 'Description']
df_table['Id'] = df_table.apply (lambda row: get_internal_id(), axis=1)
df_table['ObjectID'] = df_table.apply (lambda row: get_uuid(), axis=1)
df_table['Code'] = df_table.apply (lambda row: get_column_code(row['Name']), axis=1)
df_table['CreationDate'] = df_table.apply (lambda row: get_timestamp(), axis=1)
df_table['Creator'] = df_table.apply (lambda row: get_operator_name(), axis=1)
df_table['ModificationDate'] = df_table.loc[:, 'CreationDate']
df_table['Modifier'] = df_table.loc[:, 'Creator']
df_table['Index'] = df_table.loc[:, 'Name']

#df_table


table_dict = df_table.set_index('Index').T.to_dict('dict')
output_dict['Tables'] = table_dict
#print(output_dict)



# read PK sheet
df_pk = pd.read_excel('scrape_dump - Reduced.xlsx', sheet_name = "allPkTable")

df_pk = pd.pivot_table(df_pk, index='tableName', values='pkFieldName', aggfunc=lambda x:list(x))

df_pk['tableName'] = df_pk.index
df_pk['Name'] = df_pk.index + '_PK'

df_pk['Id'] = df_pk.apply (lambda row: get_internal_id(), axis=1)
df_pk['ObjectID'] = df_pk.apply (lambda row: get_uuid(), axis=1)
df_pk['Code'] = df_pk.apply (lambda row: get_column_code(row['Name']), axis=1)
df_pk['CreationDate'] = df_pk.apply (lambda row: get_timestamp(), axis=1)
df_pk['Creator'] = df_pk.apply (lambda row: get_operator_name(), axis=1)
df_pk['ModificationDate'] = df_pk.loc[:, 'CreationDate']
df_pk['Modifier'] = df_pk.loc[:, 'Creator']

#df_pk

for row_dict in df_pk.to_dict(orient='records'):
  #print(row_dict)
  output_dict['Tables'][row_dict['tableName']]['PrimaryKeyDict'] = row_dict

#print(output_dict)


# read Fields sheet
df_column = pd.read_excel(input_file, sheet_name = "allFieldsTable")
df_column = df_column.fillna('')
df_column['isEnum'] = df_column['isEnum'].apply(lambda val: convert_to_int(val)) # converts the int column to int values, skipping the filled NaNs.
df_column.columns = ['tableName', 'Name', 'Comment', 'dataElement', 'checkTable', 'dataType', 'blank', 'Length', 'Precision', 'isEnum']
df_column = df_column.drop(columns=['dataElement', 'checkTable', 'blank'])

df_column['Id'] = df_column.apply (lambda row: get_internal_id(), axis=1)
df_column['ObjectID'] = df_column.apply (lambda row: get_uuid(), axis=1)
df_column['Code'] = df_column.apply (lambda row: get_column_code(row['Name']), axis=1)
df_column['CreationDate'] = df_column.apply (lambda row: get_timestamp(), axis=1)
df_column['Creator'] = df_column.apply (lambda row: get_operator_name(), axis=1)
df_column['ModificationDate'] = df_column.loc[:, 'CreationDate']
df_column['Modifier'] = df_column.loc[:, 'Creator']

#df_column


def apply_domain_to_column(row):
  if (
    row['dataType'] == 'CHAR'
    or row['dataType'] == 'CURR'
    or row['dataType'] == 'DEC'
    or row['dataType'] == 'QUAN'
    or row['dataType'] == 'RAW'
  ):
    return ''
  else:
    return row['dataType']

def apply_datatype_to_column(row):
  if (row['dataType'] == 'CHAR'):
    return 'char(' + str(row['Length']) + ')'
  elif (row['dataType'] == 'CURR'):
    return 'decimal(' + str(row['Length']) + ',' + str(row['Precision']) + ')'
  elif (row['dataType'] == 'DEC'):
    return 'decimal(' + str(row['Length']) + ',' + str(row['Precision']) + ')'
  elif (row['dataType'] == 'QUAN'):
    return 'decimal(' + str(row['Length']) + ',' + str(row['Precision']) + ')'
  elif (row['dataType'] == 'RAW'):
    return 'binary(' + str(row['Length']) + ')'
  else:
    return ''


df_column['Domain'] = df_column.apply (lambda row: apply_domain_to_column(row), axis=1)
df_column['dataType'] = df_column.apply (lambda row: apply_datatype_to_column(row), axis=1)
#df_column


for row_dict in df_column.to_dict(orient='records'):
  #print(row_dict)
  if 'Columns' not in output_dict['Tables'][row_dict['tableName']]:
    output_dict['Tables'][row_dict['tableName']]['Columns'] = {}
  output_dict['Tables'][row_dict['tableName']]['Columns'][row_dict['Name']] = row_dict

#print(output_dict)

# read Enums sheet
df_enum = pd.read_excel('scrape_dump - Reduced.xlsx', sheet_name = "allEnumTable")

#df_enum

for row_dict in df_enum.to_dict(orient='records'):
  #print(row_dict)
  if 'Enums' not in output_dict['Tables'][row_dict['tableName']]['Columns'][row_dict['fieldName']]:
    output_dict['Tables'][row_dict['tableName']]['Columns'][row_dict['fieldName']]['Enums'] = {}
  output_dict['Tables'][row_dict['tableName']]['Columns'][row_dict['fieldName']]['Enums'][row_dict['enumCode']] = row_dict['enumDescription']

#print(output_dict)


# read FKs sheet
df_fk = pd.read_excel(input_file, sheet_name = "allFkTable")
df_fk = df_fk.fillna('NaN')
df_fk = df_fk[df_fk.fieldName != 'NaN']

df_fk.columns = ['childTableName', 'Drop1', 'Drop2', 'childFieldName', 'parentTableName', 'targetTableGroup', 'parentTableField']
df_fk = df_fk.drop(columns=['Drop1', 'Drop2', 'targetTableGroup'])

df_fk['Name'] = df_fk['childTableName'] + '.' + df_fk['childFieldName'] + '_to_' + df_fk['parentTableName']
df_fk['ReferenceId'] = df_fk.apply (lambda row: get_internal_id(), axis=1)
df_fk['ReferenceObjectID'] = df_fk.apply (lambda row: get_uuid(), axis=1)
df_fk['ReferenceJoinId'] = df_fk.apply (lambda row: get_internal_id(), axis=1)
df_fk['ReferenceJoinObjectID'] = df_fk.apply (lambda row: get_uuid(), axis=1)
df_fk['Code'] = df_fk.apply (lambda row: get_column_code(row['Name']), axis=1)
df_fk['CreationDate'] = df_fk.apply (lambda row: get_timestamp(), axis=1)
df_fk['Creator'] = df_fk.apply (lambda row: get_operator_name(), axis=1)
df_fk['ModificationDate'] = df_fk.loc[:, 'CreationDate']
df_fk['Modifier'] = df_fk.loc[:, 'Creator']

#df_fk


fk_dict = df_fk.to_dict('records')
output_dict['References'] = fk_dict
#print(output_dict)



with open('pdm.json', 'w', encoding='utf-8') as output_file:
  json.dump(output_dict, output_file, ensure_ascii=False, indent=2)




