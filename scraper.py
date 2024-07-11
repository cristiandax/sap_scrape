import requests
from bs4 import BeautifulSoup
import pandas as pd

tableNames = []
seenTableNames = set()
def appendToTableNames(newTableNames):
  global tableNames
  global seenTableNames

  #using 'not in' gets very slow on lists,
  #'set()' does not maintain order :(
  for newTableName in newTableNames:
    if newTableName not in seenTableNames:
      seenTableNames.add(newTableName)
      tableNames.append(newTableName)
 
allTablesTable = []
allFieldsTable = []
allFkTable = []
allPkTable = []
allEnumTable = [] 


def processFkTable(fkTable):
  tableRows = []
  newTableNames = []

  for tr in fkTable.find_all("tr", recursive=False): #, limit=5
    #print('8888888888888888888')
    tds = tr.find_all("td", recursive=False)
    cellContents = []

    for td in tds:
      #print('index:' + str(tr.index(td)))
      #print(td.text.strip())

      # 9 is the checktable
      if tr.index(td) == 9:
        if td.text.strip() != '':
          newTableNames.append(td.text.strip())

      cellContents.append(td.text.strip())

    #print(cellContents)
    tableRows.append(cellContents)

  global allFkTable
  allFkTable = allFkTable + tableRows

  #print(tableRows)
  #print(newTableNames)
  appendToTableNames(newTableNames)
  #print(tableNames)


def processFieldsTable(tableName, fieldsTable):
  newTableNames = []

  for tr in fieldsTable.find_all("tr", recursive=False): #, limit=5
    #print('8888888888888888888')
    cellContents = [tableName]
    isPK = 0
    currentFieldName =''

    # flag PK if it's a PK field
    if tr.has_attr('class'):
      if tr['class'][0] == 'info': # tr['class'] is a list, info should be the only one there, and means it's a PK
        isPK = 1

    tds = tr.find_all("td", recursive=False)
    for td in tds:
      # 1 is the field name
      if tr.index(td) == 1:
        currentFieldName = td.text.strip()
        if isPK == 1:
          global allPkTable
          allPkTable.append([tableName, td.text.strip()])

      #print('index:' + str(tr.index(td)))

      # 7 is the checktable
      if tr.index(td) == 7:
        if td.text.strip() != '':
          newTableNames.append(td.text.strip())

      if 'Possible values' in td.text:
        #print ('bonus content!')
        cellContents.append('1')
        processEnumeratedValues(tableName, currentFieldName, td)
      else:
        cellContents.append(td.text.strip())

    global allFieldsTable
    #print(cellContents)
    allFieldsTable.append(cellContents)

  #print(newTableNames)
  appendToTableNames(newTableNames)

  #df = pd.DataFrame(tableRows)
  #df

def processEnumeratedValues(tableName, currentFieldName, incomingElement):

  for tr in incomingElement.find_all("tr"): #, limit=5
    cellContents = [tableName, currentFieldName]
    for td in tr.find_all("td", recursive=False):
      cellContents.append(td.text.strip())

    global allEnumTable
    allEnumTable.append(cellContents)
    
    
def dumpScrapedData():
  #print(allTablesTable)
  #print(allFieldsTable)
  #print(allFkTable)
  #print(allPkTable)
  #print(allEnumTable)

  with pd.ExcelWriter('pandas_to_excel.xlsx') as writer:
    pd.DataFrame(tableNames).to_excel(writer, sheet_name='tableNames')
    pd.DataFrame(allTablesTable).to_excel(writer, sheet_name='allTablesTable')
    pd.DataFrame(allFieldsTable).to_excel(writer, sheet_name='allFieldsTable')
    pd.DataFrame(allFkTable).to_excel(writer, sheet_name='allFkTable')
    pd.DataFrame(allPkTable).to_excel(writer, sheet_name='allPkTable')
    pd.DataFrame(allEnumTable).to_excel(writer, sheet_name='allEnumTable')    
    
    
initTableNames = [
  'DD01L'
, 'DD02L'
, 'DD02T'
, 'DD02V'
, 'DD03L'
, 'DD03T'
, 'DD04L'
, 'DD04T'
, 'DD07L'
, 'DD09L'
, 'DD12L'
, 'TDEVC'
, 'TFDIR'
, 'TFTIT'
, 'ENLFDIR'
, 'TADIR'
, 'TRDIR'
, 'TMDIR'
, 'D010TAB'
, 'TAPLT'
, 'SEOCLASS'
, 'D020S'
, 'D020T'
, 'TSE05'
, 'DM42S'
, 'T100'
, 'TSTC'
, 'TSTCP'
, 'FILEPATH'
, 'PATH'
, 'SYST'
, 'VBMOD'
, 'VBDATA'
, 'STXFADM'
, 'STXB'
, 'STXH'
, 'STXL'
, 'TSP01'
, 'TSP02'
, 'TST01'
, 'TST03'
, 'TTXOB'
, 'TTXOT'
, 'DOKIL'
, 'DOKHL'
, 'DOKTL'
, 'NRIV'
, 'TNRO'
, 'LTDX'
, 'LTDXD'
, 'LTDXS'
, 'TCVIEW'
, 'USR01'
, 'USR03'
, 'USR02'
, 'USR04'
, 'USR05'
, 'USR10'
, 'UST12'
, 'USR12'
, 'USR21'
, 'USR41'
, 'UST04'
, 'DEVACCESS'
, 'USR40'
, 'USOBT'
, 'TSTCA'
, 'TOBJ'
, 'SKA1'
, 'SKAT'
, 'SKAS'
, 'SKB1'
, 'SKM1'
, 'SKMT'
, 'SKPF'
, 'AUFK'
, 'AFKO'
, 'AFPO'
, 'KNA1'
, 'KNAS'
, 'KNB1'
, 'KNB5'
, 'KNB4'
, 'KNBK'
, 'KNC1'
, 'KNC3'
, 'LFA1'
, 'LFAS'
, 'LFB1'
, 'LFB5'
, 'LFBK'
, 'LFC1'
, 'LFC3'
, 'LFM1'
, 'T078K'
, 'BKPF'
, 'BSEG'
, 'BSID'
, 'BSIK'
, 'BSIP'
, 'BSIS'
, 'BSAD'
, 'BSAK'
, 'BSAS'
, 'BSET'
, 'BSEC'
, 'VBKPF'
, 'F111G'
, 'AGKO'
, 'GLT0'
, 'REGUH'
, 'REGUP'
, 'REGUT'
, 'CSKA'
, 'CSKB'
, 'CSKU'
, 'CSLA'
, 'CSLT'
, 'CSKS'
, 'CSKT'
, 'CSSK'
, 'CSSL'
, 'COSP'
, 'COEP'
, 'COBK'
, 'COST'
, 'TKA01'
, 'TKA02'
, 'KEKO'
, 'KEPH'
, 'KALO'
, 'KANZ'
, 'CEPC'
, 'CKPH'
, 'ADCP'
, 'ADRP'
, 'ADR2'
, 'ADRC'
, 'ADRCITY'
, 'BTCUEV'
, 'BTCUED'
, 'BTCSEV'
, 'BTCSED'
, 'BTCJSTAT'
, 'CCCFLOW'
, 'SDBAC'
, 'VRSX'
, 'CVERS'
, 'PAT03'
, 'TPROT'
, 'DBTABLOG'
, 'E070'
, 'E71K'
, 'E070C'
, 'TLOCK'
, 'VARID'
, 'VARIT'
, 'TVARV'
, 'VARI'
, 'VARIS'
, 'SOFM'
, 'SOOS'
, 'SWWWIHEAD'
, 'CDHDR'
, 'CDPOS'
, 'JCDS'
, 'JEST'
, 'JSTO'
, 'ARCH_OBJ'
, 'SNAP'
, 'TBTCO'
, 'TBTCP'
, 'DDSHPVAL5'
, 'TCURC'
, 'TCURT'
, 'TCURR'
, 'TCURX'
, 'TCURF'
, 'TCURV'
, 'T247'
, 'T015M'
, 'TFACD'
, 'TTZZ'
, 'TTZD'
, 'THOCI'
, 'T012'
, 'T012A'
, 'T012B'
, 'T012C'
, 'T012D'
, 'T012E'
, 'T012K'
, 'T012O'
, 'BNKA'
, 'TIBAN'
, 'T056'
, 'T056G'
, 'T056R'
, 'T056S'
, 'T056Z'
, 'T5DCX'
, 'T059A'
, 'T059B'
, 'T059C'
, 'T059D'
, 'T059E'
, 'T059G'
, 'T059F'
, 'T059K'
, 'T059P'
, 'T059Z'
, 'T007A'
, 'T007S'
, 'T007B'
, 'T042'
, 'T042A'
, 'T042B'
, 'T042C'
, 'T042D'
, 'T042E'
, 'T042F'
, 'T042H'
, 'T042G'
, 'T042I'
, 'T042J'
, 'T042K'
, 'T042N'
, 'T042L'
, 'T042S'
, 'T042V'
, 'T042W'
, 'T042Z'
, 'T008'
, 'T043'
, 'T043G'
, 'T043I'
, 'T043K'
, 'T043S'
, 'T043U'
, 'T001'
, 'T000'
, 'T880'
, 'T003'
, 'T004'
, 'T077S'
, 'T009'
, 'T014'
, 'T010O'
, 'T010P'
, 'T001B'
, 'T002'
, 'T005'
, 'T006'
, 'TGSB'
]
    

appendToTableNames(initTableNames)
    
    
    
    
try:
  breaker = 0
  for tableName in tableNames:
    if breaker == 500: break

    URL = "https://www.leanx.eu/en/sap/table/" + tableName + ".html"

    print('breaker:' +str(breaker))
    #print(URL)

    page = requests.get(URL)
    

    cellContents = []

    #print(tableName)
    #print(page.text)
    cellContents.append(tableName)

    soup = BeautifulSoup(page.content, "html.parser")
    mainContent = soup.find(class_="main-container")
    #print(mainContent.prettify())

    try:
      tableLongName = mainContent.find("h1").text
      #print(tableLongName)
      cellContents.append(tableLongName)
    except AttributeError:
      cellContents.append('tableLongName not found')

    try:
      tableShortDescription = mainContent.find("h2").text
      #print(tableShortDescription)
      cellContents.append(tableShortDescription)
    except AttributeError:
      cellContents.append('tableShortDescription not found')

    cellContents.append(URL)

    allTablesTable.append(cellContents)

    contentTableHeaders = mainContent.find_all("h3")
    #print(contentTableHeaders[0].prettify())

    try:
      fieldsTable = contentTableHeaders[0].findNext("tbody")
      #print(fieldsTable.prettify())
      processFieldsTable(tableName, fieldsTable)
    except AttributeError:
      pass
    except IndexError:
      pass

    if len(contentTableHeaders) == 2:
      fkTable = contentTableHeaders[1].findNext("tbody")
      #print(fkTable.prettify())

      processFkTable(fkTable)


    breaker = breaker +1
except:
  dumpScrapedData()
  raise

dumpScrapedData()    
    
    