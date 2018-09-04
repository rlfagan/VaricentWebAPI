import os
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pyodbc
import xmltodict
import logging

with open(os.path.dirname(os.path.realpath(__file__)) + "\\config.xml") as c:
    config = xmltodict.parse(c.read())

logging.basicConfig(level=logging.ERROR,
                    format='%(asctime)s [line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename=os.path.dirname(os.path.realpath(__file__)) + '\\log.txt',
                    filemode='a')

loginURL = config['config']['Login']['LoginURL']

coptions = Options()
coptions.add_argument('--headless')

webDriverPath = os.path.dirname(os.path.realpath(__file__)) + "\\chromedriver.exe"
wd = webdriver.Chrome(executable_path=webDriverPath,options=coptions)

logging.error("Starting grab logging file: "+loginURL)
wd.get(loginURL)

try:
    loginBox = wd.find_element_by_name("username")
    passwordBox = wd.find_element_by_name("password")
    btnLogin = wd.find_element_by_xpath("/html/body/div[2]/form/div/div[5]/input")

    loginBox.send_keys(config['config']['Login']['User'])
    passwordBox.send_keys(config['config']['Login']['Password'])
    btnLogin.submit()
    logging.error("Logging sucessfully")

    downloadRequest = requests.session()

    headers = { "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 " "(KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36"} 
    downloadRequest.headers.update(headers)

    for cookie in wd.get_cookies():
        downloadRequest.cookies.set(cookie['name'],cookie['value'])
except Exception as e:
    logging.error(e)

for item in config['config']['DownloadLists']['DownloadItem']:
    r = downloadRequest.get(item['URL'])
    fileContent = r.content.decode("utf-8")
    fileLines = fileContent.split("\n")
    lineNum = len(fileLines)
    tableName = item['TableName']

    try:
        myDB = pyodbc.connect("Trusted_Connection=Yes;DRIVER={ODBC Driver 13 for SQL Server};SERVER="+config['config']['DatabaseSetting']['Server']+";DATABASE="+config['config']['DatabaseSetting']['Database']) 
        cursor = myDB.cursor()
        createTableSQL = "IF OBJECT_ID('"+tableName+"') is not null drop table "+tableName+"; create table "+tableName+" (["+fileLines[0].replace("|","] varchar(2500), [")+ "] varchar(2500));"
        logging.debug("Create Table SQL: "+createTableSQL)

        cursor.execute(createTableSQL)
        cursor.commit()
        logging.error("Table created")

        for i in range(1,lineNum):
            if "|" not in fileLines[i]:
                continue
            insertSQL = "Insert into "+tableName+"\n"
            trimedSQL = fileLines[i].replace("'","'")
            insertSQL = insertSQL+"select '"+trimedSQL.replace("|","','")+"'"
            logging.debug(insertSQL)
            cursor.execute(insertSQL)
            cursor.commit()
    except Exception as e:
        logging.error(e)

    finally:
        cursor.close()    
        myDB.close()
    logging.error("Task finished!")
