import undetected_chromedriver.v2 as uc
import time
import datetime
import pandas as pd
import time
from bs4 import BeautifulSoup, SoupStrainer
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import pandas as pd
import glob
import csv
import mysql.connector

df = pd.DataFrame(
    columns=[
        "SNO",
        "call OI",
        "call CHNG IN OI",
        "call VOLUME",
        "call IV",
        "call LTP",
        "call CHNG",
        "call BID QTY",
        "call BID PRICE",
        "call ASK PRICE",
        "call ASK QTY",
        "STRIKE PRICE",
        "put BID QTY",
        "put BID PRICE",
        "put ASK PRICE",
        "put ASK QTY",
        "put CHNG",
        "put LTP",
        "put IV",
        "put VOLUME",
        "put CHNG IN OI",
        "put OI",
    ]
)


co = Options()
co.add_argument("--log-level=3")
from selenium.webdriver import Chrome

# chrome_options = uc.ChromeOptions() # new solution

# chrome_options.add_argument('--headless')
options = Options()
# options.headless = True

# driver = uc.Chrome(version_main=95)
driver = uc.Chrome(
    executable_path=r"C:\\Users\\amitbsharma01\\Desktop\\Alog Trading\\chrome\\chromedriver.exe",
    options=options,
)


# driver.get('https://www.nseindia.com/get-quotes/derivatives?symbol=BANKNIFTY')
driver.get("https://www.nseindia.com/option-chain")
from selenium.webdriver.support.ui import Select

select = Select(driver.find_element_by_id("equity_optionchain_select"))
select.select_by_visible_text("BANKNIFTY")
time.sleep(20)
ref = driver.find_element_by_class_name("refreshIcon")
ref.click()
time.sleep(20)
l = driver.find_element_by_partial_link_text("Download (.csv)")
l.click()

driver.minimize_window()


try:
    connection = mysql.connector.connect(
        host="localhost", database="investtoo", user="root", password=""
    )
    cursor = connection.cursor()
    files = glob.glob("C:\\Users\\amitbsharma01\\Downloads\\*.csv")
    for i in files:
        print(i)
        csv_data = csv.reader(open(i))
        for row in csv_data:
            print(row[1])
            sql_select_Query = (
                'INSERT INTO `daynifty` (`CALL_OI`, `CALL_CHNG_IN_OI`, `CALL_VOLUME`, `CALL_IV`, `CALL_LTP`, `CALL_CHNG`, `CALL_BID_QTY`, `CALL_BID_PRICE`, `CALL_ASK_PRICE`, `CALL_ASK_QTY`, `STRIKE_PRICE`, `PUT_BID_QTY`, `PUT_BID_PRICE`, `PUT_ASK_PRICE`, `PUT_ASK_QTY`, `PUT_CHNG`, `PUT_LTP`, `PUT_IV`, `PUT_VOLUME`, `PUT_CHNG_IN_OI`, `PUT_OI`) VALUES ("'
                + row[0]
                + '", "'
                + row[1]
                + '", "'
                + row[2]
                + '", "'
                + row[3]
                + '", "'
                + row[4]
                + '", "'
                + row[5]
                + '", "'
                + row[6]
                + '", "'
                + row[7]
                + '", "'
                + row[8]
                + '", "'
                + row[9]
                + '", "'
                + row[10]
                + '", "'
                + row[11]
                + '", "'
                + row[12]
                + '", "'
                + row[13]
                + '", "'
                + row[14]
                + '", "'
                + row[15]
                + '", "'
                + row[16]
                + '", "'
                + row[17]
                + '", "'
                + row[18]
                + '", "'
                + row[19]
                + '", "'
                + row[20]
                + '")'
            )
            print(sql_select_Query)
            cursor.execute(sql_select_Query)
            # close the connection to the database.
            connection.commit()


except mysql.connector.Error as error:
    print("error: {}".format(error))

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")