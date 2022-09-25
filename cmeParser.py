from lib2to3.pygram import Symbols
from msilib.schema import Property
import os
import pickle
import random
from re import L
import string
from datetime import datetime
from time import sleep

import peewee
from peewee import *
import pytz
import requests
import selenium
from bs4 import BeautifulSoup as Soup
from dotenv import dotenv_values, load_dotenv
from genericpath import exists
from requests import request, session
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from models import *

config = None

class CmeParser:
    driver = None
    driverOptions = None
    __commonTerminalPath:string = ''
    config = dict()
    __postCodes = None
    __session = None
    #Properties
    def get_EventTarget(self):
        return "__EVENTTARGET"
    eventTarget = property(fget=get_EventTarget,doc="EventTarget value")

    def get_FxMajors(self):
        return "FX Majors"
    FxMajors = property(fget=get_FxMajors,doc="Finding FX Majors section")

    def get_FirstColumnName(self):
        return "Time (CT)"
    FirstColumnName = property(fget=get_FirstColumnName)

    def func_delay(func):
        def wrapper(*args, **kargs):
            return_value = func(*args, **kargs)
            sleep(random.randint(3,5))
            return return_value
        return wrapper

    def __init__(self):
        self.__postCodes = PostCodes()
        self.__session = requests.Session()
        self.config = dotenv_values('.env')
        self.__commonTerminalPath = f"{os.getenv('APPDATA')}{self.config['CommonTerminalPath']}"

    
    def __del__(self):
        try:
            self.__session.close()
        except Exception as e:
            print(f"Excetion message: {e}")

        try:
            self.driver.close()
            self.driver.quit()
        except Exception as e:
            print(f"Exception message: {e}")
   
    def runBrowser(self):
        #Setup browser
        driverPath:string = os.curdir
        if self.config['WebBrowserType'] == 'edge':
                driverPath += f"\\{self.config['EdgeDriver']}"
                self.driverOptions = webdriver.EdgeOptions()
                self.driver = webdriver.Edge(executable_path=driverPath)
        if self.config['WebBrowserType'] == 'chrome':
                self.driverOptions = webdriver.ChromeOptions()
                driverPath +=f"\\{self.config['ChromeDriver']}"
                self.driver = webdriver.Chrome(executable_path=driverPath)

    def closeBrowser(self):
        try:
            self.driver.close()
            self.driver.quit()
        except Exception as e:
            print(e)

    def saveCookies(self):
        pickle.dump(self.driver.get_cookies(),open(self.config['Cookies'],"wb"))

    def loadCookies(self):
        if os.path.exists(self.config['Cookies']):
            for cookie in pickle.load(open(self.config['Cookies'],"rb")):
                self.driver.add_cookie(cookie)
        else:
            return False
        return True
   
    def getCookies(self):
        if os.path.exists(self.config['Cookies']):
            return pickle.load(open(self.config['Cookies'],"rb"))
        else:
            return None
    
    @func_delay
    def getPage(self):
        return self.__session.get(self.__postCodes.mainFormUrl)
    
    @func_delay
    def postPage(self, paramValue:string):
        return self.__session.post(self.__postCodes.mainFormUrl,data={self.eventTarget:paramValue})

    def setSymbolsPostCodeIntoDb(self):
        res = self.postPage(self.__postCodes.ChangeProductBtn)
        src = res.text
        soup = Soup(src,"lxml")
        res = soup.find_all("div",class_="tile-header")
        for item in res:
            val = item.text.strip()
            if self.FxMajors==val:
                symResList = item.parent.find_all("a")
                for symRes in symResList:
                    name = symRes.text.strip()[0:3].strip()
                    code=None
                    try:
                        code = SymbolCodes.get(SymbolCodes.CME_CODE==name)
                    except Exception as e:
                        continue
                    href = self.__parseJShref(symRes.attrs['href'])
                    code.Post_Product=href
                    code.save() 
                return

    @func_delay
    def setSymbolPage(self,symbol:string):
        src:string = ''
        symbolPostCode:string = ''
        try:
            symbolPostCode = SymbolCodes.get(SymbolCodes.CME_CODE==symbol)
        except Exception as e:
            print(e)
            return
        self.postPage(self.__postCodes.FxTab)
        self.postPage(symbolPostCode.Post_Product)
        src = self.setAllOption().text  
        with open(f'{symbol}.html','w') as file:
            file.write(src)
        return src

    def parsePage(self,source:string):
        soup = Soup(source,"lxml")
        res = soup.find_all("div",class_="hover-container")
        try:
            theadEl = res[0].find("tr",class_="compact").text.strip().split("\n")
        except Exception as e:
            print(e)
            return None
        items = list()
        if len(theadEl)>0:
            if self.FirstColumnName!=theadEl[0]:
                src = self.initPage()
                soup = Soup(source,"lxml")
                res = soup.find_all("div",class_="hover-container")            
            trs = res[0].find_all("tr")
            for tr in trs:
                if (len(tr.attrs)==0):
                    continue
                if (tr.attrs['class']=='compact'):
                    continue
                tds = tr.find_all("td")
                if(len(tds)>0):
                    timeFormat = "%a %m/%d/%Y %I:%M:%S %p"
                    time = datetime.strptime(tds[0].attrs['title'],timeFormat)
                    timezone = pytz.timezone('US/Central')
                    utcTime = timezone.localize(time).astimezone(pytz.UTC)
                    symbol = tds[2].text.strip()
                    size = tds[3].text.strip()
                    tradeInfo = tds[4].text.strip()
                    try:
                        tradePrice = tradeInfo.split(' ')[1]
                    except Exception as e:
                        print(f"Parse Error: {tradeInfo}")
                        continue
                    tradeType = 0
                    if tradeInfo.split(' ')[2] == 'C':
                            tradeType = 1
                    if tradeInfo.split(' ')[2] == 'P':
                            tradeType = -1
                    
                    price = tds[5].text.strip()
                    item = {'Time':utcTime,
                            'Symbol':symbol,
                            'Size':size,
                            'Trade':tradePrice,
                            'Type':tradeType,
                            'Price':price}
                    items.append(item)
                #end if(len(tds)>0):
            #end if self.FirstColumnName!=theadEl[0]:
        #end if len(theadEl)>0:
        return items
    
    @func_delay
    def initPage(self):
        self.postPage(self.__postCodes.AllListSelector)
        self.postPage(self.__postCodes.TimeSelector)
        src = self.postPage(self.__postCodes.OptionOnlySelector).text
        return src

    @func_delay
    def setMinimumPage(self):
        return self.postPage(self.__postCodes.Last10Selector)
    
    @func_delay
    def setMaximumPage(self):
        return self.postPage(self.__postCodes.AllListSelector)

    def __parseJShref(self,href:string):
        return href.strip()[25:-5].strip()

    def getFormAction(self,src:string):
        soup = Soup(src,"lxml")
        form1 = soup.find("form",id="Form1")
        if form1 != None:
            formAction = f"{self.config['CmeFormURL']}{form1.attrs['action'][1:]}"
            return(formAction)
    
    @func_delay
    def checkPage(self):
        res = self.getPage()
        soup = Soup(res.text,"lxml")
        form1 = soup.find("form",id="Form1")
        if form1==None:
            return False
        formAction = form1.attrs.get('action')
        if formAction==None:
            return False
        if formAction.find("ErrorPage")>-1:
            return False
        return True
    
    def parseAll(self,symbol):
        self.postPage(self.__postCodes.ChangeProductBtn)
        self.setSymbolPage(symbol)
        i = 5
        while i>=0:
            pageName = f"day{i}"
            dayPost = Form_Posts.get(Form_Posts.Name==pageName).Value
            res = self.postPage(dayPost)
            items = self.parsePage(res.text)
            self.saveDb(symbol,items)
            i-=1
            sleep(random.randint(3,5))

    def saveDb(self,symbol:string,items:list):
        symbolId = SymbolCodes.get(SymbolCodes.CME_CODE==symbol).ID
        for item in items:
            res = Options.select().where((Options.Time==item['Time'].timestamp())&(Options.Symbol_Id==symbolId))
            if res.count()>0:
                continue
            Options.create(
                Symbol_Id=symbolId,
                Time=item['Time'].timestamp(),
                Symbol=item['Symbol'],
                Size=item['Size'],
                Trade=item['Trade'],
                Type=item['Type'],
                Price=item['Price'])
            
    @func_delay
    def changeProduct(self):
        eventTarget = '__EVENTTARGET'
        paramName = 'ctl00$MainContent$ucViewControl_IntegratedGlobexTrades$ucViewControl$ucTradesView$ucToolbar$btnNewProduct'
        paramValue = 'Change Product'
        return self.__session.post(self.__postCodes.mainFormUrl,data={paramName:paramValue, eventTarget:''})
    @func_delay
    def setAllOption(self):
        eventTarget = '__EVENTTARGET'
        paramName = 'ctl00$MainContent$ucViewControl_IntegratedGlobexTrades$ucViewControl$ucTradesView$ucToolbar$ucProductSymbolPicker$lvProductCodes$ctrl$chkSymbol'
        paramValue = 'on'
        params = {
            eventTarget: '',
            'ctl00$smPublic':'ctl00$upMain|ctl00$MainContent$ucViewControl_IntegratedGlobexTrades$ucViewControl$ucTradesView$ucToolbar$ucProductSymbolPicker$btnApplySettings'
            }
        for i in range(16):
            param = f"{paramName[:-10]}{i}{paramName[-10:]}"
            params.update({param: paramValue})
        params.update({'ctl00$MainContent$ucViewControl_IntegratedGlobexTrades$ucViewControl$ucTradesView$ucToolbar$ucProductSymbolPicker$ucTrigger$hfPopupTrigger':'open'})
        params.update({'ctl00$MainContent$ucViewControl_IntegratedGlobexTrades$ucViewControl$ucTradesView$ucToolbar$ucProductSymbolPicker$btnApplySettings':'Apply'})
        return self.__session.post(self.__postCodes.mainFormUrl,data=params)
    
class PostCodes:

    def __getSelect(self):
        return Form_Posts.select()

    def get_mainFormUrl(self):
        return self.__getSelect().where(Form_Posts.Name=="mainFormUrl").first().Value
    mainFormUrl = property(fget=get_mainFormUrl)

    def get_FxTab(self):
        return self.__getSelect().where(Form_Posts.Name=="FxTab").first().Value
    FxTab = property(fget=get_FxTab)
    
    def get_RefreshBtn(self):
        return self.__getSelect().where(Form_Posts.Name=="RefreshBtn").first().Value
    RefreshBtn = property(fget=get_RefreshBtn)

    def get_ChangeProductBtn(self):
        return self.__getSelect().where(Form_Posts.Name=="ChangeProductBtn").first().Value
    ChangeProductBtn = property(fget=get_ChangeProductBtn)

    def get_TimeSelector(self):
        return self.__getSelect().where(Form_Posts.Name=="TimeSelector").first().Value
    TimeSelector = property(fget=get_TimeSelector)

    def get_OptionOnlySelector(self):
        return self.__getSelect().where(Form_Posts.Name=="OptionOnlySelector").first().Value
    OptionOnlySelector = property(fget=get_OptionOnlySelector)

    def get_AllListSelector(self):
        return self.__getSelect().where(Form_Posts.Name=="AllListSelector").first().Value
    AllListSelector = property(fget=get_AllListSelector)

    def get_Last10Selector(self):
        return self.__getSelect().where(Form_Posts.Name=="Last10Selector").first().Value
    Last10Selector = property(fget=get_Last10Selector)

    def get_day0(self):
        return self.__getSelect().where(Form_Posts.Name=="day0").first().Value
    day0 = property(fget=get_day0)

    def get_day1(self):
        return self.__getSelect().where(Form_Posts.Name=="day1").first().Value
    day1 = property(fget=get_day1)

    def get_day2(self):
        return self.__getSelect().where(Form_Posts.Name=="day2").first().Value
    day2 = property(fget=get_day2)

    def get_day3(self):
        return self.__getSelect().where(Form_Posts.Name=="day3").first().Value
    day3 = property(fget=get_day3)

    def get_day4(self):
        return self.__getSelect().where(Form_Posts.Name=="day4").first().Value
    day4 = property(fget=get_day4)
        
    def get_day5(self):
        return self.__getSelect().where(Form_Posts.Name=="day5").first().Value
    day5 = property(fget=get_day5)

def benchmark(func):
    import time
    def wrapper():
        start = time.time()*1000
        return_val = func()
        end = time.time()*1000
        print('[*] Время выполнения: {} мс.'.format(end-start))
        return return_val
    return wrapper 

@benchmark
def count1():
    return Options.select().where(Options.Symbol_Id==9).count()

@benchmark
def count2():
    return Options.select(fn.Count(Options.Id).alias('num')).where(Options.Symbol_Id==9)

def main():
    print(count1())
    res = count2()
    print(res.sql())
    print(res[0].num)


if __name__ == '__main__':
    print("This is a module!")
    config = dotenv_values('.env')
    main()
    




