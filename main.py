
import csv
import os
import pickle
import string
import threading
import time
from datetime import timedelta, timezone, tzinfo

import peewee
import pytz
from dotenv import dotenv_values, load_dotenv
from peewee import *
from PyQt5 import QtWidgets, uic
# from PyQt5.QtGui import QTextCursor
from PyQt5.QtWidgets import *
from selenium.webdriver.common.by import By

from cmeParser import CmeParser, PostCodes
from models import *

config = dotenv_values('.env')

#MT4 Path
appDataPath = os.getenv("APPDATA")
metaQuotesPath = f"{appDataPath}\\{config['CommonTerminalPath']}"

parser: CmeParser = CmeParser()

app = QtWidgets.QApplication([])
ui = uic.loadUi("interface.ui")

#UI Elements
messageBox: QMessageBox = QMessageBox()
messageBox.setWindowTitle("CME Parser")

enterCmeBtn: QPushButton = ui.enterCmeBtn
getInfoBtn: QPushButton = ui.getInfoBtn

startParserBtn: QPushButton = ui.startParserBtn

addSymbolItemBtn: QPushButton = ui.addSymbolItemBtn
removeSymbolItemBtn: QPushButton = ui.removeSymbolItemBtn
dbApplyBtn: QPushButton = ui.dbApplyBtn
symbolTable: QTableWidget = ui.symbolTable

refreshTimeSpin: QSpinBox = ui.refreshTimeSpin

logText: QTextEdit = ui.logText

#Events handlers


def enterCmeBtnOnClicked():
    global parser
    appendLog("Открытие браузера")
    parser.runBrowser()
    isCookies = False
    if os.path.exists(config['Cookies']):
        appendLog("Обнаружены файлы cookie.")
        isCookies = True
        parser.driver.minimize_window()

    parser.driver.get(config['CmeURL'])
    if isCookies:
        appendLog("Идет попытка авторизации.")
        parser.loadCookies()
        parser.driver.refresh()
        parser.driver.maximize_window()
    appendLog("Страница загружена. Проверьте правильность авторизации")


def getInfoBtnOnClicked():
    global parser
    if parser == None:
        return False
    if not os.path.exists(config['Cookies']):
        parser.saveCookies()
    try:
        iframe = parser.driver.find_element(
            By.CSS_SELECTOR, ".cmeIframeContainer > iframe")
        if None != iframe:
            parser.driver.switch_to.frame(iframe)
            src = parser.driver.page_source
            formActionStr = parser.getFormAction(src=src)
           
            mainFormUrl = Form_Posts.select().where(Form_Posts.Name == 'mainFormUrl')
            if mainFormUrl.count() == 0:
                Form_Posts.create(Name="mainFormUrl", Value=formActionStr)
            else:
                row: Form_Posts = mainFormUrl.first()
                row.Value = formActionStr
                row.save()
            appendLog("Данные получены. Закрытие браузера")
            parser.closeBrowser()
            appendLog("Браузер закрыт")
    except Exception as e:
        messageBox.setText(
            "Главная форма не найдена\nПерезагрузите страницу и попробуйте снова")
        messageBox.exec()
        return False
    return True


def startParserBtnOnClicked():
    global parserThread
    global parserThreadStatus
    global parserTimeout
    parserTimeout = int(refreshTimeSpin.value())

    if parserThread.is_alive():
        if parserThreadStatus == 2:
            appendLog("Запуск парсера")
            parserThreadStatus = 1
            databaseThreadStatus = 1
            if parserTimeout > 0:
                startParserBtn.setText("Остановить")
        elif parserThreadStatus == 1:
            appendLog("Остановка парсера")
            parserThreadStatus = 2
            databaseThreadStatus = 2
            startParserBtn.setText("Начать")

#Grid events buttons functions


def addSymbolItemBtnClicked():
    rowIndex = symbolTable.rowCount()
    symbolTable.insertRow(rowIndex)


def removeSymbolItemBtnClicked():
    selectedItems = symbolTable.selectedItems()
    for item in selectedItems:
        try:
            res = symbolTable.row(item)
            symbolTable.removeRow(res)
            print(res)
        except RuntimeError:
            continue


def dbApplyBtnClicked():
    curIndex = 0
    rowsCount = symbolTable.rowCount()
    currentTimestamp: int = int(getBeginDay(datetime.now()).timestamp())
    cmeCodeList: list = list()
    while curIndex < rowsCount:
        cmeCode = symbolTable.item(curIndex, 0).text()
        mtCode = symbolTable.item(curIndex, 1).text()
        # priceOffset = symbolTable.item(curIndex, 2).text()
        symbol = SymbolCodes.select().where((SymbolCodes.CME_CODE == cmeCode))
        cmeCodeList.append(cmeCode)
        if symbol.count() == 0:
            SymbolCodes.create(CME_CODE=cmeCode, MT4_CODE=mtCode, Is_Use=True)
        else:
            dbRow = symbol[0]
            dbRow.MT4_CODE = mtCode
            dbRow.Is_Use = True
            dbRow.save()
        row = SymbolCodes.get(SymbolCodes.CME_CODE == cmeCode)
        # updateSpread(row.ID, currentTimestamp, priceOffset)
        curIndex += 1
    #Помечаем не активные коды
    symCodes = SymbolCodes.select()
    for symCode in symCodes:
        if findInList(cmeCodeList, symCode.CME_CODE) < 0:
            symCode.Is_Use = False
            symCode.save()
    appendLog("Коды успешно сохранены в БД")


#Thread parser function
def parserFunc():
    global parser
    global parserThreadStatus
    global parserTimeout
    runOnce = True
    errorCount = 0
    symbols = None
    lastState = parserThreadStatus
    while parserThreadStatus:
        #TODO insert parser functions
        if parserThreadStatus == 0:
            break
        if parserThreadStatus == 2:
            runOnce = True
            if lastState != 2:
                appendLog("Парсер остановлен")
            lastState = parserThreadStatus
            time.sleep(0.1)
            continue
        if errorCount == 3:
            break
        if runOnce:
            runOnce = False
            #Получить список инструментов из БД
            appendLog("Получаем список символов из БД")
            symbols = SymbolCodes.select().where(SymbolCodes.Is_Use == True)
            appendLog("Символы в БД:")
            for symbol in symbols:
                appendLog(f"{symbol.ID} {symbol.CME_CODE}")
            #Проверить доступность формы по инструментам
            if not parser.checkPage():
                #Если ошибка авторизоваться через selenium в скрытом режиме
                appendLog("Ошибка получения формы")
                errorCount += 1
                runOnce = True
                if os.path.exists(config["Cookies"]):
                    appendLog("Пробуем авторизоваться")
                    parser.runBrowser()
                    parser.driver.minimize_window()
                    parser.driver.get(config['CmeURL'])
                    appendLog("Идет попытка авторизации.")
                    parser.loadCookies()
                    parser.driver.refresh()
                    iframe = parser.driver.find_element(
                        By.CSS_SELECTOR, ".cmeIframeContainer > iframe")
                    if None == iframe:
                        continue
                    parser.driver.switch_to.frame(iframe)
                    src = parser.driver.page_source
                    formActionStr = parser.getFormAction(src)
                    if formActionStr == None:
                        parser.closeBrowser()
                        continue
                    mainFormUrl = Form_Posts.select().where(Form_Posts.Name == 'mainFormUrl')
                    if mainFormUrl.count() == 0:
                        Form_Posts.create(Name="mainFormUrl",
                                          Value=formActionStr)
                    else:
                        row: Form_Posts = mainFormUrl.first()
                        row.Value = formActionStr
                        row.save()
                    parser.closeBrowser()
                    appendLog("Адрес формы обновлен")
                continue
            appendLog("Получаем данные по инструментам из формы")
            #Получить коды из формы инструментов
            parser.setSymbolsPostCodeIntoDb()
            parser.setSymbolPage(symbol=symbols[0].CME_CODE)
            parser.initPage()
        #end if runOnce:
        #Проверить наличие последних данных
        if symbols != None:
            #Загружаем данные за сегодня по UTC
            utc_zone = pytz.timezone('UTC')
            utcTime = datetime.now(utc_zone)
            dayBegin = getBeginDay(utcTime)
            for symbol in symbols:
                appendLog(
                    f"Получаем данные по: {symbol.CME_CODE} - {symbol.MT4_CODE}")
                options = Options.select().where((Options.Symbol_Id == symbol.ID)
                                                 & (Options.Time >= dayBegin.timestamp()))
                sourcePage = parser.setSymbolPage(symbol=symbol.CME_CODE)
                items = parser.parsePage(sourcePage)
                if None == items:
                    continue
                parser.saveDb(symbol=symbol.CME_CODE, items=items)
                appendLog(
                    f"Данные по: {symbol.CME_CODE} - {symbol.MT4_CODE} получены")
                parser.changeProduct()
            #END for symbol in symbols:
        lastState = parserThreadStatus
        if 0 == parserTimeout:
            parserThreadStatus = 2
            appendLog("Парсинг окончен")
            startParserBtn.setText("Начать")
        appendLog(f"Ждем {parserTimeout} сек.")
        time.sleep(parserTimeout)

#Thread database function


def databaseFunc():
    global databaseThreadStatus
    lastCount: list = list()

    #Init list
    symbols = SymbolCodes.select().where(SymbolCodes.Is_Use == True)
    for symbol in symbols:
        lastCount.append({'Id': symbol.ID, 'Count': 0})
    while databaseThreadStatus > 0:
        time.sleep(databaseTimeout)
        if databaseThreadStatus == 2:
            continue
        symbols = SymbolCodes.select().where(SymbolCodes.Is_Use == True)
        for symbol in symbols:
            #Проверяем есть ли изменения в БД
            num = Options.select(fn.Count(Options.Id).alias('num')).where(
                Options.Symbol_Id == symbol.ID)[0].num
            index = findDictValInList(lastCount, 'Id', symbol.ID)
            if index < 0:
                lastCount.append({'Id': symbol.ID, 'Count': num})
                SendToMT4(symbol.ID)
            else:
                if num != lastCount[index]['Count']:
                    lastCount[index]['Count'] = num
                    SendToMT4(symbol.ID)
            #Получаем данные из БД
            spreads: peewee.ModelSelect = Spread.select().where(
                Spread.Symbol_Id == symbol.ID).order_by(Spread.Time.desc()).limit(1)
            spread: Spread = None
            if spreads.count() > 0:
                spread = spreads[0]
            else:
                spread = Spread(Symbol_Id=symbol.ID, Time=getBeginDay(
                    datetime.now(pytz.timezone('UTC'))), Value=0)
            data = Options.select().where(Options.Symbol_Id == symbol.ID)


def readSettings():
    if not os.path.exists(".\\settings"):
        return 0
    with open('.\\settings', 'rb') as file:
        refreshTime: int = pickle.load(file)
        refreshTimeSpin.setValue(refreshTime)


def writeSettings():
    with open('.\\settings', 'wb') as file:
        pickle.dump(refreshTimeSpin.value(), file)


def appendLog(msg: string):
    currentTime = datetime.now().strftime("%H:%M:%S")
    logText.append(f"{currentTime}: {msg}")
    # logText.moveCursor(QTextCursor.MoveOperation.EndOfBlock)
    # cursor = logText.textCursor()
    # cursor.movePosition(QTextCursor.End)
    # logText.setTextCursor(cursor)    


def loadSymbolTable():
    while symbolTable.rowCount() > 0:
        symbolTable.removeRow(0)
    symbols = SymbolCodes().select().where(SymbolCodes.Is_Use == True)
    symbols.order_by(SymbolCodes.ID.asc)

    for symbol in symbols:
        # spreads = Spread.select().where((Spread.Symbol_Id == symbol.ID)
        #                                 ).order_by(Spread.Time.desc()).limit(1)
        # spread = 0
        # if spreads.count() > 0:
        #     spread = spreads[0]

        rowIndex = symbolTable.rowCount()
        symbolTable.insertRow(rowIndex)
        symbolTable.setItem(rowIndex, 0, QTableWidgetItem(symbol.CME_CODE))
        symbolTable.setItem(rowIndex, 1, QTableWidgetItem(symbol.MT4_CODE))
        # if None != spread:
        #     symbolTable.setItem(
        #         rowIndex, 2, QTableWidgetItem(str(spread.Value)))


def SendToMT4(SymbolId: int):
    if not os.path.exists(metaQuotesPath):
        return 0
    symbol: SymbolCodes = SymbolCodes.get_or_none(SymbolCodes.ID == SymbolId)
    if symbol == None:
        return 0
    #Проверяем наличие файла
    symbolFilePath = f"{metaQuotesPath}\\{symbol.MT4_CODE}"
    dataList: list = list()
    tzone = pytz.timezone('UTC')
    lastTime: int = getBeginDay(datetime.now(tzone)).timestamp()
    if os.path.exists(symbolFilePath):
        #Читаем файл
        with open(symbolFilePath, 'r', encoding='utf8') as file:
            csvReader = csv.reader(file, delimiter=',', lineterminator="\n")
            for row in csvReader:
                dataList.append(row)
    #Получить последнюю запись из файла
    if len(dataList) > 0:
        lastTime = int(dataList[-1][0])
    #Если прошлый день, то удаляем файл
    lt = getBeginDay(datetime.fromtimestamp(lastTime))
    nt = getBeginDay(datetime.now(tzone))
    if lt.timestamp() < nt.timestamp():
        dataList.clear()
        os.remove(symbolFilePath)
    #Получить данные по символу из БД
    if lastTime == getBeginDay(datetime.fromtimestamp(lastTime)).timestamp():
        lastTime -= 1
    dbData = Options.select().where((Options.Symbol_Id == SymbolId) & (
        Options.Time > lastTime)).order_by(Options.Time.asc())
    if dbData.count() == 0:
        return 0
    #Вычислить цену фьючерса для записей
    # spreadData = Spread.select().where(
    #     Spread.Symbol_Id == SymbolId).order_by(Spread.Time.desc()).limit(1)
    # if spreadData.count() == 0:
    #     return 0
    # spread = spreadData[0].Value
    dataList = list()
    dataList.clear()
    for row in dbData:
        futurePrice = row.Trade+(row.Price*row.Type)
        dataList.append([row.Time, futurePrice, row.Type,row.Symbol])
    #Если есть данные - дописать в конец файла
    if len(dataList) > 0:
        with open(symbolFilePath, 'a', encoding='utf8') as file:
            csvWriter = csv.writer(file, delimiter=',', lineterminator='\n')
            csvWriter.writerows(dataList)

#Other functions


def getBeginDay(time: datetime):
    return time.combine(time.date(), time.min.time())


def getDayShift(time: datetime, shift: int):
    return time + timedelta(days=shift)


def updateSpread(symbol_id: int, timestamp: int, val: int):
    spreads = Spread.select().where((Spread.Symbol_Id == symbol_id)
                                    & (Spread.Time == timestamp))
    if spreads.count() == 0:
        Spread.create(Symbol_Id=symbol_id, Time=timestamp, Value=val)
    else:
        spread = spreads[0]
        spread.Value = val
        spread.save()


def findInList(items: list, val):
    index = 0
    while index < len(items):
        if(items[index] == val):
            return index
        index += 1
    return -1


def findDictValInList(items: list, key, val):
    index = 0
    while index < len(items):
        item = items[index]
        if item[key] == val:
            return index
        index += 1
    return -1


def main():
    #start threads
    global parserThread
    global parserThreadStatus
    parserThreadStatus = 2
    parserThread = threading.Thread(target=parserFunc)
    parserThread.start()

    global databaseThread
    global databaseThreadStatus
    databaseThreadStatus = 1
    databaseThread = threading.Thread(target=databaseFunc)
    databaseThread.start()
    #start GUI
    curCol = 0
    while symbolTable.columnCount() > curCol:
        symbolTable.horizontalHeader().setSectionResizeMode(
            curCol, QHeaderView.ResizeToContents)
        curCol += 1
    loadSymbolTable()
    readSettings()
    ui.show()
    app.exec()
    writeSettings()
    parserThreadStatus = 0
    databaseThreadStatus = 0


#Thread variables
parserThread = None
parserThreadStatus: int = 0  # 0 - stop, 1 - activate/resum, 2 - pause
parserTimeout: int = 0

databaseThread = None
databaseThreadStatus: int = 0  # 0 - stop, 1 - activate/resum, 2 - pause
databaseTimeout: float = 1.0

#Signals
enterCmeBtn.clicked.connect(enterCmeBtnOnClicked)
getInfoBtn.clicked.connect(getInfoBtnOnClicked)

startParserBtn.clicked.connect(startParserBtnOnClicked)

#Grid signals
addSymbolItemBtn.clicked.connect(addSymbolItemBtnClicked)
removeSymbolItemBtn.clicked.connect(removeSymbolItemBtnClicked)
dbApplyBtn.clicked.connect(dbApplyBtnClicked)

if __name__ == '__main__':
    main()
else:
    print(f"\"{__name__}\" is not a module")
