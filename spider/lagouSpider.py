#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#爬取拉钩网全国站，指定关键词的信息
import logging,logging.config,os,re
#from urllib import request
import requests
from datetime import datetime,timedelta
import random
import json
import time
import sys

##########################################
#设置日志格式

def setLog():
    logging.config.fileConfig('logging.conf')
    logger = logging.getLogger('cycleRent')
    return logger
logger = setLog()

###########################################

#get random
def int_random():
    return random.randint(0, 9)

#UA list
UA_list = [
            "Mozilla/5.0 ( ; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)",
           "Opera/9.80 (Windows NT 6.1; U; en-US) Presto/2.7.62 Version/11.01",
           "Mozilla/5.0 (Windows NT 6.1; rv:1.9) Gecko/20100101 Firefox/4.0",
           "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/11.0.696.3 Safari/534.24",
           "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/11.0.696.0 Safari/534.24",
           "Mozilla/5.0 (X11; U; Linux i686; en-US) AppleWebKit/534.13 (KHTML, like Gecko) Chrome/9.0.597.44 Safari/534.13",
           "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.13 (KHTML, like Gecko) Chrome/9.0.597.19 Safari/534.13",
           "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:55.0) Gecko/20100101 Firefox/55.0",
           "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_3; zh-cn) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.16",
           "Mozilla/5.0(iPad; U; CPU iPhone OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B314 Safari/531.21.10gin_lib.cc"
            ]

#获取网页json
def get_job_json(url,pn,UA,search):
    search = search.encode('utf-8')
    headers = {
        "Accept":"application/json, text/javascript, */*; q=0.01",
        "User-Agent":UA,
        "X-Requested-With":"XMLHttpRequest",
        "Referer":"https://www.lagou.com/jobs/list_%s?px=default" % search,
        "Host":"www.lagou.com"
    }

    data = {'first': 'false', 'pn': pn, 'kd': search}
    respone = requests.post(url,headers=headers,data=data).text
    print(respone)
    respone = respone.encode('utf-8')
    return respone

#传入查询关键词
def search(keyword,filename,isFist,UA):
    #需要查询的url
    request_url = "https://www.lagou.com/jobs/positionAjax.json?needAddtionalResult=false"
    #添加关键词查询
    searchinfo = keyword
    logger.info("第一次查询的URL为："+request_url)
    #查询到的页数，通过设置pn获取不同的页数
    pageNO = 1
    pn = 0
    while pageNO != 0:
        pn += 1
        #json文件格式化
        json_data = json.loads(get_job_json(request_url, pn, UA_list[int_random()],searchinfo).decode("utf-8"))

        print(json_data)
        #查询当前的时间
        dateStr = datetime.now().strftime('%Y%m%d')

        if(json_data['success'] == True ):
            if (json_data['content']['pageNo'] == 0):
                break
            #保存文件
            saveAllData(json_data['content']['positionResult']['result'], filename, isFist)
            savefile(json_data['content']['positionResult']['result'], filename, isFist)
            time.sleep(5)
        else:
            time.sleep(random.randint(0, 60))
        time.sleep(5)

#筛选处符合的数据存储
def savefile(dataJson, filename, isFist,):
    filename = filename + '.txt'
    with open(filename, 'a' ,encoding='utf-8') as rData:
        for data in dataJson:
            #如果不是第一次爬取，并且该数据的时间不是爬取日期时间，则不存入文件中
            if not isFist and not isYestoday(getForMatTime(str(data['createTime']), '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'), '%Y-%m-%d'):
                continue
            rStr = ''.join([
                            str(data['companyId']), ' ' +
                            str(data['companyShortName']), ' '+
                            str(data['createTime']), ' '+
                            str(data['positionId']), ' ' +
                            str(data['positionAdvantage']),' '+
                            str(data['salary']), ' '+
                            str(data['workYear']), ' '+
                            str(data['education']), ' ' +
                            str(data['city']), ' '+
                            str(data['positionName']),' '+
                            str(data['createTime']), ' '+
                            str(data['financeStage']),' '+
                            str(data['industryField']), ' ' +
                            str(data['jobNature']), ' ' +
                            str(data['companySize']), ' ' +
                            str(data['companyLabelList']), ' ' +
                            str(data['companyFullName']), ' ' +
                            str(data['firstType']), ' ' +
                            str(data['secondType']), ' '+
                            str(data['isSchoolJob'])])
            rData.write(rStr)
            rData.write("\n")


#保存数据到指定文件中
def saveAllData(dataJson, filename, isFist):

    #增加该文件为Hbaase标志
    filename = filename + '_hb' + '.txt'
    print(dataJson)
    logger.info("Hbase所存储的文件名为："+str(filename))
    with open(str(filename), 'a', encoding='utf-8') as rData:
        for data in dataJson:
            #如果不是第一次爬取，并且该数据的时间不是爬取日期时间，则不存入文件中
            if not isFist and not isYestoday(getForMatTime(str(data['createTime']), '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'), '%Y-%m-%d'):
                continue
            #转化为标准json字符串存入文件中
            rData.write(json.dumps(data, ensure_ascii=False))
            rData.write("\n")

#判断所传入的时间字符串是不是当天日期
#dateTimeStr 时间字符串,dateTimeFormat 时间字符串格式
def isNowDate(dateTimeStr,dateTimeFormat):
    nowDateStr = datetime.now().strftime(dateTimeFormat)
    if nowDateStr == dateTimeStr:
        return True
    else:
        return False

#判断传入的日期是否是昨天的日期
def isYestoday(dateTimeStr,dateTimeFormat):
    nowDay = datetime.now()
    oneDay = timedelta(days=1)
    yestoday = nowDay - oneDay
    yesDateStr = yestoday.strftime("%Y-%m-%d")
    if dateTimeStr == yesDateStr:
        return True
    return False

#按照指定格式返回字符串
#dateTimeStr 时间字符串,dateTimeFormat 时间字符串格式,toDateTimeFormat 转化的时间格式
def getForMatTime(dateTimeStr,dateTimeFormat,toDateTimeFormat):
    dt = datetime.strptime(dateTimeStr,dateTimeFormat)
    return dt.strftime(toDateTimeFormat)

#获得从当前时间到明日的具体秒数
def getSleepSeconds():
    dateNow = datetime.now()
    dataNowStr = dateNow.strftime('%Y%m%d')
    #获得明日日期
    dataN = dateNow + timedelta(days=1)
    dataNStr = dataN.strftime('%Y%m%d')
    dataNTime = datetime.strptime(dataNStr+'000000', '%Y%m%d%H%M%S')
    #计算差异时间
    dataC = dataNTime - dateNow
    return dataC.seconds



def exeCmd(cmd):
    resultStr = os.popen(cmd).read()
    return resultStr

#创建指定文件夹公用方法
def createDirUtil(dirStr):
    if not os.path.exists(dirStr):
        logger.info("创建了文件夹："+ dirStr)
        os.mkdir(dirStr)

#格式化字符串，避免数据中出现 {'positionName': '数据分析工程师助理','positionAdvantage': "技术氛围好，大牛领队，rmbp+27'苹果显示器", 'plus': '否'}
#后无法转换为json对象的错误
#data   需要转换的字符串
def formatStr(data):
    myRe = re.compile(r'"(.*)"')
    strs = myRe.findall(data)
    for str in strs:
        data = data.replace(str, str.replace("'", " "))
    return data

if __name__ == '__main__':
    try:
        #保存是否是第一次的标志
        isFist = True
        #接收输入的参数值，多个参数以 ; 分割
        #inputKeyword = str(input("请输入需要爬取的关键字："))
        #keyword = inputKeyword.split(";")
        #需要查询的关键词
        keyword = [ '数据挖掘',u'Hadoop',u'Spark', u'数据分析']

        #创建关键词对应文件字典
        filePaths = {u'Hadoop':u'Hadoop',u'数据挖掘':u'DataMining',u'数据分析':u'DataAnalySis', u'Spark': u'Spark'}
        dirStr = "/home/sanduo/PycharmProjects/lagouSpiderInit/data"
        createDirUtil(dirStr)

        #查询的当前日期
        dateStr = datetime.now().strftime('%Y%m%d')
        #拼装当前查询时间存储文件夹
        dirStr = dirStr + '/' + dateStr
        createDirUtil(dirStr)

        while True:
            logger.info("程序启动：")
            logger.info("关键词内容抽取启动：")
            #searchInfo(dirStr,isFist,UA_list[int_random()])
            for k in keyword:
                #print(filePaths[k])
                search(k, dirStr + '/' + filePaths[k],isFist,UA_list[int_random()])
            
            #将是否是第一次的标志置为 False
            isFist = False
            logger.info("关键词内容抽取结束：")
            logger.info("程序跑完了~~~")
            seconds = getSleepSeconds()
            logger.info("开始休眠，休眠时间为："+str(seconds))
            time.sleep(seconds)
            logger.info("休眠结束~~~~")
    except BaseException as b:
        logger.error("程序出错，错误原因："+ str(b))

