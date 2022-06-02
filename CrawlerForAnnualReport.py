# -*- coding = utf-8 -*-
import json
import os
from time import sleep
from urllib import parse

import requests
import pandas as pd
import os
import xlrd
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import *
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage, PDFTextExtractionNotAllowed
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
import xlwt
import os
import chardet

import threading
import queue
import pipreqs

class CrawlerForAnnualReport():

    def parsePDF(self, pdf_path, txt_path):
        """解析pdf为txt
        pdf_path:待解析的pdf路径
        txt_path:解析后txt存放路径
        """
        # 以二进制读模式打开pdf文档
        fp = open(pdf_path, 'rb')

        # 用文件对象来创建一个pdf文档分析器
        parser = PDFParser(fp)

        # pdf文档的对象，与分析器连接起来
        doc = PDFDocument(parser=parser)
        parser.set_document(doc=doc)

        # 如果是加密pdf，则输入密码，新版好像没有这个属性
        # doc._initialize_password()

        # 创建pdf资源管理器 来管理共享资源
        resource = PDFResourceManager()

        # 参数分析器
        laparam = LAParams()

        # 创建一个聚合器
        device = PDFPageAggregator(resource, laparams=laparam)

        # 创建pdf页面解释器
        interpreter = PDFPageInterpreter(resource, device)

        # 用来计数页面，图片，曲线，figure，水平文本框等对象的数量
        num_page, num_image, num_curve, num_figure, num_TextBoxHorizontal = 0, 0, 0, 0, 0

        # 获取页面的集合
        for page in PDFPage.get_pages(fp):
            num_page += 1  # 页面增一
            # 使用页面解释器来读取
            interpreter.process_page(page)

            # 使用聚合器来获取内容
            layout = device.get_result()
            # 这里layout是一个LTPage对象 里面存放着 这个page解析出的各种对象 一般包括LTTextBox, LTFigure, LTImage, LTTextBoxHorizontal 等等 想要获取文本就获得对象的text属性，
            for x in layout:
                if isinstance(x, LTImage):  # 图片对象
                    num_image += 1
                if isinstance(x, LTCurve):  # 曲线对象
                    num_curve += 1
                if isinstance(x, LTFigure):  # figure对象
                    num_figure += 1
                if isinstance(x, LTTextBoxHorizontal):  # 获取文本内容
                    num_TextBoxHorizontal += 1  # 水平文本框对象增一
                    # 保存文本内容
                    with open(txt_path, 'a', encoding=self.encoding, errors='ignore') as f:
                        results = x.get_text()
                        # print(results, end='')
                        f.write(results + '\n')
            print("正在解析第%s页" % num_page)
            # print('对象数量：\n', '页面数：%s\n' % num_page, '图片数：%s\n' % num_image, '曲线数：%s\n' % num_curve,
            #       '水平文本框：%s\n' % num_TextBoxHorizontal)

    def pdf2txt(self, ):
        "parsePDF的封装"
        files = os.listdir(self.folder)
        for file in files:
            if os.path.splitext(file)[-1] == ".pdf":
                self.pdf_name.append(file)
        for file_name in self.pdf_name:
            pdf_path = self.folder + "/" + file_name
            txt_path = self.folder + "/" + file_name[:-4] + '.txt'
            self.parsePDF(pdf_path, txt_path)

    def get_adress(self, bank_name):
        """通过模糊关键词，获取文档在网站的位置
        bank_name:传入的模糊关键词
        """
        url = "http://www.cninfo.com.cn/new/information/topSearch/detailOfQuery"
        data = {
            'keyWord': bank_name,
            'maxSecNum': 10,
            'maxListNum': 5,
        }
        hd = {
            'Host': 'www.cninfo.com.cn',
            'Origin': 'http://www.cninfo.com.cn',
            'Pragma': 'no-cache',
            'Accept-Encoding': 'gzip,deflate',
            'Connection': 'keep-alive',
            'Content-Length': '70',
            'User-Agent': 'Mozilla/5.0(Windows NT 10.0;Win64;x64) AppleWebKit / 537.36(KHTML, likeGecko) Chrome / 75.0.3770.100Safari / 537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': 'application/json,text/plain,*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        }
        r = requests.post(url, headers=hd, data=data)
        # print(r.text)
        r = r.content
        m = str(r, encoding="utf-8")
        pk = json.loads(m)
        orgId = pk["keyBoardList"][0]["orgId"]  # 获取参数
        plate = pk["keyBoardList"][0]["plate"]
        code = pk["keyBoardList"][0]["code"]
        zwjc = pk["keyBoardList"][0]["zwjc"]
        # print(orgId, plate, code)
        return orgId, plate, code, zwjc

    def download_PDF(self, url, file_name):
        """下载pdf，配合get_PDF使用"""
        url = url
        # 遇到*号替换掉
        if "*" in file_name:
            file_name = file_name.replace("*",'')
        r = requests.get(url)
        #创建文件（文件命名不能有*号）
        f = open(self.folder + "/" + file_name + ".pdf", "wb")
        f.write(r.content)


    def get_PDF(self, orgId, plate, code, zwjc):
        """获取pdf文件，传入的四个参数由get_address获取"""
        url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
        data = {
            'stock': '{},{}'.format(code, orgId),
            'tabName': 'fulltext',
            'pageSize': 30,
            'pageNum': 1,
            'column': plate,
            'category': self.category,
            'plate': '',
            'seDate': self.seDate,  # 设置时间
            'searchkey': self.searchkey,
            'secid': '',
            'sortName': '',
            'sortType': '',
            'isHLtitle': 'true',
        }

        hd = {
            'Host': 'www.cninfo.com.cn',
            'Origin': 'http://www.cninfo.com.cn',
            'Pragma': 'no-cache',
            'Accept-Encoding': 'gzip,deflate',
            'Connection': 'keep-alive',
            # 'Content-Length': '216',
            'User-Agent': 'User-Agent:Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': 'application/json,text/plain,*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'X-Requested-With': 'XMLHttpRequest',
            # 'Cookie': cookies
        }
        data = parse.urlencode(data)
        # print(data)
        r = requests.post(url, headers=hd, data=data)
        # print(r.text)
        r = str(r.content, encoding="utf-8")
        r = json.loads(r)

        reports_list = r['announcements']
        try:
            for report in reports_list:
                # print(report)
                # if '摘要' in report['announcementTitle'] or "20" not in report['announcementTitle']:
                #     continue
                if '摘要' in report['announcementTitle']:
                    continue
                if '正文' in report['announcementTitle']:
                    continue
                if 'H' in report['announcementTitle']:
                    continue
                else:  # http://static.cninfo.com.cn/finalpage/2019-03-29/1205958883.PDF
                    pdf_url = "http://static.cninfo.com.cn/" + report['adjunctUrl']
                    # 社会责任报告，中间会有特殊字符，替换掉2017<em>社会</em><em>责任</em>报告
                    my_str = report['announcementTitle'].replace('<em>', '').replace('</em>', '')

                    file_name = "{}-{}-{}".format(my_str, code, zwjc)
                    print("正在下载：" + pdf_url, "存放在当前目录：/" + self.folder + "/" + file_name)
                    self.download_PDF(pdf_url, file_name)  # 主要功能，下载pdf
                    self.succ_list.append(code)
                    sleep(2)
        except:
            self.error_list.append([code,zwjc])

#以下两个函数2选1
    def matchKeyWords(self, txt_folder, keyWords):
        """单行匹配关键词，保存到excel"""
        files = os.listdir(txt_folder)
        words_num = []  # 保存所有文件词频
        for file in files:
            word_freq = {}  # 单词出现频率次：word：num
            if os.path.splitext(file)[-1] == ".txt":
                txt_path = os.path.join(txt_folder, file)
                with open(txt_path, "r", encoding=self.encoding, errors='ignore') as fp:
                    text = fp.readlines()
                    for word in keyWords:
                        num = 0
                        for line in text:
                            num += line.count(word)
                        word_freq[word] = num
                    stock_code = file.split("-")[1]
                    stock_name = file.split("-")[2]
                    year = file.split("-")[0][0:4]
                    words_num.append((word_freq, stock_code, stock_name, year))

        book = xlwt.Workbook(encoding='utf-8', style_compression=0)

        sheet = book.add_sheet('年报关键词词频统计', cell_overwrite_ok=True)
        sheet.write(0, 0, '年份')
        sheet.write(0, 1, '企业代码')

        for i in range(0, len(keyWords)):
            sheet.write(0, i + 2, keyWords[i])

        for index, one in enumerate(words_num):
            word_f = one[0]
            stock_code = one[1]
            stock_name = one[2]
            year = one[3]
            for ind, word in enumerate(keyWords):
                sheet.write(index + 1, ind + 2, word_f[word])
            sheet.write(index + 1, 0, year)
            sheet.write(index + 1, 1, stock_code)
            # sheet.write(index + 1, 1, stock_name)

        book.save(self.folder + '\年报关键词词频统计.xls')

    def matchKeyWords2(self, txt_folder, keyWords):
        """两行匹配关键词，保存到excel"""
        files = os.listdir(txt_folder)
        words_num = []  # 保存所有文件词频
        for file in files:
            word_freq = {}  # 单词出现频率次：word：num
            if os.path.splitext(file)[-1] == ".txt":
                txt_path = os.path.join(txt_folder, file)
                with open(txt_path, "r", encoding=self.encoding, errors='ignore') as fp:
                    text = fp.readlines()
                    alltext = ''
                    for line in text:
                        alltext += line.replace("\n", "")
                    for word in keyWords:
                        num = 0
                        num += alltext.count(word)
                        word_freq[word] = num
                    #start
                    #关注点1-词频统计列表溢出，大概率是txt文本命名方式不是 2019年年度报告-000001-平安银行 的格式，需要修改下面几行
                    stock_code = file.split("-")[1]
                    stock_name = file.split("-")[2]
                    year = file.split("-")[0][0:4]
                    #end
                    words_num.append((word_freq, stock_code, stock_name, year))

        book = xlwt.Workbook(encoding='utf-8', style_compression=0)

        sheet = book.add_sheet('年报关键词词频统计', cell_overwrite_ok=True)
        sheet.write(0, 0, '年份')
        sheet.write(0, 1, '企业代码')
        # 自定义表格
        for i in range(0, len(keyWords)):
            sheet.write(0, i + 2, keyWords[i])

        for index, one in enumerate(words_num):
            word_f = one[0]
            stock_code = one[1]
            stock_name = one[2]
            year = one[3]
            for ind, word in enumerate(keyWords):
                sheet.write(index + 1, ind + 2, word_f[word])
            sheet.write(index + 1, 0, year)
            sheet.write(index + 1, 1, stock_code)
            # sheet.write(index + 1, 1, stock_name)

        book.save(self.folder + '\年报关键词词频统计.xls')

    def judgmentTextEncoding(self, txt_folder, ):
        """判断txt文件夹中第一个txt文件的编码方式，作为程序的默认解析编码"""
        files = os.listdir(txt_folder)
        for file in files:
            if os.path.splitext(file)[-1] == ".txt":
                txt_path = os.path.join(txt_folder, file)
                # print(txt_path)
                break
        with open(txt_path, 'rb') as f:
            text = f.read()
            info = chardet.detect(text)
            # print(type(info))
            self.encoding = info['encoding']
            # print(self.encoding)

    def __init__(self, bank_list, seDate, kw):
        self.folder = ''
        self.category = ''
        self.bank_list = bank_list
        self.seDate = seDate
        self.kw = kw.split("、")  # 这里方
        self.pdf_name = []

        self.encoding = "utf-8"
        self.searchkey = ""
        self.error_list = [['错误列表如下，需要手动下载：']]
        self.succ_list = []


    def modechange(self, num):
        if num == 1:
            self.folder = '年报'
            self.category = 'category_ndbg_szsh'
        elif num == 2:
            self.folder = '半年报'
            self.category = 'category_bndbg_szsh'
        elif num == 3:
            self.folder = '一季报'
            self.category = 'category_yjdbg_szsh'
        elif num == 4:
            self.folder = '三季报'
            self.category = 'category_sjdbg_szsh'
        elif num == 5:
            self.folder = '社会责任'
            self.category = ''
            self.searchkey = '社会责任;'
        elif num == 0:
            self.folder = '自定义'
            self.category = ''#目录分类为空
            self.searchkey = '社会责任;'#这里自定义，搜索的关键词

    def step1(self):
        '''爬取pdf'''
        if not os.path.exists(self.folder):
            os.mkdir(self.folder)
        # 爬取pdf并转换为txt
        for bank in self.bank_list:
            orgId, plate, code, zwjc = self.get_adress(bank)
            self.get_PDF(orgId, plate, code, zwjc)
        print(self.error_list)

    def step2(self):
        '''pdf2txt'''
        print('开始pdf2txt！！！！！')
        self.pdf2txt()
        print("pdf2txt finish")

    def step2_5(self):
        print('开始提取文本编码格式！！！')
        self.judgmentTextEncoding(self.folder)

    def step3(self):
        '''词频统计'''
        print('开始统计词频！！！！！')
        # self.matchKeyWords(self.folder,self.kw)#不能统计到换行关键字
        self.matchKeyWords2(self.folder, self.kw)  # 可以统计到换行关键字，可能有未知bug
        print('统计结束')

def bank_xlsx(xlsxPath):
    '''用于读取xlsx表格'''
    data = xlrd.open_workbook(xlsxPath)
    table = data.sheets()[0]
    col1_value = table.col_values(0)
    return col1_value

class Producer(threading.Thread):

    def __init__(self,bank_queue,*args,**kwargs):
        super(Producer, self).__init__(*args, **kwargs)
        self.bank_queue = bank_queue
        self.error = []
        self.succ = []

    def run(self) -> None:
        the_thread = threading.current_thread()

        seDate = '2019-12-31~2020-12-31'  # 时间设置
        kw = "核心、价值、数字经济、提升"  # 关键词，顿号隔开

        while not self.bank_queue.empty():
            try:
                bank_list = []
                bank_list.append(self.bank_queue.get(timeout=10))
                try:
                    test = CrawlerForAnnualReport(bank_list, seDate, kw)  # 实例化
                    #这里定义线程任务
                    test.modechange(1)  # 这里选择模式
                    # test.step1()  # get pdf，如果不下载文件，跳转到get_PDF查看下载行是否被注释
                    test.step2()  # pdf2txt
                    # test.step2_5()  # 判断文本编码格式
                    # test.step3()  # 词频统计
                except:
                    print(bank_list+"发生错误")
            except:
                break


def single_thread():
    """单线程"""
    bank_list = bank_xlsx('banks.xlsx')[1:]  # 年报设置
    print(bank_list)
    seDate = '2019-12-31~2020-12-31'  # 时间设置
    kw = "核心、价值、数字经济、提升"  # 关键词，顿号隔开
    test = CrawlerForAnnualReport(bank_list, seDate, kw)  # 实例化
    test.modechange(1)  # 这里选择模式

    test.step1()  # get pdf，如果不下载文件，跳转到get_PDF查看下载行是否被注释
    # test.step2()  # pdf2txt
    # test.step2_5()  # 判断文本编码格式
    # test.step3()  # 词频统计

def multi_thread():
    """多线程，不太成熟"""
    bank_queue = queue.Queue(1000)
    bank_list = bank_xlsx('banks.xlsx')[1:]  # 年报设置
    for bank in bank_list:
        bank_queue.put(bank)

    #这里定义线程数量
    for x in range(2):
        th = Producer(bank_queue,name="爬虫%d号"%x)
        th.start()

if __name__ == '__main__':
    single_thread()
    # multi_thread()
