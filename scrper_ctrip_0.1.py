# !/usr/bin/env python
# -*- coding: utf-8 -*-

#导入模块
import queue
import csv
import json
import re
import time
import datetime
import requests
import gevent
from gevent import monkey;monkey.patch_all()

#----------module document----------
__pyVersion__ = '3.6.0'
__author__ = 'Zhongxin Yue'
#----------module document----------

__doc__ = '''   A page Scraper for Ctrip. 
获取携程网单程机票信息 url:'http://flights.ctrip.com/domestic/Search/FirstRoute'
爬取方式：Queue+gevent
默认爬取内容：厦门到上海 2017-04-04 到 2014-04-06的单程机票信息
更多功能进一步讨论后再添加
'''

#-----给出起始日期和最终日期可以返回一个包含这期间所有日期的list用于url构造
def datelist(start, end):
    start_date = datetime.date(*start)
    end_date = datetime.date(*end)

    result = []
    curr_date = start_date
    while curr_date != end_date:
        result.append("%04d-%02d-%02d" % (curr_date.year, curr_date.month, curr_date.day))
        curr_date += datetime.timedelta(1)
    result.append("%04d-%02d-%02d" % (curr_date.year, curr_date.month, curr_date.day))
    return result

#获取页面信息，反爬也在这里做文章
def get_html(url):
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'

    headers = {'User-Agent':user_agent,
               'Accept':'* / *',
               'Host': 'flights.ctrip.com',
               'Referer':'http://flights.ctrip.com/booking/xmn-sha-day-1.html?ddate1=2017-04-10'
               }
    try:
        r = requests.get(url, headers)
        return r.text
    except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
        print('Fail to get',url)
        return None

#构造url
def join_url(st_time=(2017,4,4),end_time=(2017,4,6),payload1='XMN',payload2='SHA'):
    '''
    payload 输入三个参数分别为出发城市，到达城市，出发日期  并构造储存名
    '''
    date_list = datelist(st_time, end_time)
    init_url = 'http://flights.ctrip.com/domesticsearch/search/SearchFirstRouteFlights?DCity1={DCity1}&ACity1={ACity1}&DDate1={DDate1}'
    joinurls =[]
    for i in date_list:
        joinurls.append(init_url.format(DCity1=payload1, ACity1=payload2, DDate1=i))

    return joinurls

#解析页面信息，并存储（存储名从url中构造出）
def parse_json(url):
    html =get_html(url)

#从url中获取存储名
    save_name= '-'.join(re.findall(r'DCity1=(.*?)&',url)+re.findall(r'ACity1=(.*?)&',url))
    if html:
        info = json.loads(html)
        fis = info['fis']
        info_list = []
        for i in fis:
            slist = []
            slist.append(i['fn'])
            slist.append(str(i['dcn']))
            slist.append(str(i['dpbn']))
            slist.append(str(i['acn']))
            slist.append(str(i['apbn']))
            slist.append(str(i['dt']))
            slist.append(str(i['at']))
            slist.append(str(i['lp']))
            print(' '.join(slist))
            info_list.append(slist)
        save_csv(info_list, save_name)
        return info_list

    else:
        print('Fail to get info')

#储存函数
def save_csv(info_list,save_name):

    with open(save_name, 'a',newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(info_list)

def create_csv(name):
    titles = ['fn','dpt_city', 'dpt_airport', 'at_city', 'at_airport', 'dpt_time', 'at_time', 'price']
    with open(name, 'w',newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(titles)

#主函数
def main():
    print(__doc__)
    ts = time.time()

#获取构造的url并创建csv文集那
    urls =join_url((2017,4,4),(2017,4,6),'XMN','SHA')
    save_name = '-'.join(re.findall(r'DCity1=(.*?)&', urls[0]) + re.findall(r'ACity1=(.*?)&', urls[0]))
    create_csv(save_name)
# 运行函数，并使用gevent，修改下面的参数就可以修改爬取内容
    q =queue.Queue()
    for i in urls:
        q.put(i)
    jobs = []
    while not q.empty():
        jobs.append(gevent.spawn(parse_json, q.get()))
    gevent.joinall(jobs)
    end = time.time()

#统计爬取数据量与耗时
    with open('XMN-SHA', 'r') as csvfile:
        reader = csv.reader(csvfile)
        rows = [row for row in reader]
    print('Cost time',end-ts,'抓取：',len(rows))

if __name__ == '__main__':
    main()
