import requests
import json
import pandas as pd
from sql_config import *
from datetime import datetime, timedelta

class getDataFromSEE():


    def __init__(self,startdate,enddate,keyword):
        self.startdate = startdate
        self.enddate = enddate
        self.keyword = keyword
        self.table_name = 'sh_convertbond_publish_info_total'

    def get_data(self,page,size,begin_date,end_date):

        headers = {
            'Referer': 'http://www.sse.com.cn/',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
        }

        params = {

            'keyWord': self.keyword,
            # 'keyWord': '公告',
            'securityType': '0101,120100,020100,020200,120200',
            'beginDate': begin_date,
            'endDate': end_date,
            'pageHelp.pageSize': size,
            'pageHelp.pageCount': '50',
            'pageHelp.pageNo': page,
            'pageHelp.beginPage': str(page),
            'pageHelp.cacheSize': '1',
            'pageHelp.endPage': '5',
            # '_': '1692443889885',
        }

        response = requests.get(
            'http://query.sse.com.cn/security/stock/queryCompanyBulletin.do',
            params=params,
            headers=headers,
        )
        res = response.text

        res_json = json.loads(res)
        
        return res_json


    def wash_data(self,res):
        data_list = res['pageHelp']['data']
        

        df = pd.DataFrame()
        for data in data_list:
            tmp = pd.DataFrame()
            tmp.at[0,'title'] = data['TITLE']
            tmp.at[0,'add_date'] = data['ADDDATE']
            tmp.at[0,'code'] = data['SECURITY_CODE']
            tmp.at[0,'name'] = data['SECURITY_NAME']
            tmp.at[0,'see_date'] = data['SSEDATE']
            tmp.at[0,'url'] = data['URL']
            df = pd.concat([df,tmp])

        return df


    def get_data_and_save(self,begin_date,end_date):
        finish_data = pd.DataFrame()
        page = 1
        size = 200
        print("page:" + str(page))

        res = self.get_data(page,size,begin_date,end_date)
        total = res['pageHelp']['total']
        df = self.wash_data(res)
        finish_data = pd.concat([finish_data,df])

        while total > size * page:
            page += 1
            print("page:" + str(page))
            res = self.get_data(page,size,begin_date,end_date)
            total = res['pageHelp']['total']
            df = self.wash_data(res)
            finish_data = pd.concat([finish_data,df])
        # print(finish_data)
        # finish_data.to_csv("get_data.csv",index=False)
        finish_data.to_sql(self.table_name, sqlExecute.engine, if_exists='append', index=False, chunksize=100)
        print("begin_date: " + begin_date + " end_date: " + end_date + " has been submit")


    def run(self):
        startdate = datetime.strptime(self.startdate, '%Y-%m-%d')
        enddate = datetime.strptime(self.enddate, '%Y-%m-%d')
        
        current_month_start = startdate.replace(day=1) # 置为本月第一天
        next_month_start = (startdate.replace(day=1) + timedelta(days=32)).replace(day=1) # 置为下个月第一天
        
        while current_month_start <= enddate:
            if next_month_start > enddate:
                next_month_start = enddate + timedelta(days=1) # 因为后面要减一天
            
            tmp_startdate = current_month_start.strftime('%Y-%m-%d')
            tmp_enddate = (next_month_start - timedelta(days=1)).strftime('%Y-%m-%d')
            self.get_data_and_save(tmp_startdate, tmp_enddate)
            
            current_month_start = next_month_start
            next_month_start = (next_month_start + timedelta(days=32)).replace(day=1)




class cleanData():
    def __init__(self,keyword,tag):
        self.keyword = keyword
        self.tag = tag
        self.table_name = 'sh_convertbond_publish_info'


    def get_data_and_save_result(self):
        sql = 'select title,add_date,code,name,see_date from sh_convertbond_publish_info_total where title like \'%%{keyword}%%\' '.format(keyword = self.keyword)
        df = pd.read_sql(sql,sqlExecute.engine)
        df['type'] = self.tag
        print(df)
        df.to_sql(self.table_name, sqlExecute.engine, if_exists='append', index=False, chunksize=100)





def main():

    ## 获取证监会数据代码
    begin_date = '2021-11-01'
    end_date = '2021-12-31'
    keyword = '可转换公司债券'
    g1 = getDataFromSEE(begin_date,end_date,keyword)
    g1.run()

    begin_date = '2021-01-01'
    end_date = '2023-08-19'
    keyword = '可转债'
    g1 = getDataFromSEE(begin_date,end_date,keyword)
    g1.run()


    ## 数据清洗
    keyword_list = ['发审委审核通过','发审会审核通过','审核委员会审议通过','科创板上市委员会审议','上市委审议通过','上市委员会审核通过','审核委员会审核通过','发行审核委员会通过','发行审核委员审核通过']
    for keyword in keyword_list:
        tag = '审核委员会审核通过的公告'
        c = cleanData(keyword,tag)
        c.get_data_and_save_result()


    keyword_list = ['同意注册批复','核准批文','核准批复','中国证监会核准','中国证监会批复','可转换公司债券批复','中国证券监督管理委员会核准']
    for keyword in keyword_list:
        tag = '获得中国证监会同意注册批复的公告'
        c = cleanData(keyword,tag)
        c.get_data_and_save_result()

    keyword_list = ['募集说明书摘要','募集说明书（摘要）','债券发行公告']
    for keyword in keyword_list:
        tag = '可转换公司债券募集说明书摘要'
        c = cleanData(keyword,tag)
        c.get_data_and_save_result()





    





if __name__ == "__main__":
    main()