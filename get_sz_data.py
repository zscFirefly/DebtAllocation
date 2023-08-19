import requests
import pandas as pd 
from sql_config import *
import time
from datetime import datetime, timedelta

class getDataFromSZSE():

    def __init__(self,key_list,tag,cnt):
        self.key_list = key_list
        self.tag = tag
        self.cnt = cnt
        self.table_name = 'convertbond_publish_info'

    def get_data(self,page):
        headers = {
            'Origin': 'http://www.szse.cn',
            'Referer': 'http://www.szse.cn/disclosure/listed/notice/index.html?stock=300239',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
        }

        params = {
            'random': '0.4828239397876348',
        }

        json_data = {
                'seDate': [
                '',
                '',
            ],
            'searchKey': self.key_list,
            'channelCode': [
                'listedNotice_disc',
            ],
            'pageSize': 50,
            'pageNum': page,
            # 'bigCategoryId': ['0109'],
        }

        response = requests.post('http://www.szse.cn/api/disc/announcement/annList', params=params, headers=headers, json=json_data, verify=False)

        res = response.json()
        print(res)

        return res


    def wash_data(self,res):
        data_list = res['data']

        df = pd.DataFrame()
        for data in data_list:
            tmp = pd.DataFrame()
            tmp.at[0,'id'] = data['id']
            tmp.at[0,'title'] = data['title']
            tmp.at[0,'publishTime'] = data['publishTime']
            tmp.at[0,'type'] = self.tag
            tmp.at[0,'secCode'] = str(data['secCode'][0])
            tmp.at[0,'secName'] = str(data['secName'][0])
            print(tmp)
            df = df.append(tmp,ignore_index=True)
        return df 

    def get_data_and_save(self):
        finish_data = pd.DataFrame()
        for i in range(1,self.cnt):
            res = self.get_data(i)
            df = self.wash_data(res)
            finish_data = finish_data.append(df,ignore_index=True)
            time.sleep(0.5)
        finish_data.to_sql(self.table_name, sqlExecute.engine, if_exists='append', index=False, chunksize=100)
        print("finish.")

        


def main():
    key_list = ['可转换公司债券募集说明书摘要']
    tag = '向不特定对象发行可转换公司债券募集说明书摘要'
    g = getDataFromSZSE(key_list,tag,10)
    g.get_data_and_save()

    key_list = ['向不特定对象发行可转换公司债券并在创业板上市募集说明书摘要']
    tag = '向不特定对象发行可转换公司债券募集说明书摘要'
    g = getDataFromSZSE(key_list,tag,2)
    g.get_data_and_save()


    key_list = ['宁波大叶园林设备股份有限公司创业板向不特定对象发行可转换公司债券募集说明书']
    tag = '向不特定对象发行可转换公司债券募集说明书摘要'
    g = getDataFromSZSE(key_list,tag,2)
    g.get_data_and_save()


    key_list = ['恒逸石化','募集说明书摘要']
    tag = '向不特定对象发行可转换公司债券募集说明书摘要'
    g = getDataFromSZSE(key_list,tag,2)
    g.get_data_and_save()


    key_list = ['中环海陆','募集说明书摘要']
    tag = '向不特定对象发行可转换公司债券募集说明书摘要'
    g = getDataFromSZSE(key_list,tag,2)
    g.get_data_and_save()


    key_list = ['小熊电器','募集说明书摘要']
    tag = '向不特定对象发行可转换公司债券募集说明书摘要'
    g = getDataFromSZSE(key_list,tag,2)
    g.get_data_and_save()

    key_list = ['英力股份','募集说明书摘要']
    tag = '向不特定对象发行可转换公司债券募集说明书摘要'
    g = getDataFromSZSE(key_list,tag,2)
    g.get_data_and_save()

    key_list = ['药石科技','募集说明书摘要']
    tag = '向不特定对象发行可转换公司债券募集说明书摘要'
    g = getDataFromSZSE(key_list,tag,2)
    g.get_data_and_save()


    key_list = ['山河药辅','向不特定对象发行可转换公司债券并在创业板上市募集说明书募集说明书摘要']
    tag = '向不特定对象发行可转换公司债券募集说明书摘要'
    g = getDataFromSZSE(key_list,tag,2)
    g.get_data_and_save()


    key_list = ['丝路视觉','向不特定对象发行可转换公司债券发行公告']
    tag = '向不特定对象发行可转换公司债券募集说明书摘要'
    g = getDataFromSZSE(key_list,tag,2)
    g.get_data_and_save()

    key_list = ['测绘股份','募集说明书（摘要）']
    tag = '向不特定对象发行可转换公司债券募集说明书摘要'
    g = getDataFromSZSE(key_list,tag,2)
    g.get_data_and_save()

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
   

    key_list = ['可转换公司债券','核准批复']
    tag = '获得中国证监会同意注册批复的公告'
    g2 = getDataFromSZSE(key_list,tag,5)
    g2.get_data_and_save()

    key_list = ['可转换公司债券','同意批复']
    tag = '获得中国证监会同意注册批复的公告'
    g2 = getDataFromSZSE(key_list,tag,5)
    g2.get_data_and_save()

    key_list = ['可转换公司债券','核准批文']
    tag = '获得中国证监会同意注册批复的公告'
    g2 = getDataFromSZSE(key_list,tag,5)
    g2.get_data_and_save()

    key_list = ['可转换公司债券','同意注册']
    tag = '获得中国证监会同意注册批复的公告'
    g2 = getDataFromSZSE(key_list,tag,5)
    g2.get_data_and_save()


    key_list = ['可转换公司债券','获得中国证监会核准']
    tag = '获得中国证监会同意注册批复的公告'
    g2 = getDataFromSZSE(key_list,tag,5)
    g2.get_data_and_save()

    key_list = ['可转债','核准批复']
    tag = '获得中国证监会同意注册批复的公告'
    g2 = getDataFromSZSE(key_list,tag,2)
    g2.get_data_and_save()


    key_list = ['可转债','同意批复']
    tag = '获得中国证监会同意注册批复的公告'
    g2 = getDataFromSZSE(key_list,tag,5)
    g2.get_data_and_save()

    key_list = ['可转债','核准批文']
    tag = '获得中国证监会同意注册批复的公告'
    g2 = getDataFromSZSE(key_list,tag,5)
    g2.get_data_and_save()

    key_list = ['可转债','同意注册']
    tag = '获得中国证监会同意注册批复的公告'
    g2 = getDataFromSZSE(key_list,tag,5)
    g2.get_data_and_save()

    key_list = ['可转债','获得中国证监会核准']
    tag = '获得中国证监会同意注册批复的公告'
    g2 = getDataFromSZSE(key_list,tag,5)
    g2.get_data_and_save()


    key_list = ['卡倍亿','获得深圳证券交易所创业板上市委审核通过的公告']
    tag = '获得中国证监会同意注册批复的公告'
    g2 = getDataFromSZSE(key_list,tag,2)
    g2.get_data_and_save()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 



    key_list = ['可转换公司债券','委员会审核通过']
    tag = '审核委员会审核通过的公告'
    g3 = getDataFromSZSE(key_list,tag,10)
    g3.get_data_and_save()

    key_list = ['可转换公司债券','委员会审议通过']
    tag = '审核委员会审核通过的公告'
    g3 = getDataFromSZSE(key_list,tag,5)
    g3.get_data_and_save()

    key_list = ['可转换公司债券','发审委审核通过']
    tag = '审核委员会审核通过的公告'
    g3 = getDataFromSZSE(key_list,tag,5)
    g3.get_data_and_save()

    key_list = ['可转债','发审委审核通过']
    tag = '审核委员会审核通过的公告'
    g3 = getDataFromSZSE(key_list,tag,5)
    g3.get_data_and_save()

    key_list = ['可转债','发审会审核通过'] 
    tag = '审核委员会审核通过的公告'
    g3 = getDataFromSZSE(key_list,tag,5)
    g3.get_data_and_save()

    key_list = ['可转换公司债券','发审会审核通过']
    tag = '审核委员会审核通过的公告'
    g3 = getDataFromSZSE(key_list,tag,2)
    g3.get_data_and_save()


    key_list = ['可转换公司债券','上市委审核通过']
    tag = '审核委员会审核通过的公告'
    g3 = getDataFromSZSE(key_list,tag,10)
    g3.get_data_and_save()

    key_list = ['可转换公司债券','审核中心审核']
    tag = '审核委员会审核通过的公告'
    g3 = getDataFromSZSE(key_list,tag,2)
    g3.get_data_and_save()

    key_list = ['亚康股份','审核中心审核公司可转债申请结果'] # 亚康
    tag = '审核委员会审核通过的公告'
    g3 = getDataFromSZSE(key_list,tag,2)
    g3.get_data_and_save()


    key_list = ['杭氧股份','关于公开发行可转债申请获得中国证监会发行审核委员会审核通过的公告']
    tag = '审核委员会审核通过的公告'
    g3 = getDataFromSZSE(key_list,tag,2)
    g3.get_data_and_save()


    key_list = ['盘龙药业','关于公开发行可转换公司债券发审委会议准备工作的函的回复公告']
    tag = '审核委员会审核通过的公告'
    g3 = getDataFromSZSE(key_list,tag,2)
    g3.get_data_and_save()

    key_list = ['姚记科技','获得深圳证券交易所审核通过的公告']
    tag = '审核委员会审核通过的公告'
    g3 = getDataFromSZSE(key_list,tag,2)
    g3.get_data_and_save()


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 


    key_list = ['可转换公司债券申请文件反馈意见回复']
    tag = '反馈意见回复'
    g4 = getDataFromSZSE(key_list,tag,5)
    g4.get_data_and_save()




if __name__ == '__main__':
    main()
