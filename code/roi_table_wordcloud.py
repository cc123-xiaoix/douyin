#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pymysql
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
import json
import math
import time
from wordcloud import WordCloud
import os
from datetime import datetime

import third
U2 = third.Ui_Form()

year_list, input_time, output_time, select_roi, time_type, cday, cday2, start_time_res, end_time_res, min_year, min_month, min_day, min_hour, min_minute, min_second, max_year, max_month, max_day, max_hour, max_minute, max_second = U2.get_number()
global time_select
time_select = ['start_end', 'time_interval']
# cday = datetime.strptime(cday, '%Y-%m-%d %H:%M:%S')
# cday2 = datetime.strptime(cday2, '%Y-%m-%d %H:%M:%S')
#中文设置
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']

# # 连接mysql
# conn = pymysql.connect(host='39.106.2.176', user='tju',password='123456',
#                        database='dyqc')
# sql_1 = "select * from qc_task_detail"
# sql_2 = "select * from qc_task_data_2111"
# sql_3 = "select * from qc_task_data_2112"
# #利用pandas直接获取数据
# qc_data = pd.read_sql(sql_1, conn)
# qc_data_2111 = pd.read_sql(sql_2,conn)
# qc_data_2112 = pd.read_sql(sql_3,conn)
# conn.close()


# In[2]:


#从数据库中读取表
def read_database(table,host,user,password,database):
    conn = pymysql.connect(host=host, user=user,password=password,
                           database=database)
    sql = "select * from "+table
    #利用pandas直接获取数据
    data = pd.read_sql(sql, conn)
    conn.close()
    return data


#将list中的json格式数据生成新列
def output_json_list(data,data_columns,columns_key):
    data[data_columns+'_'+columns_key] = data.apply(lambda x:json.loads(x[data_columns])[0].get(columns_key) if len(json.loads(x[data_columns]))!=0 else np.nan,axis = 1)

#将json格式数据生成新列
def output_json(data,data_columns,columns_key):
    data[data_columns+'_'+columns_key] = data.apply(lambda x:json.loads(x[data_columns]).get(columns_key) if len(json.loads(x[data_columns]))!=0 else np.nan,axis = 1)
    

#将时间变为年月日时分秒列
def time_connvert(data,columns):
    data[columns] = pd.to_datetime(data[columns], format='%Y-%m-%d %H:%M:%S')
    data[columns+'_year'] = data[columns].dt.year
    data[columns+'_month'] = data[columns].dt.month
    data[columns+'_day'] = data[columns].dt.day
    data[columns+'_hour'] = data[columns].dt.hour
    data[columns+'_minute'] = data[columns].dt.minute
    data[columns+'_second'] = data[columns].dt.second
    

#序号编码
def toDict(data,col):
    """
    这个函数的作用：生成替换字典，加入 col 列有两种值 A B
    则生成：{A: 0, B: 1}  即把 A 替换为 0 ， B 替换为 1
    """
    res = {}
    dic = []
    cnt = 0  
    for i in col:
        for e in data[i].unique():
            res[e] = cnt
            cnt += 1
        data = data.replace({i:res})
        cnt = 0
    return data


#查询空值
def search_null_values(data):
    print("各列是否包含空值:")
    print(data.isnull().any())
    print("各列包含空值数:")
    print(data.isnull().sum(axis=0))
#     # 顶多有 null_number 行是空值
#     null_number = sum(data.isnull().sum(axis=0))
#     # 把所有空值的行显示出来
#     print("所有包含空值的行:")
#     print(data[data.isnull().T.any()].head(null_number))
    
#删除空值
def delete_null_values(data,data_label):
#     for i in data_label_list:
    #删除列存在空值的数据
    new_data = data.dropna(subset=[data_label],inplace=False)
    #重新生成index行索引
    new_data = new_data.reset_index(drop=True)
    return new_data


#按团队分类
def class_Tju(data,advertiser_list,campaign_id_list):
    #重新生成index行索引
    data = data.reset_index(drop=True)

    #生成class_id列
    data['class_id']=''
    class_id_set = set()
    #     advertiser_list = [1696827732541517,1696898004380686,1696993595431943,1697736606312455,1702901918105607,1711685115933699,1714195536577544,1714296423936007,1716820186570759]
    #     campaign_id_list = [1701275894994999, 1707671910546491, 1710617989499931, 1711602056635412, 1711602126695463, 1711602029279243, 1711602073722932, 1712237352138804, 1713511682447380, 1713511667950603, 1713429197611028, 1713600825933843, 1713600864418931, 1713875338932260, 1714058004496420, 1714320718938164,1714510799518772, 1715209794393219, 1714780228454423, 1715568631855127, 1715569577116676, 1715569580818493, 1716417266343975, 1716724754923555, 1716724754923555, 1717177943513099, 1717449957879828, 1717992978009150, 1717996231896083, 1717996196826132, 1719191133927444, 1719411371675709, 1719681886585915]
    for i in range(len(data.index)):            
        if data['advertiser_id'][i] == advertiser_list[0] and data['campaign_id'][i] in campaign_id_list:
            data['class_id'][i] = 1
            class_id_set.add(1)
        elif data['advertiser_id'][i] == advertiser_list[1]:
            data['class_id'][i] = 2
            class_id_set.add(2)
        elif data['advertiser_id'][i] == advertiser_list[2]:
            data['class_id'][i] = 3
            class_id_set.add(3)
        elif data['advertiser_id'][i] == advertiser_list[3]:
            data['class_id'][i] = 4
            class_id_set.add(4)
        elif data['advertiser_id'][i] == advertiser_list[4]:
            data['class_id'][i] = 5
            class_id_set.add(5)
        elif data['advertiser_id'][i] == advertiser_list[5]:
            data['class_id'][i] = 6
            class_id_set.add(6)
        elif data['advertiser_id'][i] == advertiser_list[6]:
            data['class_id'][i] = 7
            class_id_set.add(7)
        elif data['advertiser_id'][i] == advertiser_list[7]:
            data['class_id'][i] = 8
            class_id_set.add(8)
        elif data['advertiser_id'][i] == advertiser_list[8]:
            data['class_id'][i] = 9
            class_id_set.add(9)
        else:
            data['class_id'][i] = 0


    #去除九类投放方以外的
    data = data[data.class_id.isin(class_id_set)]
    #重新生成index行索引
    data = data.reset_index(drop=True)
    return data


#按roi分类
def class_roi(data,data_label):
    new_data_label = 'class'+data_label
    data[new_data_label]=''
    for i in range(len(data.index)):
        if data[data_label][i] <= select_roi:
            data[new_data_label][i] = 0
        else:
            data[new_data_label][i] = 1
    return data


#表中的标签添加前缀
def add_pre_label(data,add_pre):
    temp_list = []
    for i in data.columns.values:
        i = add_pre+i
        temp_list.append(i)
    data.columns =temp_list
    return data


#分时间段  hour最大为24点
def class_time(data,data_label,mini,maxi):
    temp_list = []
    temp = '_time_hour'
    if data_label == data_label[:5]+temp: 
        if maxi == 24:
            for i in range(math.ceil(mini),24):
                temp_list.append(i)
        else:
            for i in range(math.ceil(mini),int(maxi+1)):
                temp_list.append(i)
    else:
        for i in range(math.ceil(mini),int(maxi+1)):
            temp_list.append(i)
    data = data[data.eval(data[data_label].name).isin(temp_list)]

    #重新生成index行索引
    data = data.reset_index(drop=True)
    return data


#特征类型转换
def type_to_int(data,data_label_list):
    for i in range(len(data_label_list)):
        data[data_label_list[i]] = data[data_label_list[i]].astype(int)
    return data


#保留每天最后一条数据
def today_last_data(data, min_day, max_day):
    data = data.set_index(input_time+'_time',drop=False)
    min_day_int =eval(min_day)
    max_day_int =eval(max_day)
    new_data = pd.DataFrame(columns = data.columns)
    #if data[input_time+'_time_month'] ==
    #同一月份
    if input_time == output_time:
        for i in range(min_day_int,max_day_int+1):
            today_data = data['20'+input_time[1:3]+'-'+input_time[3:]+'-'+str(i):'20'+input_time[1:3]+'-'+input_time[3:]+'-'+str(i)].sort_values(by=[input_time+'_time_year',input_time+'_time_month',input_time+'_time_day',input_time+'_time_hour',input_time+'_time_minute',input_time+'_time_second'],axis=0,ascending=False)
            today_data = today_data.drop_duplicates([input_time+'_ad_id']) #删除数据记录中col3列值相同的记录
            new_data = pd.concat([new_data,today_data])
    #不同月份
    else:
        #获取所有年份月份
        res = []
        for i in range(len(year_list)):
            res.append('20' + year_list[i][1:3] + year_list[i][3:])
        #中间月份
        for j in range(1, len(res)-1):
            year_data = data[data[input_time+'_time_year'].isin([int(res[j][:4])])]
            month_data = year_data[year_data[input_time + '_time_month'].isin([int(res[j][4:])])]
            for i in range(1, 31):
                today_data = month_data['20' + input_time[1:3] + '-' + input_time[3:] + '-' + str(i):'20' + input_time[
                                                                                                      1:3] + '-' + input_time[
                                                                                                                   3:] + '-' + str(
                    i)].sort_values(by=[input_time + '_time_year', input_time + '_time_month', input_time + '_time_day',
                                        input_time + '_time_hour', input_time + '_time_minute',
                                        input_time + '_time_second'], axis=0, ascending=False)
                today_data = today_data.drop_duplicates([input_time + '_ad_id'])  # 删除数据记录中col3列值相同的记录
                new_data = pd.concat([new_data, today_data])
        #首尾月份
        year_data = data[data[input_time + '_time_year'].isin([int(res[0][:4])])]
        month_data = year_data[year_data[input_time + '_time_month'].isin([int(res[0][4:])])]
        for i in range(min_day_int, 31):
            today_data = month_data['20' + input_time[1:3] + '-' + input_time[3:] + '-' + str(i):'20' + input_time[
                                                                                                        1:3] + '-' + input_time[
                                                                                                                     3:] + '-' + str(
                i)].sort_values(by=[input_time + '_time_year', input_time + '_time_month', input_time + '_time_day',
                                    input_time + '_time_hour', input_time + '_time_minute',
                                    input_time + '_time_second'], axis=0, ascending=False)
            today_data = today_data.drop_duplicates([input_time + '_ad_id'])  # 删除数据记录中col3列值相同的记录
            new_data = pd.concat([new_data, today_data])

        year_data = data[data[input_time + '_time_year'].isin([int(res[-1][:4])])]
        month_data = year_data[year_data[input_time + '_time_month'].isin([int(res[-1][4:])])]
        for i in range(1, max_day_int):
            today_data = month_data['20' + input_time[1:3] + '-' + input_time[3:] + '-' + str(i):'20' + input_time[
                                                                                                        1:3] + '-' + input_time[
                                                                                                                     3:] + '-' + str(
                i)].sort_values(by=[input_time + '_time_year', input_time + '_time_month', input_time + '_time_day',
                                    input_time + '_time_hour', input_time + '_time_minute',
                                    input_time + '_time_second'], axis=0, ascending=False)
            today_data = today_data.drop_duplicates([input_time + '_ad_id'])  # 删除数据记录中col3列值相同的记录
            new_data = pd.concat([new_data, today_data])
#     #重新生成index行索引
#     new_data = new_data.reset_index(drop=True)
    return new_data

#起始时间和终止时间转为datetime格式
def time_res_select(res):
    str_ = ''
    for keys in res.keys():
        if keys == 'year' or keys == 'month':
            str_ = str_+str(res.get(keys)) +'-'
            #print(1)
        elif keys == 'day':
            str_ = str_+str(res.get(keys)) +' '
            #print(2)
        elif keys == 'hour' or keys == 'minute':
            str_ = str_+str(res.get(keys)) +':'
        elif keys == 'second':
            str_ = str_+str(res.get(keys))
            #print(3)
    cday = datetime.strptime(str_, '%Y-%m-%d %H:%M:%S')
    return cday

#提出满足时间段的数据
def class_time_interval(data, min_year,max_year,min_month,max_month,min_day,max_day,min_hour,max_hour,min_minute,max_minute,min_second,max_second):
    #筛选时间段
    _31_days_month_list = ['01','03','05','07','08','10','12']
    _30_days_month_list = ['02','04','06','09','11'] 
    if min_year != '' or max_year != '':
        if min_year != '' and max_year != '':
            data = class_time(data,input_time+'_time_year',eval(min_year),eval(max_year))
        elif min_year != '':
            max_year = time.strftime('%Y',time.localtime(time.time()))
            data = class_time(data,input_time+'_time_year',eval(min_year),eval(time.strftime('%Y',time.localtime(time.time()))))
        elif max_year != '':
            min_year = '0'
            data = class_time(data,input_time+'_time_year',0,eval(max_year)) 
    else:
        min_year = '0'
        max_year = time.strftime('%Y',time.localtime(time.time()))

    if min_month != '' or max_month != '':
        if min_month != '' and max_month != '':
            data = class_time(data,input_time+'_time_month',eval(min_month),eval(max_month))
        elif min_month != '':
            max_month = '12'
            data = class_time(data,input_time+'_time_month',eval(min_month),eval(max_month))
        elif max_month != '':
            min_month = '1'
            data = class_time(data,input_time+'_time_month',eval(min_month),eval(max_month))
    else:
        min_month = '1'
        max_month = '12'

    if min_day != '' or max_day != '':
        if min_day != '' and max_day != '':
            data = class_time(data,input_time+'_time_day',eval(min_day),eval(max_day))
        elif min_day != '':
            if input_time[3:5] in _31_days_month_list:
                max_day = '31'
                data = class_time(data,input_time+'_time_day',eval(min_day),eval(max_day))
            elif input_time[3:5] in _30_days_month_list:
                max_day = '30'
                data = class_time(data,input_time+'_time_day',eval(min_day),eval(max_day))
        elif max_day != '':
            min_day = '1'
            data = class_time(data,input_time+'_time_day',eval(min_day),eval(max_day))  
    else:
        if input_time[3:5] in _31_days_month_list:
            min_day = '1'
            max_day = '31'
        elif input_time[3:5] in _30_days_month_list:
            min_day = '1'
            max_day = '30'

    if max_hour != '' or min_hour != '':
        if max_hour != '' and min_hour != '':
            data = class_time(data,input_time+'_time_hour',eval(min_hour),eval(max_hour))
        elif min_hour != '':
            max_hour = '23'
            data = class_time(data,input_time+'_time_hour',eval(min_hour),eval(max_hour))
        elif max_hour != '':
            min_hour = '0'
            data = class_time(data,input_time+'_time_hour',eval(min_hour),eval(max_hour))
    else:
        min_hour = '0'
        max_hour = '23'

    if min_minute != '' or max_minute != '':
        if min_minute != '' and max_minute != '':
            data = class_time(data,input_time+'_time_minute',eval(min_minute),eval(max_minute))
        elif min_minute != '':
            max_minute = '59'
            data = class_time(data,input_time+'_time_minute',eval(min_minute),eval(max_minute))
        elif max_minute != '':
            min_minute = '0'
            data = class_time(data,input_time+'_time_minute',eval(min_minute),eval(max_minute))
    else:
        min_minute = '0'
        max_minute = '59'

    if min_second != '' or max_second != '':
        if min_second != '' and max_second != '':
            data = class_time(data,input_time+'_time_second',eval(min_second),eval(max_second))
        elif min_second != '':
            max_second = '59'
            data = class_time(data,input_time+'_time_second',eval(min_second),eval(max_second))
        elif max_second != '':
            min_second = '0'
            data = class_time(data,input_time+'_time_second',eval(min_second),eval(max_second))
    else:
        min_second = '0'
        max_second = '59'
    return data, min_year,max_year,min_month,max_month,min_day,max_day,min_hour,max_hour,min_minute,max_minute,min_second,max_second
    #return min_year,max_year,min_month,max_month,min_day,max_day,min_hour,max_hour,min_minute,max_minute,min_second,max_second

#提出满足起始时间到终止时间的数据
def class_time_start_end(data, start_time_res, end_time_res):
    cday = time_res_select(start_time_res)
    cday2 = time_res_select(end_time_res)
    time_list = []
    #end_time_list = []
    data_time_list = []
    for i in data[input_time+'_time']:
        data_time_list.append(i)
    for i in data_time_list:
        if cday < i < cday2:
            time_list.append(i)
    data = data[data.eval(data[input_time+'_time'].name).isin(time_list)]
    return data
    #return new_qc_data


# ### 数据处理

# In[3]:


def data_pre_handle_no_toDict():
    global year_list, min_year,max_year,min_month,max_month,min_day,max_day,min_hour,max_hour,min_minute,max_minute,min_second,max_second
    # #数据库读取表
    # qc_detail = read_database("qc_task_detail",'39.106.2.176','tju','123456','dyqc')
    # qc_data = read_database("qc_task_data"+input_time,'39.106.2.176','tju','123456','dyqc')
    # #保存csv
    # qc_detail.to_csv('/Users/jiangshihua/Desktop/qc_data.csv')
    # qc_data.to_csv('/Users/jiangshihua/Desktop/qc_data'+input_time+'.csv')
    #读取csv
#     qc_detail = pd.read_csv('/Users/jiangshihua/Desktop/项目/data/qc_detail.csv')
#     qc_data = pd.read_csv('/Users/jiangshihua/Desktop/项目/data/qc_data'+input_time+'.csv')
    
    if not (os.path.isfile('./qc_detail.csv')):
        qc_detail = read_database("qc_task_detail",'39.106.2.176','tju','123456','dyqc')
        #保存为csv
        qc_detail.to_csv('./qc_detail.csv')
    elif (os.path.isfile('./qc_detail.csv')):
        #读取csv
        qc_detail = pd.read_csv('./qc_detail.csv')

    qc_data_list = []
    for i in range(len(year_list)):
        qc_data_list.append('qc_data' + str(i))
    for i in range(len(year_list)):
        if not (os.path.isfile('./qc_data' + year_list[i] + '.csv')):
            qc_data_list[i] = read_database("qc_task_data" + year_list[i], '39.106.2.176', 'tju', '123456', 'dyqc')
            # 保存为csv
            qc_data_list[i].to_csv('./qc_data' + year_list[i] + '.csv')
        elif (os.path.isfile('./qc_data' + year_list[i] + '.csv')):
            # 读取csv
            qc_data_list[i] = pd.read_csv('./qc_data' + year_list[i] + '.csv', index_col=0)

    qc_data = pd.DataFrame()
    for i in qc_data_list:
        qc_data = pd.concat([qc_data, i], axis=0)
        
        
    output_json_list(qc_detail,'aweme_info','aweme_show_id')
    output_json_list(qc_detail,'aweme_info','aweme_name')
    output_json(qc_detail,'audience','auto_extend_enabled')
    output_json(qc_detail,'audience','smart_interest_action')
    output_json(qc_detail,'audience','location_type')
    output_json(qc_detail,'audience','district')
    output_json(qc_detail,'audience','aweme_fan_behaviors_days')
    output_json(qc_detail,'audience','action_days')
    output_json(qc_detail,'audience','gender')
    output_json(qc_detail,'audience','age')
    output_json(qc_detail,'audience','city')
    output_json(qc_detail,'delivery_setting','smart_bid_type')
    output_json(qc_detail,'delivery_setting','external_action')
    output_json(qc_detail,'delivery_setting','schedule_fixed_range')
    output_json(qc_detail,'delivery_setting','budget_mode')
    output_json(qc_detail,'delivery_setting','budget')

    #删除空值
    qc_detail = delete_null_values(qc_detail,'campaign_id')
    qc_detail = delete_null_values(qc_detail,'audience_location_type')
    qc_detail = delete_null_values(qc_detail,'audience_auto_extend_enabled')
    

    #去掉Unnamed: 0列
    qc_detail = qc_detail.iloc[:,1:]
    qc_data = qc_data.iloc[:,1:]
    #表格每个标签添加前缀
    add_pre_label(qc_data,input_time+'_')
    #时间转换
    time_connvert(qc_data,input_time+'_time')

    if time_type == time_select[0]:
        qc_data = class_time_start_end(qc_data, start_time_res, end_time_res)
    elif time_type == time_select[1]:
        qc_data, min_year,max_year,min_month,max_month,min_day,max_day,min_hour,max_hour,min_minute,max_minute,min_second,max_second = class_time_interval(qc_data, min_year,max_year, min_month,max_month, min_day,max_day, min_hour,max_hour,min_minute,max_minute,min_second,max_second)
    qc_data = today_last_data(qc_data, min_day, max_day)
    
    return qc_data, qc_detail


# ### roi图表对比

# In[4]:


def class_data_table_roi(qc_data,qc_detail):
    #按roi分类
    qc_data = class_roi(qc_data,input_time+'_prepay_and_pay_order_roi')
    #合并表格
    qc_data['ad_id'] = qc_data[input_time+'_ad_id']
    qc_data = pd.merge(qc_data,qc_detail,on='ad_id',how='left')
    #训练所用标签
    qc_data_label_roi = ['marketing_goal','promotion_way','creative_material_mode','first_industry_id','second_industry_id','third_industry_id',
                          'creative_auto_generate','is_homepage_hide','delivery_setting_budget_mode','audience_location_type','audience_district',
                          'audience_auto_extend_enabled','delivery_setting_smart_bid_type','delivery_setting_budget','delivery_setting_external_action','class'+input_time+'_prepay_and_pay_order_roi']                    
    #空值删除
    qc_data = delete_null_values(qc_data,'marketing_goal')
    return qc_data
    
def Plot_bar_gender(data, data_label,roi_result_list):
    x_label = []
    for i in data[data_label].unique():
        x_label.append(i)
        
#     print(x_label)
    x_number = [0]*len(x_label)
    for j in data[data_label]:
        for k in range(len(x_label)):
            if j==x_label[k]:
                x_number[k]+=1
                
    
    
    fig = plt.figure(figsize=(15, 10), dpi=80)
    
    x_arange = np.arange(len(x_label))  # [0 1 2 3 4 5]，相当于x轴上的坐标序列

    bar_width = 0.35  # 一个bar的宽度，注意x轴每两项的刻度的间距为1，注意合理设置宽度

    #plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']  # 防止中文乱码

    """
    绘制条形图，各入参的含义：
    x_arange - bar_width / 2：第一个bar在x轴上的中心值，每个刻度值减一半bar宽度得到，第二个bar则是加一半bar宽度；
    scores_zhangsan：bar高度，这里也就是分数值；
    bar_width：bar宽度；
    label：标签
    """
    plt.bar(x_arange - bar_width / 2, x_number, bar_width, label=x_label)
    #plt.bar(range(len(num_list)), num_list1, bottom=num_list, label='girl',tick_label = name_list,fc = 'r')

    # 在各个bar上标注数值，使用zip()来同步遍历x_arange, scores_zhangsan, scores_lisi
    for x, x_number in zip(x_arange, x_number):

        plt.text(x - bar_width / 2, x_number + 1, x_number, ha='center', fontsize=12)

    plt.xlabel(data_label)
    plt.ylabel("频数")
    #plt.xticks(x_arange, labels=x_label)  # x轴上的刻度用courses的项来绘制
    plt.xticks([index - bar_width/2 for index in x_arange], labels=x_label)
    plt.title(roi_result_list)
    #plt.legend()

    return fig


def Plot_label_gender(data,data_label):
    global cday, cday2, select_roi
    roi_list = [0,1]
    roi_result_list = ['坏','好']
    for i in roi_list:
        data_roi = data[data['class'+input_time+'_prepay_and_pay_order_roi'].isin([i])]
        fig = Plot_bar_gender(data_roi, data_label,roi_result_list[i])

        cday = datetime.strftime(cday, '%Y-%m-%d %H:%M:%S')
        cday2 = datetime.strftime(cday2, '%Y-%m-%d %H:%M:%S')
        select_roi = str(select_roi)

        if time_type == time_select[0]:
            if not (os.path.exists('./roi_start_end')):
                os.makedirs('./roi_start_end')
                plt.savefig('./roi_start_end/'+ cday + '~~' + cday2 +'(roi='+select_roi+'_gender)_'+roi_result_list[i]+'.png', bbox_inches='tight')    
            elif (os.path.isfile('./roi_start_end/'+ cday + '~~' + cday2 +'(roi='+select_roi+'_gender)_'+roi_result_list[i]+'.png')):
                None
            elif (os.path.exists('./roi_start_end')):
                plt.savefig('./roi_start_end/'+ cday + '~~' + cday2 +'(roi='+select_roi+'_gender)_'+roi_result_list[i]+'.png', bbox_inches='tight')
        elif time_type == time_select[1]:
            if not (os.path.exists('./roi_time_interval')):
                os.makedirs('./roi_time_interval')
                plt.savefig('./roi_time_interval/'+ input_time[1:]+' '+min_day+' '+min_hour+':'+min_minute+':'+min_second+' '+max_day+' '+max_hour+':'+max_minute+':'+max_second+'(roi='+select_roi+'_gender)_'+roi_result_list[i]+'.png', bbox_inches='tight')
            elif (os.path.isfile('./roi_time_interval/'+ input_time[1:]+' '+min_day+' '+min_hour+':'+min_minute+':'+min_second+' '+max_day+' '+max_hour+':'+max_minute+':'+max_second+'(roi='+select_roi+'_gender)_'+roi_result_list[i]+'.png')):
                None
            elif (os.path.exists('./roi_time_interval')):
                plt.savefig('./roi_time_interval/'+ input_time[1:]+' '+min_day+' '+min_hour+':'+min_minute+':'+min_second+' '+max_day+' '+max_hour+':'+max_minute+':'+max_second+'(roi='+select_roi+'_gender)_'+roi_result_list[i]+'.png', bbox_inches='tight')

        cday = datetime.strptime(cday, '%Y-%m-%d %H:%M:%S')
        cday2 = datetime.strptime(cday2, '%Y-%m-%d %H:%M:%S')
        select_roi = float(select_roi)
                
#         if (os.path.exists('./picture/one_feature/roi')):
#             plt.savefig('./picture/one_feature/roi/'+input_time+'_'+min_day+'号~'+max_day+'号_'+min_hour+'时~'+max_hour+'时_'+min_minute+'分~'+max_minute+'分_'+min_second+'秒~'+max_second+'秒_roi_gender_'+roi_result_list[i]+'.png', bbox_inches='tight')
#         elif not (os.path.exists('./picture/one_feature/roi')):
#             os.makedirs('./picture/one_feature/roi')
#             plt.savefig('./picture/one_feature/roi/'+input_time+'_'+min_day+'号~'+max_day+'号_'+min_hour+'时~'+max_hour+'时_'+min_minute+'分~'+max_minute+'分_'+min_second+'秒~'+max_second+'秒_roi_gender_'+roi_result_list[i]+'.png', bbox_inches='tight')
            
        #plt.savefig('/Users/jiangshihua/Desktop/项目/picture/'+input_time+'_'+min_day+'号~'+max_day+'号_'+min_hour+'时~'+max_hour+'时_'+min_minute+'分~'+max_minute+'分_'+min_second+'秒~'+max_second+'秒_roi_gender_'+roi_result_list[i]+'.png', bbox_inches='tight')
        #plt.show()
        plt.tight_layout()
    #return qc_data_roi



# In[5]:


def Plot_bar_age(data, data_label,roi_result_list):
    data_list = list(data[data_label])
    x_label_set = set()
    for i in range(len(data_list)):
        if i != []:
            for j in range(len(data_list[i])):
                x_label_set.add(data_list[i][j])    
    x_label = list(x_label_set)
    
    x_number = [0]*len(x_label)
#     print(x_label)

    for i in range(len(data_list)):
        for j in range(len(data_list[i])):
            for k in range(len(x_label)):
                if data_list[i][j] == x_label[k]:
                    x_number[k] +=1
        
        
    fig = plt.figure(figsize=(15, 10), dpi=80)
    
    x_arange = np.arange(len(x_label))  # [0 1 2 3 4 5]，相当于x轴上的坐标序列

    bar_width = 0.35  # 一个bar的宽度，注意x轴每两项的刻度的间距为1，注意合理设置宽度

    #plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']  # 防止中文乱码

    """
    绘制条形图，各入参的含义：
    x_arange - bar_width / 2：第一个bar在x轴上的中心值，每个刻度值减一半bar宽度得到，第二个bar则是加一半bar宽度；
    scores_zhangsan：bar高度，这里也就是分数值；
    bar_width：bar宽度；
    label：标签
    """
    plt.bar(x_arange - bar_width / 2, x_number, bar_width, label=x_label)
    #plt.bar(range(len(num_list)), num_list1, bottom=num_list, label='girl',tick_label = name_list,fc = 'r')

    # 在各个bar上标注数值，使用zip()来同步遍历x_arange, scores_zhangsan, scores_lisi
    for x, x_number in zip(x_arange, x_number):

        plt.text(x - bar_width / 2, x_number + 1, x_number, ha='center', fontsize=12)

    plt.xlabel(data_label)
    plt.ylabel("频数")
    #plt.xticks(x_arange, labels=x_label)  # x轴上的刻度用courses的项来绘制
    plt.xticks([index - bar_width/2 for index in x_arange], labels=x_label)
    plt.title(roi_result_list)
    #plt.legend()

    return fig


def Plot_label_age(data,data_label):
    global cday, cday2, select_roi
    roi_list = [0,1]
    roi_result_list = ['坏','好']
    for i in roi_list:
        data_roi = data[data['class'+input_time+'_prepay_and_pay_order_roi'].isin([i])]
        fig = Plot_bar_age(data_roi, data_label,roi_result_list[i])

        cday = datetime.strftime(cday, '%Y-%m-%d %H:%M:%S')
        cday2 = datetime.strftime(cday2, '%Y-%m-%d %H:%M:%S')
        select_roi = str(select_roi)

        if time_type == time_select[0]:
            if not (os.path.exists('./roi_start_end')):
                os.makedirs('./roi_start_end')
                plt.savefig('./roi_start_end/'+ cday + '~~' + cday2 +'(roi='+select_roi+'_age)_'+roi_result_list[i]+'.png', bbox_inches='tight')
            elif (os.path.isfile('./roi_start_end/' + cday + '~~' + cday2 + '(roi=' + select_roi + '_age)_' + roi_result_list[
                        i] + '.png')):
                None
            elif (os.path.exists('./roi_start_end')):
                plt.savefig(
                    './roi_start_end/' + cday + '~~' + cday2 + '(roi=' + select_roi + '_age)_' + roi_result_list[
                        i] + '.png', bbox_inches='tight')
        elif time_type == time_select[1]:
            if not (os.path.exists('./roi_time_interval')):
                os.makedirs('./roi_time_interval')
                plt.savefig('./roi_time_interval/'+ input_time[1:]+' '+min_day+' '+min_hour+':'+min_minute+':'+min_second+' '+max_day+' '+max_hour+':'+max_minute+':'+max_second +'(roi='+select_roi+'_age)_'+roi_result_list[i]+'.png', bbox_inches='tight')
            elif (os.path.isfile('./roi_time_interval/' + input_time[
                                                     1:] + ' ' + min_day + ' ' + min_hour + ':' + min_minute + ':' + min_second + ' ' + max_day + ' ' + max_hour + ':' + max_minute + ':' + max_second + '(roi=' + select_roi + '_age)_' +
                            roi_result_list[i] + '.png')):
                None
            elif (os.path.exists('./roi_time_interval')):
                plt.savefig('./roi_time_interval/' + input_time[
                                                     1:] + ' ' + min_day + ' ' + min_hour + ':' + min_minute + ':' + min_second + ' ' + max_day + ' ' + max_hour + ':' + max_minute + ':' + max_second + '(roi=' + select_roi + '_age)_' +
                            roi_result_list[i] + '.png', bbox_inches='tight')

        cday = datetime.strptime(cday, '%Y-%m-%d %H:%M:%S')
        cday2 = datetime.strptime(cday2, '%Y-%m-%d %H:%M:%S')
        select_roi = float(select_roi)
#         if (os.path.exists('./picture/one_feature/roi')):
#             plt.savefig('./picture/one_feature/roi/'+input_time+'_'+min_day+'号~'+max_day+'号_'+min_hour+'时~'+max_hour+'时_'+min_minute+'分~'+max_minute+'分_'+min_second+'秒~'+max_second+'秒_roi_age_'+roi_result_list[i]+'.png', bbox_inches='tight')
#         elif not (os.path.exists('./picture/one_feature/roi')):
#             os.makedirs('./picture/one_feature/roi')
#             plt.savefig('./picture/one_feature/roi/'+input_time+'_'+min_day+'号~'+max_day+'号_'+min_hour+'时~'+max_hour+'时_'+min_minute+'分~'+max_minute+'分_'+min_second+'秒~'+max_second+'秒_roi_age_'+roi_result_list[i]+'.png', bbox_inches='tight')
        #plt.savefig('/Users/jiangshihua/Desktop/项目/picture/'+input_time+'_'+min_day+'号~'+max_day+'号_'+min_hour+'时~'+max_hour+'时_'+min_minute+'分~'+max_minute+'分_'+min_second+'秒~'+max_second+'秒_roi_age_'+roi_result_list[i]+'.png', bbox_inches='tight')
        #plt.show()
        plt.tight_layout()
    #return qc_data_roi
    #return fig


# In[6]:


def GetData_city(data,data_label):
    all_data = data[data_label]
    # 遍历所有列名，排除不需要的
#     cols = [i for i in all_data.columns if i not in [all_data.columns[-1]]]
#     data = all_data[cols]
    
    data_array = np.array(all_data)
    data_list = data_array.tolist()
    label = all_data.loc[:, all_data.columns[-1]]
    label_array = np.array(label)
    label_list = label_array.tolist()
#     print(data_list)
#     print(label_list)
    n_samples, n_features = data.shape
#     print(n_samples, n_features)
#     print(data)
#     print(label)
#     print(df)
    return data_list, label_list, n_samples, n_features


def Data_city():
    city_json = pd.read_json('./city.json')

    city_json_T = city_json.T
    # #重新生成index行索引
    # city_json_T = city_json_T.reset_index(drop=True)

    city_json_T_new = city_json_T[city_json_T['parent'].isin([0])]
    #重新生成index行索引
    city_json_T_new = city_json_T_new.reset_index(drop=True)

    city_json_T_new_label = list(city_json_T_new.columns)
    data, label, n_samples, n_features = GetData_city(city_json_T_new,city_json_T_new_label)
    return data, label, city_json_T_new

def Plot_bar_city(data, data_label,roi_result_list,city_data,label,city_json_T_new):
    data_list = list(data[data_label])

    city_list = []
    city_id_list = []
    res = {}
    for i in range(len(city_json_T_new)):
        city_list.append(label[i])
    #print(city_list)          
                    
    for i in range(len(city_json_T_new)):
        for j in range(len(city_list)):
            if city_data[i][4] == city_list[j]:
                city_id_list.append(city_data[i][2])
    #print(city_id_list)
    x_label = city_id_list
    
    #print(res)

    x_number = [0]*len(x_label)
    for i in range(len(data_list)):
        for j in range(len(data_list[i])):
            for k in range(len(x_label)):
                if data_list[i][j] == x_label[k]:
                    x_number[k] +=1
                    
    res = dict(zip(city_list,x_number))
    #print(res)
    
    
    fig = plt.figure(figsize=(15, 10), dpi=80)
    
    x_arange = np.arange(len(x_label))  # [0 1 2 3 4 5]，相当于x轴上的坐标序列

    bar_width = 0.35  # 一个bar的宽度，注意x轴每两项的刻度的间距为1，注意合理设置宽度

    #plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']  # 防止中文乱码

    """
    绘制条形图，各入参的含义：
    x_arange - bar_width / 2：第一个bar在x轴上的中心值，每个刻度值减一半bar宽度得到，第二个bar则是加一半bar宽度；
    scores_zhangsan：bar高度，这里也就是分数值；
    bar_width：bar宽度；
    label：标签
    """
    plt.bar(x_arange - bar_width / 2, x_number, bar_width, label=x_label)
    #plt.bar(range(len(num_list)), num_list1, bottom=num_list, label='girl',tick_label = name_list,fc = 'r')

    # 在各个bar上标注数值，使用zip()来同步遍历x_arange, scores_zhangsan, scores_lisi
    for x, x_number in zip(x_arange, x_number):

        plt.text(x - bar_width / 2, x_number + 1, x_number, ha='center', fontsize=12)

    plt.xlabel(data_label)
    plt.ylabel("频数")
    #plt.xticks(x_arange, labels=x_label)  # x轴上的刻度用courses的项来绘制
    plt.xticks([index - bar_width/2 for index in x_arange], labels=city_list)
    plt.title(roi_result_list)
    #plt.legend()

    return fig


def Plot_label_city(data,data_label,city_data,label,city_json_T_new):
    global cday, cday2, select_roi
    roi_list = [0,1]
    roi_result_list = ['坏','好']
    for i in roi_list:
        data_roi = data[data['class'+input_time+'_prepay_and_pay_order_roi'].isin([i])]
        fig = Plot_bar_city(data_roi, data_label,roi_result_list[i],city_data,label,city_json_T_new)

        cday = datetime.strftime(cday, '%Y-%m-%d %H:%M:%S')
        cday2 = datetime.strftime(cday2, '%Y-%m-%d %H:%M:%S')
        select_roi = str(select_roi)

        if time_type == time_select[0]:
            if not (os.path.exists('./roi_start_end')):
                os.makedirs('./roi_start_end')
                plt.savefig('./roi_start_end/'+ cday + '~~' + cday2 +'(roi='+select_roi+'_city)_'+roi_result_list[i]+'.png', bbox_inches='tight')    
            elif (os.path.isfile('./roi_start_end/'+ cday + '~~' + cday2 +'(roi='+select_roi+'_city)_'+roi_result_list[i]+'.png')):
                None
            elif (os.path.exists('./roi_start_end')):
                plt.savefig('./roi_start_end/'+ cday + '~~' + cday2 +'(roi='+select_roi+'_city)_'+roi_result_list[i]+'.png', bbox_inches='tight')
        elif time_type == time_select[1]:
            if not (os.path.exists('./roi_time_interval')):
                os.makedirs('./roi_time_interval')
                plt.savefig('./roi_time_interval/'+ input_time[1:]+' '+min_day+' '+min_hour+':'+min_minute+':'+min_second+' '+max_day+' '+max_hour+':'+max_minute+':'+max_second +'(roi='+select_roi+'_city)_'+roi_result_list[i]+'.png', bbox_inches='tight')
            elif (os.path.isfile('./roi_time_interval/'+ input_time[1:]+' '+min_day+' '+min_hour+':'+min_minute+':'+min_second+' '+max_day+' '+max_hour+':'+max_minute+':'+max_second +'(roi='+select_roi+'_city)_'+roi_result_list[i]+'.png')):
                None
            elif (os.path.exists('./roi_time_interval')):
                plt.savefig('./roi_time_interval/'+ input_time[1:]+' '+min_day+' '+min_hour+':'+min_minute+':'+min_second+' '+max_day+' '+max_hour+':'+max_minute+':'+max_second +'(roi='+select_roi+'_city)_'+roi_result_list[i]+'.png', bbox_inches='tight')

        cday = datetime.strptime(cday, '%Y-%m-%d %H:%M:%S')
        cday2 = datetime.strptime(cday2, '%Y-%m-%d %H:%M:%S')
        select_roi = float(select_roi)
#         if (os.path.exists('./picture/one_feature/roi')):
#             plt.savefig('./picture/one_feature/roi/'+input_time+'_'+min_day+'号~'+max_day+'号_'+min_hour+'时~'+max_hour+'时_'+min_minute+'分~'+max_minute+'分_'+min_second+'秒~'+max_second+'秒_city_'+roi_result_list[i]+'.png', bbox_inches='tight')
#         elif not (os.path.exists('./picture/one_feature/roi')):
#             os.makedirs('./picture/one_feature/roi')
#             plt.savefig('./picture/one_feature/roi/'+input_time+'_'+min_day+'号~'+max_day+'号_'+min_hour+'时~'+max_hour+'时_'+min_minute+'分~'+max_minute+'分_'+min_second+'秒~'+max_second+'秒_city_'+roi_result_list[i]+'.png', bbox_inches='tight')
        #plt.savefig('/Users/jiangshihua/Desktop/项目/picture/'+input_time+'_'+min_day+'号~'+max_day+'号_'+min_hour+'时~'+max_hour+'时_'+min_minute+'分~'+max_minute+'分_'+min_second+'秒~'+max_second+'秒_city_'+roi_result_list[i]+'.png', bbox_inches='tight')
        #plt.show()
        plt.tight_layout()
    #return qc_data_roi
    #return fig


# ### roi词云

# In[13]:



def WordCloud_getdata(data,data_label):
    list1 = list(data[data_label])
    list2 = []
    re_move=['[',']'] #无效数据
    #去除无效数据
    for x in list1:
        for i in re_move:
            x=x.replace(i,"")
        list2.append(x)
    data = ','.join(list2)
    return data


def img_grearte(data,data_label,roi_result_list):
    global cday, cday2, select_roi
#    mask=imread("boy.png")
#     with open("txt_save.txt","r") as file:
#         txt=file.read()
    data = WordCloud_getdata(data,data_label)
    
    word=WordCloud(background_color="white",                    width=1500,                   height=1700,
                   font_path="Arial Unicode.ttf",
                   #mask=mask,
                   ).generate(data)
    #word.to_file('/Users/jiangshihua/Desktop/项目/WordCloud/'+roi_result_list+input_time+'.png')
    #print(roi_result_list)
    
    plt.imshow(word)    #使用plt库显示图片
    plt.axis("off")

    cday = datetime.strftime(cday, '%Y-%m-%d %H:%M:%S')
    cday2 = datetime.strftime(cday2, '%Y-%m-%d %H:%M:%S')
    select_roi = str(select_roi)

    if time_type == time_select[0]:
        if not (os.path.exists('./roi_start_end')):
            os.makedirs('./roi_start_end')
            plt.savefig('./roi_start_end/'+ cday + '~~' + cday2 +'(roi='+select_roi+'_wordcloud)_'+roi_result_list+'.png', bbox_inches='tight')
        elif (os.path.isfile('./roi_start_end/'+ cday + '~~' + cday2 +'(roi='+select_roi+'_wordcloud)_'+roi_result_list+'.png')):
            None
        elif (os.path.exists('./roi_start_end')):
            plt.savefig('./roi_start_end/'+ cday + '~~' + cday2 +'(roi='+select_roi+'_wordcloud)_'+roi_result_list+'.png', bbox_inches='tight')

    elif time_type == time_select[1]:
        if not (os.path.exists('./roi_time_interval')):
            os.makedirs('./roi_time_interval')
            plt.savefig('./roi_time_interval/'+ input_time[1:]+' '+min_day+' '+min_hour+':'+min_minute+':'+min_second+' '+max_day+' '+max_hour+':'+max_minute+':'+max_second +'(roi='+select_roi+'_wordcloud)_'+roi_result_list+'.png', bbox_inches='tight')
        elif (os.path.isfile('./roi_time_interval/'+ input_time[1:]+' '+min_day+' '+min_hour+':'+min_minute+':'+min_second+' '+max_day+' '+max_hour+':'+max_minute+':'+max_second +'(roi='+select_roi+'_wordcloud)_'+roi_result_list+'.png')):
            None
        elif (os.path.exists('./roi_time_interval')):
            plt.savefig('./roi_time_interval/'+ input_time[1:]+' '+min_day+' '+min_hour+':'+min_minute+':'+min_second+' '+max_day+' '+max_hour+':'+max_minute+':'+max_second +'(roi='+select_roi+'_wordcloud)_'+roi_result_list+'.png', bbox_inches='tight')

    cday = datetime.strptime(cday, '%Y-%m-%d %H:%M:%S')
    cday2 = datetime.strptime(cday2, '%Y-%m-%d %H:%M:%S')
    select_roi = float(select_roi)

#     if (os.path.exists('./picture/one_feature/roi')):
#         plt.savefig('./picture/one_feature/roi/'+input_time+'_'+min_day+'号~'+max_day+'号_'+min_hour+'时~'+max_hour+'时_'+min_minute+'分~'+max_minute+'分_'+min_second+'秒~'+max_second+'秒_roi_wordcloud_'+roi_result_list+'.png', bbox_inches='tight')
#     elif not (os.path.exists('./picture/one_feature/roi')):
#         os.makedirs('./picture/one_feature/roi')
#         plt.savefig('./picture/one_feature/roi/'+input_time+'_'+min_day+'号~'+max_day+'号_'+min_hour+'时~'+max_hour+'时_'+min_minute+'分~'+max_minute+'分_'+min_second+'秒~'+max_second+'秒_roi_wordcloud_'+roi_result_list+'.png', bbox_inches='tight')
#     #plt.savefig('/Users/jiangshihua/Desktop/项目/picture/'+input_time+'_'+min_day+'号~'+max_day+'号_'+min_hour+'时~'+max_hour+'时_'+min_minute+'分~'+max_minute+'分_'+min_second+'秒~'+max_second+'秒_roi_wordcloud_'+roi_result_list+'.png', bbox_inches='tight')
    #plt.show()
#     #中文设置
# plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']


def Plot_WordCloud_roi(data,data_label):
    roi_list = [0,1]
    roi_result_list = ["坏","好"]
    for i in roi_list:
        data_roi = data[data['class'+input_time+'_prepay_and_pay_order_roi'].isin([i])]
        if not data_roi.empty:
            img_grearte(data_roi,data_label,roi_result_list[i])
    #return qc_data_roi
    #return fig


# In[4]:


# #输入内容
# input_time = '_2201'
# select_roi = 1
# #筛选时间段
# min_year = ''
# max_year = ''

# min_month = ''
# max_month = ''

# min_day = ''
# max_day = ''

# min_hour = ''
# max_hour = ''

# min_minute = ''
# max_minute = ''

# min_second = ''
# max_second = ''

# time_select = ['start_end', 'time_interval']
# #start_time_res = {'year':2022, 'month':1, 'day':10, 'hour':10, 'minute':10, 'second':10}
# #end_time_res = {'year':2022, 'month':1, 'day':20, 'hour':14, 'minute':10, 'second':10}
# #time_type = 'start_end'
# time_type = 'time_interval'
# # class_standard = '' #团队 or roi
# # analyse_idea = '' #t-sne or umap
# # one_feature_select = '' #gender or age or city


# In[9]:


qc_data, qc_detail = data_pre_handle_no_toDict()
qc_data = class_data_table_roi(qc_data, qc_detail)
#删除空值
qc_data_gender = delete_null_values(qc_data,'audience_gender')
Plot_label_gender(qc_data_gender,'audience_gender')


# In[10]:


# qc_data, qc_detail = data_pre_handle_no_toDict()
# qc_data = class_data_table_roi(qc_data, qc_detail)
Plot_label_age(qc_data,'audience_age')


# In[11]:


# qc_data, qc_detail = data_pre_handle_no_toDict()
# qc_data = class_data_table_roi(qc_data, qc_detail)
data, label, city_json_T_new = Data_city()
Plot_label_city(qc_data,'audience_city', data, label, city_json_T_new)


# In[14]:


# qc_data, qc_detail = data_pre_handle_no_toDict()
# qc_data = class_data_table_roi(qc_data, qc_detail)
Plot_WordCloud_roi(qc_data,'ad_keywords')


# In[ ]:




