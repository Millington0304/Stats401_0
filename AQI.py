import time
import csv
import re
import gevent
from gevent import monkey,pool
#monkey.patch_all()

import requests

# 存储城市url
city_url = []
# 存储要查看的时间范围
weather_date = []
# task列表
task_list = []


def func(url):
    """获取html页面"""
    response = requests.get(url)
    # 判断是否请求成功
    if response.status_code != 200:
        print("请求失败")
        print(response.headers)
        return
    html = response.content.decode("gbk")
    response.close()
    return html


def get_city_url_list(url):
    """获取城市url列表"""
    try:
        html = func(url)
    except Exception as e:
        return
    city_list = re.findall(r"<dd>(.*?)</dd>", html, re.S)
    for i in city_list:
        # cities = re.findall(r'<a href="(.*?)"\s?>', i)
        cities = re.findall(r'<a href="(.*?)"\s?>(.*?)</a>', i)
        for j in cities:
            # city_url.append('http://www.tianqihoubao.com' + j)
            city_url.append(j)


def get_day_weather_data(urlname):
    """获取每个城市的2019全年与2020年之间的天气情况，并存储为csv文件
    urlname:tuple，第一项为url，第二项为name
    f:文件描述符
    """
    with open(r".\2020年1-3月\%s.csv" % urlname[1], "w", newline='',encoding="utf-8-sig") as f:
        name = urlname[1]
        for i in weather_date:
            new_url = re.sub(r"\.html", "-"+i+".html", urlname[0])
            print(new_url)
            html = func('http://www.tianqihoubao.com' + new_url)
            # 失败的话尝试3次
            # times = 0
            # while times < 3:
            #     try:
            #         html = func('http://www.tianqihoubao.com' + new_url)
            #         break
            #     except Exception as e:
            #         time.sleep(2)
            #         times += 1
            # if times == 3:
            #     return
            row_list = re.findall(r"<tr>(.*?)</tr>", html, re.S)
            for j in row_list[1:]:
                aqi_data = re.findall(r"<td.*?>\s*(.*?)\s*</td>", j, re.S)
                # 2. 基于文件对象构建 csv写入对象
                csv_writer = csv.writer(f)
                csv_writer.writerow([name]+aqi_data)


def filter_cities(url):
    cities=["上海","北京","深圳"]
    cities=[x+" " for x in cities]
    new_urls=[]
    for i in url:
        if i[1] in cities:
            new_urls.append(i)
    return new_urls

if __name__ == "__main__":
    get_city_url_list("http://www.tianqihoubao.com/aqi/")
    # print(city_url)
    # for i in city_url[41:]:
    #     print(i)
    # 处理标签变为201903这种格式
    date_list = [str(i) for i in range(1, 13)]
    for i in date_list:
        if len(i) < 2:
            i = "0" + i
        weather_date.append("2020"+i)
    # print(weather_date)
    # with open(r"全国城市天气2.csv", "w") as f:
    #     for i in city_url:
    #         get_day_weather_data(i, f)

    # 多协程生成csv文件
    city_url=filter_cities(city_url)
    task_pool = pool.Pool(10)
    for i in city_url:
        #get_day_weather_data(i,)
        task_pool.apply_async(get_day_weather_data, args=(i,))
    task_pool.join()
import os


# 如果需要将所有文件合成为一个的话，执行这段代码
path = 'C:\\Users\\KelM\\Desktop\\2020年1-3月'
pathnames = []
for (dirpath, dirnames, filenames) in os.walk(path):
    for filename in filenames:
        print()
        pathnames += [os.path.join(path, filename)]
print(pathnames)


with open(r".\weather_data_202001-03.csv", "w",encoding="utf-8-sig") as f:
    for i in pathnames:
        with open(i, "r",encoding="utf-8-sig") as g:
            data = g.read()
            f.write(data)