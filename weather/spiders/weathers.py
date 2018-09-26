# -*- coding: UTF-8 -*-
import json
import sys
import time

import pymysql
from datetime import datetime, timedelta
from scrapy import Request
from scrapy.spiders import Spider

from weather.items import RegionItem, BaseWeatherItem, DetailWeatherItem, HourWeatherItem, LifeHelperItem
from weather import settings

reload(sys)
sys.setdefaultencoding('utf8')


# 中国天气网按每省爬取天气概要
class BaseWeatherSpider(Spider):
    web_id = 1
    name = "baseWeather"
    allowed_domains = ["weather.com.cn"]

    def __init__(self):
        # 连接数据库
        self.connect = pymysql.connect(
            host=settings.MYSQL_HOST,
            port=3306,
            db=settings.MYSQL_DBNAME,
            user=settings.MYSQL_USER,
            passwd=settings.MYSQL_PASSWD,
            charset='utf8',
            use_unicode=True)
        # 通过cursor执行增删查改
        self.cursor = self.connect.cursor()

    def start_requests(self):
        # 获取所有省份url
        # starttime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        # start = datetime.datetime.now()
        self.cursor.execute(
            """select region_url_info.url from region_info, region_url_info where
                region_info.id = region_url_info.region_id and region_info.level=3""")
        urls = self.cursor.fetchall()
        # print urls

        for url in urls:
            yield Request(url=url[0], callback=self.parse)
        #    break

        # endtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        # end = datetime.datetime.now()
        # print "开始时间："+starttime+" 结束时间："+endtime
        # print "共运行 " + str((end-start).seconds)+" 秒"

    def parse(self, response):
        url = response._get_url()
        # print "url:" + url
        # 省份
        try:
            if response.status != 200:
                raise Exception("响应馬不是200")
            provience_name = response.xpath("//div[@class='contentboxTab']/h1/a[last()]/text()").extract_first()

            days = response.xpath("//div[@class='conMidtab']")
            i = 0
            today = datetime.today()
            # date = today.isoformat(" ")
            # print "provience_name:" + provience_name
            for day in days:
                # self.parse_day(selector=day, day=i)
                cities = day.xpath("./div[@class='conMidtab3']/table")
                for city in cities:
                    # self.parse_city(selector=city, days=day)
                    counties = city.xpath("./tr")
                    for county in counties:
                        yield self.parse_item(county, i, today)
                i = i + 1

        except Exception as error:
            print "error in BaseWeatherSpider.parse:" + str(error)
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            sql = """update region_url_info set q_times = q_times+1, 
                          last_query = %s where url = %s"""
            self.cursor.execute(sql, (date, url))
            self.connect.commit()
            pass
        else:
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            sql = """update region_url_info set q_times = q_times+1, s_times = s_times+1, 
                          last_query = %s, last_success = %s where url = %s"""
            self.cursor.execute(sql, (date, date, url))
            self.connect.commit()
            # print "parse success url:" + url
            pass
        finally:
            pass

    def parse_item(self, county, i, today):
        item = BaseWeatherItem()
        item["web_id"] = self.web_id
        item["days"] = i
        forecast = today + timedelta(days=i)
        item["forecast"] = forecast.isoformat(" ")
        item['region_code'] = county.xpath(".//a[@href]/@href").extract_first().strip().split("/")[4].split(".")[0]
        details = county.xpath("./td")
        length = len(details)
        item["tmp_max"] = details[length - 5].xpath("./text()").extract_first().strip()
        item["tmp_min"] = details[length - 2].xpath("./text()").extract_first().strip()
        item["weather_d"] = details[length - 7].xpath("./text()").extract_first().strip()
        item["weather_n"] = details[length - 4].xpath("./text()").extract_first().strip()
        item["wind_dir_d"] = details[length - 6].xpath("./span/text()").extract()[0].strip()
        item["wind_dir_n"] = details[length - 3].xpath("./span/text()").extract()[0].strip()
        item["wind_power_d"] = details[length - 6].xpath("./span/text()").extract()[1].strip()
        item["wind_power_n"] = details[length - 3].xpath("./span/text()").extract()[1].strip()
        return item


# 按地区爬取每个区详细天气信息和小时天气
class DetailWeatherSpider(Spider):

    web_id = 1
    name = "detailWeather"
    allowed_domains = ["weather.com.cn"]

    def __init__(self):
        # 连接数据库
        self.connect = pymysql.connect(
            host=settings.MYSQL_HOST,
            port=3306,
            db=settings.MYSQL_DBNAME,
            user=settings.MYSQL_USER,
            passwd=settings.MYSQL_PASSWD,
            charset='utf8',
            use_unicode=True)
        # 通过cursor执行增删查改
        self.cursor = self.connect.cursor()

    def start_requests(self):
        # 获取所有地区url
        self.cursor.execute(
            """select region_url_info.url from region_info, region_url_info where
                region_info.id = region_url_info.region_id and region_info.level >= 4""")
        urls = self.cursor.fetchall()
        print urls

        for url in urls:
            yield Request(url=self.convert_url(url[0]), callback=self.parse)

    def parse(self, response):
        url = response._get_url()
        code = url.split("/")[4].split(".")[0]
        # print "url:" + url+"  code:"+code
        try:
            if response.status != 200:
                raise Exception("响应馬不是200")
            weathers = response.xpath("//div[@class='blueFor-container']")
            item = DetailWeatherItem()
            item["region_code"] = code
            item["base_weather_list"] = self.parse_base_weather(weathers, code)
            item["hour_weather_list"] = self.parse_hour_weather(weathers, code)
            life_shzs = response.xpath("//div[@class='weather_shzs']")
            item["life_helper_list"] = self.parse_life_shzs(life_shzs, code)

            yield item

        except Exception as error:
            print "error in DetailWeatherSpider.parse:" + str(error)
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            sql = """update region_url_info set q_times = q_times+1, 
                          last_query = %s where url = %s"""
            self.cursor.execute(sql, (date, self.reconvert_url(url)))
            self.connect.commit()
            pass
        else:
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            sql = """update region_url_info set q_times = q_times+1, s_times = s_times+1, 
                          last_query = %s, last_success = %s where url = %s"""
            self.cursor.execute(sql, (date, date, self.reconvert_url(url)))
            self.connect.commit()
            # print "parse success url:" + url
            pass
        finally:
            pass
        pass

    # 解析七天按天天气预报
    def parse_base_weather(self, selector, code):
        # 解析script中的数据
        script = selector.xpath(".//script/text()").extract()[0]
        script_list = script.split(";")
        # print script
        tmp_max_s = script_list[0].split("=")[1].strip()
        tmp_max = tmp_max_s[1: len(tmp_max_s)-2].split(",")
        tmp_min_s = script_list[1].split("=")[1].strip()
        tmp_min = tmp_min_s[1: len(tmp_min_s)-2].split(",")
        sunup_s = script_list[4].split("=")[1].strip()
        sunup = sunup_s[1: len(sunup_s)-2].split(",")
        sunset_s = script_list[5].split("=")[1].strip()
        sunset = sunset_s[1: len(sunset_s)-2].split(",")

        weathers = selector.xpath("./ul[@class='blue-container sky']/li")
        item_list = []
        for i in range(0, 7):
            item = BaseWeatherItem()
            item["forecast"] = (datetime.today() + timedelta(days=i)).isoformat(" ")
            item["days"] = i
            item["web_id"] = self.web_id
            item["region_code"] = code
            # 保存script中的数据
            # 天气列表包括昨天和未来一周天气
            item["tmp_max"] = tmp_max[i+1].replace("\"", "").strip()
            item["tmp_min"] = tmp_min[i+1].replace("\"", "").strip()
            item["sunup"] = sunup[i].replace("\"", "").strip()
            item["sunset"] = sunset[i].replace("\"", "").strip()
            # 从html节点中解析未来一周的天气
            day_weather = weathers[i+1]
            item["blue_sky"] = day_weather.xpath("./@class").extract_first().split(" lv")[1][0]
            item["weather_d"] = day_weather.xpath("./i/@title").extract()[0].strip()
            item["weather_n"] = day_weather.xpath("./i/@title").extract()[1].strip()
            item["wind_dir_d"] = day_weather.xpath("./div/i/@title").extract()[0].strip()
            item["wind_dir_n"] = day_weather.xpath("./div/i/@title").extract()[1].strip()
            item["wind_power_d"] = day_weather.xpath("./p[@class='wind-info']/text()").extract_first().split("转")[0]
            item["wind_power_n"] = day_weather.xpath("./p[@class='wind-info']/text()").extract_first().split("转")[-1]
            item_list.append(item)

        return item_list

    def parse_hour_weather(self, selector, code):
        # 解析script中的数据
        script = selector.xpath(".//script/text()").extract()[1]
        script_list = script.split(";")
        hourly_data = json.loads(script_list[0].split("=")[1].strip())
        item_list = []
        # 提取七天的数据
        for i in range(0, 7):
            one_day = hourly_data[i]
            for one_hour in one_day:
                item = HourWeatherItem()
                item["forecast"] = one_hour["jf"]
                item["days"] = i
                item["web_id"] = self.web_id
                item["region_code"] = code
                # 保存每小时天气数据
                item["tmp"] = one_hour["jb"]
                item["weather"] = one_hour["ja"]
                item["wind_dir"] = one_hour["jd"]
                item["wind_power"] = one_hour["jc"]
                item_list.append(item)

        return item_list

    def parse_life_shzs(self, selector, code):
        life_list = selector.xpath("./div")
        item_list = []
        for i in range(0, 7):
            life_day = life_list[i]
            item = LifeHelperItem()
            item["forecast"] = (datetime.today() + timedelta(days=i)).isoformat(" ")
            item["days"] = i
            item["region_code"] = code
            item["web_id"] = self.web_id
            life_item = life_day.xpath("./dl")
            # 紫外线数据获取
            item["ul_ray_level"] = str(len(life_item[0].xpath(".//i[@class='active']")))+"/"+str(len(life_item[0].xpath(".//i")))
            item["ul_ray_name"] = life_item[0].xpath(".//em/text()").extract_first().strip()
            item["ul_ray_desc"] = life_item[0].xpath(".//dd/text()").extract_first().strip()
            # 血糖数据获取
            item["blood_sugar_level"] = str(len(life_item[1].xpath(".//i[@class='active']")))+"/"+str(len(life_item[1].xpath(".//i")))
            item["blood_sugar_name"] = life_item[1].xpath(".//em/text()").extract_first().strip()
            item["blood_sugar_desc"] = life_item[1].xpath(".//dd/text()").extract_first().strip()
            # 感冒数据获取
            item["influenza_level"] = str(len(life_item[2].xpath(".//i[@class='active']")))+"/"+str(len(life_item[2].xpath(".//i")))
            item["influenza_name"] = life_item[2].xpath(".//em/text()").extract_first().strip()
            item["influenza_desc"] = life_item[2].xpath(".//dd/text()").extract_first().strip()
            # 穿衣数据获取
            item["clothes_level"] = str(len(life_item[3].xpath(".//i[@class='active']")))+"/"+str(len(life_item[3].xpath(".//i")))
            item["clothes_name"] = life_item[3].xpath(".//em/text()").extract_first().strip()
            item["clothes_desc"] = life_item[3].xpath(".//dd/text()").extract_first().strip()
            # 洗车数据获取
            item["car_wash_level"] = str(len(life_item[4].xpath(".//i[@class='active']")))+"/"+str(len(life_item[4].xpath(".//i")))
            item["car_wash_name"] = life_item[4].xpath(".//em/text()").extract_first().strip()
            item["car_wash_desc"] = life_item[4].xpath(".//dd/text()").extract_first().strip()
            # 空气污染数据获取
            item["pollution_level"] = str(len(life_item[5].xpath(".//i[@class='active']")))+"/"+str(len(life_item[5].xpath(".//i")))
            item["pollution_name"] = life_item[5].xpath(".//em/text()").extract_first().strip()
            item["pollution_desc"] = life_item[5].xpath(".//dd/text()").extract_first().strip()

            item_list.append(item)

        return item_list

    # 将旧版的地区天气地址转换为新版的地址
    def convert_url(self, old_url):
        pa = old_url.split("/")
        new_url = pa[0]+"//"+pa[2]+"/"+pa[3]+"n/"+pa[4]
        return new_url

    # 将新版的地区天气地址转换为旧版的地址
    def reconvert_url(self, new_url):
        pa = new_url.split("/")
        new_url = pa[0]+"//"+pa[2]+"/"+pa[3][0:7]+"/"+pa[4]
        return new_url
