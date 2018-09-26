# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import datetime
import pymysql as pymysql

from weather import settings


class RegionPipeline(object):
    def __init__(self):
        # print "pipeline.init"
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

    def process_item(self, item, spider):
        # print "pipeline.process_item"
        try:
            # 查重处理
            self.cursor.execute(
                """select * from region_info where code = %s""",
                item['code'])
            # 是否有重复数据
            repetition = self.cursor.fetchone()
            # 重复
            if repetition:
                pass

            else:
                # 插入地区数据
                self.cursor.execute(
                    """insert into region_info(name, level, upper_name, code)
                    value (%s, %s, %s, %s)""",
                    (item['name'],
                     item['level'],
                     item['upper_node'],
                     item['code']))
                # 提交sql语句
                self.connect.commit()

                # 插入地区url数据
                self.cursor.execute(
                    """select id from region_info where code = %s""",
                    item['code'])

                # 获取地区id
                repetition = self.cursor.fetchone()
                if repetition:
                    # 插入地区数据
                    self.cursor.execute(
                        """insert into region_url_info(web_id, region_id, url)
                      value (%s, %s, %s)""",
                        (1, repetition[0], item['url']))
                    self.connect.commit()

        except Exception as error:
            # 出现错误时打印错误日志
            print "error:" + str(error)
        return item


class WeatherPipeline(object):
    def __init__(self):
        # print "pipeline.init"
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

    def process_item(self, item, spider):
        # print "pipeline.process_item"
        try:
            self.cursor.execute(
                """select id from region_info where code = %s""",
                item['region_code'])
            # 获取地区id
            region_id = self.cursor.fetchone()[0]
            # 查重处理
            self.cursor.execute(
                """select * from base_weather_info where 'date' = %s 
                   and web_id = %s and region_id = %s and days = %s""",
                (item['date'],
                 item["web_id"],
                 region_id,
                 item["days"]))
            # 是否有重复数据
            repetition = self.cursor.fetchone()
            # 重复
            if repetition:
                pass

            else:
                # 插入天气数据
                self.cursor.execute(
                    """insert into base_weather_info(region_id, web_id, days,
                        tmp_max, tmp_min, weather_d, weather_n, wind_dir_d, wind_dir_n, 
                        wind_power_d, wind_power_n, forecast_date)
                    value (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (region_id,
                     item['web_id'],
                     item['days'],
                     item['tmp_max'],
                     item['tmp_min'],
                     item['weather_d'],
                     item['weather_n'],
                     item['wind_dir_d'],
                     item['wind_dir_n'],
                     item['wind_power_d'],
                     item['wind_power_n'],
                     item['forecast']))
                # 提交sql语句
                self.connect.commit()

        except Exception as error:
            # 出现错误时打印错误日志
            print "error:" + str(error)
        return item


class DetailPipeline(object):
    def __init__(self):
        # print "pipeline.init"
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

    def process_item(self, item, spider):
        try:
            self.cursor.execute(
                """select id from region_info where code = %s""",
                item['region_code'])
            # 获取地区id
            region_id = self.cursor.fetchone()[0]

            base_weather_list = item['base_weather_list']
            hour_weather_list = item['hour_weather_list']
            life_helper_list = item['life_helper_list']
            # 保存七天的基本天气信息
            for base_weather in base_weather_list:
                self.save_base_weather(base_weather, region_id)
            self.connect.commit()
            # 保存七天的按小时天气信息
            for hour_weather in hour_weather_list:
                self.save_hour_weather(hour_weather, region_id)
            self.connect.commit()
            # 保存七天的生活助手信息
            for life_helper in life_helper_list:
                self.save_life_data(life_helper, region_id)
            self.connect.commit()

        except Exception as error:
            # 出现错误时打印错误日志
            print "error in process_item:" + str(error)
        return item

    # 保存按天天气信息
    def save_base_weather(self, item, region_id):
        # 插入天气数据
        try:
            self.cursor.execute(
                """insert into base_weather_info(region_id, web_id, days,
                    tmp_max, tmp_min, weather_d, weather_n, wind_dir_d, wind_dir_n, 
                    wind_power_d, wind_power_n, forecast_date, sunrise, sunset, blue_sky)
                    value (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (region_id,
                    item['web_id'],
                    item['days'],
                    item['tmp_max'],
                    item['tmp_min'],
                    item['weather_d'],
                    item['weather_n'],
                    item['wind_dir_d'],
                    item['wind_dir_n'],
                    item['wind_power_d'],
                    item['wind_power_n'],
                    item['forecast'],
                    item['sunup'],
                    item['sunset'],
                    item['blue_sky']))
        except Exception as error:
            # 出现错误时打印错误日志
            print "error in save_base_weather:" + str(error)

    # 保存每小时天气
    def save_hour_weather(self, item, region_id):
        # 插入天气数据
        try:
            self.cursor.execute(
                """insert into hour_weather_info(region_id, web_id, days, date_hour,
                  tmp, weather, wind_dir, wind_power)
                  value (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (region_id,
                    item['web_id'],
                    item['days'],
                    item['forecast'],
                    item['tmp'],
                    item['weather'],
                    item['wind_dir'],
                    item['wind_power']))
        except Exception as error:
            # 出现错误时打印错误日志
            print "error in save_hour_weather:" + str(error)

    # 保存生活助手数据
    def save_life_data(self, item, region_id):
        try:
            self.cursor.execute(
                """insert into life_helper_info(region_id, web_id, days, forecast,
                  ul_ray_level, ul_ray_name, ul_ray_desc, 
                  blood_sugar_level, blood_sugar_name, blood_sugar_desc, 
                  influenza_level, influenza_name, influenza_desc, 
                  clothes_level, clothes_name, clothes_desc, 
                  car_wash_level, car_wash_name, car_wash_desc, 
                  pollution_level, pollution_name, pollution_desc)
                  value (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (region_id, item['web_id'], item['days'], item['forecast'],
                    item['ul_ray_level'], item['ul_ray_name'], item['ul_ray_desc'],
                    item['blood_sugar_level'], item['blood_sugar_name'], item['blood_sugar_desc'],
                    item['influenza_level'], item['influenza_name'], item['influenza_desc'],
                    item['clothes_level'], item['clothes_name'], item['clothes_desc'],
                    item['car_wash_level'], item['car_wash_name'], item['car_wash_desc'],
                    item['pollution_level'], item['pollution_name'], item['pollution_desc']))
        except Exception as error:
            # 出现错误时打印错误日志
            print "error in save_life_data:" + str(error)
