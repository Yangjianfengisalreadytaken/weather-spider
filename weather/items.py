# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class RegionItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    code = Field()
    name = Field()
    e_name = Field()
    code = Field()
    level = Field()
    upper_node = Field()
    url = Field()
    pass


class BaseWeatherItem(Item):
    region_code = Field()

    web_id = Field()
    date = Field()
    days = Field()
    forecast = Field()

    tmp_max = Field()
    tmp_min = Field()
    weather_d = Field()
    weather_n = Field()
    wind_dir_d = Field()
    wind_dir_n = Field()
    wind_power_d = Field()
    wind_power_n = Field()

    sunup = Field()
    sunset = Field()
    blue_sky = Field()


class HourWeatherItem(Item):
    region_code = Field()
    web_id = Field()
    date = Field()
    days = Field()
    forecast = Field()

    weather = Field()
    tmp = Field()
    wind_dir = Field()
    wind_power = Field()


class LifeHelperItem(Item):
    region_code = Field()
    web_id = Field()
    date = Field()
    days = Field()
    forecast = Field()

    ul_ray_level = Field()
    ul_ray_name = Field()
    ul_ray_desc = Field()
    blood_sugar_level = Field()
    blood_sugar_name = Field()
    blood_sugar_desc = Field()
    influenza_level = Field()
    influenza_name = Field()
    influenza_desc = Field()
    clothes_level = Field()
    clothes_name = Field()
    clothes_desc = Field()
    car_wash_level = Field()
    car_wash_name = Field()
    car_wash_desc = Field()
    pollution_level = Field()
    pollution_name = Field()
    pollution_desc = Field()


class DetailWeatherItem(Item):
    region_code = Field()
    base_weather_list = Field()
    hour_weather_list = Field()
    life_helper_list = Field()
