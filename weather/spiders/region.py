# -*- coding: UTF-8 -*-
import sys
import pymysql
from scrapy import Request
from scrapy.spiders import Spider

from weather.items import RegionItem
from weather import settings

reload(sys)
sys.setdefaultencoding('utf8')


class RegionSpider(Spider):
    name = "region"
    allowed_domains = ["weather.com.cn"]
    # start_urls = [
    #     "http://www.weather.com.cn/textFC/hb.shtml#",
    # ]
    base_url = "http://www.weather.com.cn"

    def start_requests(self):
        urls = [
            "http://www.weather.com.cn/textFC/hn.shtml#",
        ]
        for url in urls:
            yield Request(url=url, callback=self.parse_area)

    def parse_area(self, response):
        sites = response.xpath("//ul[@class='lq_contentboxTab2']//a")
        for site in sites:
            item = RegionItem()
            item['level'] = 2
            item['upper_node'] = '中国'
            item['name'] = site.xpath("text()").extract_first().strip()
            item['e_name'] = site.xpath("@href").extract_first().strip().split("/")[2].split(".")[0]
            item['url'] = self.base_url + site.xpath("@href").extract_first().strip()
            # print item
            yield item
            yield Request(url=item['url'], callback=self.parse_provience)

    def parse_provience(self, response):
        """
        The lines below is a spider contract. For more info see:
        http://doc.scrapy.org/en/latest/topics/contracts.html

        @url http://www.dmoz.org/Computers/Programming/Languages/Python/Resources/
        @scrapes name
        """
        proviences = response.xpath("//div[@class='conMidtab'][1]/div[@class='conMidtab2']")
        upper = response.xpath("//div[@class='contentboxTab']/h1/a[last()]/text()").extract_first()

        # print "proviences:"+str(proviences)
        # print "upper:"+upper

        for pro in proviences:
            # print pro
            item = RegionItem()
            item['level'] = 3
            item['upper_node'] = upper
            item['name'] = pro.xpath(".//a[@href]/text()").extract_first()
            item['url'] = self.base_url + pro.xpath(".//a[@href][1]/@href").extract_first().strip()
            item['e_name'] = pro.xpath(".//a[@href][1]/@href").extract_first().strip().split("/")[2].split(".")[0]
            yield item


class CountySpider(Spider):
    name = "county"
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
        self.cursor.execute(
            """select region_url_info.url from region_info, region_url_info where 
                region_info.id = region_url_info.region_id and region_info.level=3""")
        urls = self.cursor.fetchall()
        # print urls

        for url in urls:
            yield Request(url=url[0], callback=self.parse)
        self.cursor.close()

    def parse(self, response):
        # 省份
        provience_name = response.xpath("//div[@class='contentboxTab']/h1/a[last()]/text()").extract_first()
        sites = response.xpath("//div[@class='conMidtab'][1]/div[@class='conMidtab3']/table[1]")
        print "provience_name" + provience_name
        # print sites
        for site in sites:
            i = 0
            city_name = ""
            counties = site.xpath("./tr")
            for county in counties:
                # print county.extract()
                item = RegionItem()
                item['name'] = county.xpath(".//a[@href]/text()").extract_first()
                item['url'] = county.xpath(".//a[@href]/@href").extract_first().strip()
                item['code'] = item['url'].split("/")[4].split(".")[0]
                if i == 0:
                    city_name = county.xpath("td/text()").extract_first()
                    item['upper_node'] = provience_name
                    item['level'] = 4
                else:
                    item['upper_node'] = city_name
                    item['level'] = 5
                i = i + 1
                yield item
