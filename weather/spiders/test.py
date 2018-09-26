import codecs

from scrapy import Spider


class SaveSpider(Spider):
    name = "test"
    allowed_domains = ["weather.com.cn"]
    start_urls = [
        "http://www.weather.com.cn/weathern/101011100.shtml",
    ]

    def parse(self, response):
        files = codecs.open('detail.html', 'w', encoding='utf-8')
        files.write(response.body_as_unicode())
        files.close()
        pass
