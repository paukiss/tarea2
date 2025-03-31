import scrapy


class LostiemposSpider(scrapy.Spider):
    name = "lostiempos"
    allowed_domains = ["lostiempos.com"]
    start_urls = ["https://lostiempos.com"]

    def parse(self, response):
        pass
