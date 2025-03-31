import scrapy


class EldeberSpider(scrapy.Spider):
    name = "eldeber"
    allowed_domains = ["eldeber.com.bo"]
    start_urls = ["https://eldeber.com.bo"]

    def parse(self, response):
        pass
