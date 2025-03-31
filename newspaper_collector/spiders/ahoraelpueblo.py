import scrapy


class AhoraelpuebloSpider(scrapy.Spider):
    name = "ahoraelpueblo"
    allowed_domains = ["ahoraelpueblo.bo"]
    start_urls = ["https://ahoraelpueblo.bo"]

    def parse(self, response):
        pass
