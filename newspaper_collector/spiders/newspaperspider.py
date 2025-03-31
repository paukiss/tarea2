import scrapy
from newspaper_collector.items import NewspaperItem
import uuid 
from data_web import BASE_URLS, ALLOWED_DOMAINS
import time 

class NewspaperspiderSpider(scrapy.Spider):
    name = "newspaperspider"
    allowed_domains = ALLOWED_DOMAINS
    start_urls = []

    def start_requests(self):
        # Generate initial requests with proper user agent headers.
        for url in BASE_URLS:
            if "trabajito" in url:
                yield scrapy.Request(
                    url=url,
                    callback=self.parse,
                    headers=HEADERS
                )
            else:
                for city in CITIES:
                    # Construye la URL completa
                    full_url = f"{url}{city}"
                    # Realiza la solicitud con encabezados
                    yield scrapy.Request(
                        url=full_url,
                        callback=self.parse,
                        headers=HEADERS,
                        meta={"city": city}
                    )
            time.sleep(1)


    def parse(self, response):
        if response.status != 200:
            self.logger.warning(f"Failed to fetch {response.url}: {response.status}")
            return

        print("Parsing")

        url_site = response.url

        # Extraer ofertas dependiendo del sitio
        if "trabajando" in url_site:
            job_posting = response.css('div.views-row')
            for job in job_posting:
                relative_url = job.css('h2.views-field-title a::attr(href)').get()
                job_description_url = f'https://www.trabajando.com.bo{relative_url}'
                yield response.follow(
                    job_description_url,
                    callback=self.parse_pagina_especifica
                )

        elif "trabajito" in url_site:
            job_posting = response.css('div.job-block')
            for job in job_posting:
                relative_url = job.css('div.inner-box div.content h4 a::attr(href)').get()
                job_description_url = relative_url
                yield response.follow(
                    job_description_url,
                    callback=self.parse_pagina_especifica
                )

        # Paginación
        next_page = ''
        if "trabajito" in url_site:
            next_page = response.css('div.ls-pagination ul.pagination.bravo-pagination li')
            last_li_class = next_page[-1].css('::attr(class)').get()

            if not last_li_class:
                next_page = next_page[-1].css('a::attr(href)').get()
            else:
                print("End pagination")

        yield response.follow(next_page, callback=self.parse)


    def parse_pagina_especifica(self, response):
        job_item = JobItem()
        url_site = response.url

        if "trabajando" in url_site:
            wrapper = response.css('div.region-content')

            job_item["data_id"] = str(uuid.uuid4())
            job_item["url"] = response.url
            job_item["title"] = wrapper.css('h1.trabajando-page-header span::text').get()
            job_item["company"] = wrapper.css('div.views-field-field-nombre-empresa div.field-content a::text').get()
            job_item["location"] = wrapper.css('div.views-field-field-ubicacion-del-empleo div.field-content::text').get()
            job_item["type_job"] = wrapper.css('div.views-field-field-tipo-empleo div.field-content::text').get()
            job_item["date_published"] = wrapper.css('div.views-field-created span.field-content time::attr(datetime)').get()
            job_item["date_expiration"] = wrapper.css('div.views-field-fecha-empleo-1 div.field-content::text').get()
            job_item["job_description"] = wrapper.css('div.field--type-text-with-summary div.field--item p::text').getall()
            job_item["date_saved"] = datetime.now().isoformat()

            yield job_item

        elif "trabajito" in url_site:
            wrapper = response.css('section.job-detail-section')
            job_desc = wrapper.css('ul.job-info li')
            job_overview = wrapper.css('aside.sidebar ul.job-overview li')

            job_item["data_id"] = str(uuid.uuid4())
            job_item["url"] = response.url
            job_item["title"] = wrapper.css('h4::text').get()
            job_item["company"] = wrapper.css('div.widget-content div.company-title h5::text').get()
            job_item["location"] = job_desc[1].css('li::text').get()
            job_item["type_job"] = job_desc[2].css('li::text').get()
            job_item["job_description"] = wrapper.css('div.detail-content div.only-text p::text').getall()
            job_item["date_published"] = job_desc[0].css('li::text').get()
            job_item["date_expiration"] = job_overview[1].css('li span::text').get()
            job_item["date_saved"] = datetime.now().isoformat()

            yield job_item
