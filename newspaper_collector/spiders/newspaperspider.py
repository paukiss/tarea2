# newspaper_spider.py

import scrapy
from newspaper_collector.items import NewspaperItem 
import uuid
from datetime import datetime
import logging

from newspaper_collector.spiders.constants import ( 
    ALLOWED_DOMAINS,
    ELDEBER_SECTIONS, ELDEBER_PAGES_TO_SCRAPE,
    LOSTIEMPOS_START_URL, LOSTIEMPOS_PAGES_TO_SCRAPE,
    AHORAELPUEBLO_SECTIONS, AHORAELPUEBLO_PAGE_INCREMENT, AHORAELPUEBLO_PAGES_TO_SCRAPE,
    HEADERS 
)

class NewspaperSpider(scrapy.Spider):
    name = "newspaper_spider"
    allowed_domains = ALLOWED_DOMAINS

    def start_requests(self):

        for section, url_pattern in ELDEBER_SECTIONS.items():
            start_url = url_pattern.format(page=1)
            logging.info(f"Iniciando El Deber - Sección: {section}, URL: {start_url}")
            yield scrapy.Request(
                url=start_url,
                callback=self.parse,
                meta={
                    'page': 1,
                    'source': 'eldeber',
                    'section': section,
                    'url_pattern': url_pattern 
                },
                errback=self.handle_error
            )

        logging.info(f"Iniciando Los Tiempos - URL: {LOSTIEMPOS_START_URL}")
        yield scrapy.Request(
            url=LOSTIEMPOS_START_URL,
            callback=self.parse,
            meta={'page': 1, 'source': 'lostiempos'},
            headers=HEADERS,
            errback=self.handle_error
        )

        start_value = 0 
        for section, url_pattern in AHORAELPUEBLO_SECTIONS.items():
            start_url = url_pattern.format(start=start_value)
            logging.info(f"Iniciando Ahora El Pueblo - Sección: {section}, URL: {start_url}")
            yield scrapy.Request(
                url=start_url,
                callback=self.parse,
                meta={
                    'page': 1,
                    'start_value': start_value,
                    'source': 'ahoraelpueblo',
                    'section': section,
                    'url_pattern': url_pattern 
                },
                headers=HEADERS, 
                errback=self.handle_error
            )

    def handle_error(self, failure):
        self.logger.error(f"Request failed: {failure.request.url} - {failure.value}")

    def parse(self, response):
        if response.status != 200:
            self.logger.warning(f"Respuesta no exitosa ({response.status}) para {response.url}")
            return 

        source = response.meta.get("source", "")
        try:
            if source == 'eldeber':
                yield from self.parse_eldeber(response)
            elif source == 'lostiempos':
                yield from self.parse_lostiempos(response)
            elif source == 'ahoraelpueblo':
                yield from self.parse_ahoraelpueblo(response)
            else:
                self.logger.warning(f"Fuente desconocida o no manejada para URL: {response.url}")
        except Exception as e:
            self.logger.error(f"Error procesando {source} - {response.url}: {e}", exc_info=True)

    def parse_eldeber(self, response):

        page = response.meta['page']
        section = response.meta['section']
        url_pattern = response.meta['url_pattern']
        source = response.meta['source']

        self.logger.info(f"Parseando El Deber - Sección: {section}, Página: {page}, URL: {response.url}")

        noticias = response.xpath('//div[contains(@class, "titulo-teaser-2col")]/a/h2/ancestor::article')
        news_found_count = 0
        for noticia in noticias:
            try:
                item = NewspaperItem()

                titulo = noticia.xpath('.//div[contains(@class, "titulo-teaser-2col")]/a/h2/text()').get(default="").strip()
                descripcion = " ".join(noticia.xpath('.//div[contains(@class, "entradilla-teaser-2col")]//text()').getall()).strip()
                fecha = noticia.xpath('.//div[contains(@class, "fecha-teaser-2col")]/div/time/text()').get(default="").strip()
                relative_url = noticia.xpath('.//div[contains(@class, "titulo-teaser-2col")]/a/@href').get(default="")

                if not titulo or not relative_url: # Validar que se extrajo lo mínimo
                    self.logger.warning(f"Datos incompletos en noticia de {response.url}")
                    continue

                item["data_id"] = str(uuid.uuid4())
                item["source"] = source
                item["titulo"] = titulo
                item["descripcion"] = descripcion
                item["fecha"] = fecha
                item["seccion"] = section # Usamos la sección de la URL procesada
                item["url"] = response.urljoin(relative_url)
                item["date_saved"] = datetime.now().isoformat()

                yield item
                news_found_count += 1
            except Exception as e:
                 self.logger.error(f"Error procesando item de El Deber ({response.url}): {e}", exc_info=True)

        self.logger.info(f"Encontradas {news_found_count} noticias en El Deber - Sección: {section}, Página: {page}")

        # --- Paginación El Deber ---
        if page < ELDEBER_PAGES_TO_SCRAPE:
            next_page = page + 1
            next_page_url = url_pattern.format(page=next_page)
            self.logger.info(f"Solicitando siguiente página El Deber - Sección: {section}, Página: {next_page}, URL: {next_page_url}")
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse,
                meta={
                    'page': next_page,
                    'source': source,
                    'section': section,
                    'url_pattern': url_pattern
                },
                headers=HEADERS, 
                errback=self.handle_error
            )

    def parse_lostiempos(self, response):
        page = response.meta['page']
        source = response.meta['source']
        self.logger.info(f"Parseando Los Tiempos - Página: {page}, URL: {response.url}")

        noticias = response.xpath('//section[contains(@class, "pane-views-panes")]//div[contains(@class, "views-row")]')
        news_found_count = 0
        for noticia in noticias:
             try:
                item = NewspaperItem()

                titulo = noticia.xpath('.//div[contains(@class, "views-field-title term")]/a/text()').get(default="").strip()
                resumen = noticia.xpath('.//div[contains(@class, "views-field-field-noticia-sumario")]/span/text()').get(default="").strip()
                fecha = noticia.xpath('.//span[contains(@class, "views-field-field-noticia-fecha")]/span/text()').get(default="").strip()
                seccion = noticia.xpath('.//span[contains(@class, "views-field-seccion")]/span/a/text()').get(default="Ultimas Noticias").strip() 
                url_noticia = noticia.xpath('.//div[contains(@class, "views-field-title term")]/a/@href').get(default="")

                if not titulo or not url_noticia:
                    self.logger.warning(f"Datos incompletos en noticia de {response.url}")
                    continue

                item["data_id"] = str(uuid.uuid4())
                item["source"] = source
                item["titulo"] = titulo
                item["descripcion"] = resumen
                item["fecha"] = fecha
                item["seccion"] = seccion
                item["url"] = response.urljoin(url_noticia)
                item["date_saved"] = datetime.now().isoformat()

                yield item
                news_found_count += 1
             except Exception as e:
                 self.logger.error(f"Error procesando item de Los Tiempos ({response.url}): {e}", exc_info=True)

        self.logger.info(f"Encontradas {news_found_count} noticias en Los Tiempos - Página: {page}")

        if page < LOSTIEMPOS_PAGES_TO_SCRAPE:
            next_page_href = response.xpath('//li[contains(@class, "pager-next")]/a/@href').get()
            if next_page_href:
                next_page_url = response.urljoin(next_page_href)
                self.logger.info(f"Solicitando siguiente página Los Tiempos - Página: {page + 1}, URL: {next_page_url}")
                yield scrapy.Request(
                    url=next_page_url,
                    callback=self.parse,
                    meta={'page': page + 1, 'source': source},
                    errback=self.handle_error
                )
            else:
                 self.logger.info(f"No se encontró enlace a siguiente página en Los Tiempos - Página: {page}")


    def parse_ahoraelpueblo(self, response):
        page = response.meta['page'] 
        start_value = response.meta['start_value']
        section = response.meta['section']
        url_pattern = response.meta['url_pattern']
        source = response.meta['source']

        self.logger.info(f"Parseando Ahora El Pueblo - Sección: {section}, Página conceptual: {page} (start={start_value}), URL: {response.url}")

        noticias = response.xpath('//div[@class="article-list"]//div[@itemprop="blogPost"]')
        if not noticias:
             noticias = response.xpath('//*[@id="sp-component"]/div/div[2]/div[2]/div/div/div')

        news_found_count = 0
        for noticia in noticias:
            try:
                item = NewspaperItem()

                titulo_elem = noticia.xpath('.//h2[@itemprop="name"]/a/text()') 
                if not titulo_elem:
                    titulo_elem = noticia.xpath('.//div[2]/div[1]/h2/a/text()') 

                desc_elem = noticia.xpath('.//div[@itemprop="description"]//text()') 
                if not desc_elem:
                     desc_elem = noticia.xpath('.//div[contains(@class, "article-introtext")]//text()') 

                fecha_elem = noticia.xpath('.//time[@itemprop="datePublished"]/@datetime') 
                if not fecha_elem:
                    fecha_elem = noticia.xpath('.//time/@datetime') 

                seccion_elem = noticia.xpath('.//a[@itemprop="genre"]/text()') 
                if not seccion_elem:
                    seccion_elem = noticia.xpath('.//div[2]/div[2]/span[1]/a/text()') 

                url_elem = noticia.xpath('.//h2[@itemprop="name"]/a/@href')
                if not url_elem:
                    url_elem = noticia.xpath('.//div[2]/div[1]/h2/a/@href')


                titulo = titulo_elem.get(default="").strip()
                relative_url = url_elem.get()
                descripcion = " ".join(desc_elem.getall()).strip()
                fecha = fecha_elem.get()

                if not titulo or not relative_url:
                    self.logger.warning(f"Datos incompletos en noticia de {response.url}")
                    continue

                item["data_id"] = str(uuid.uuid4())
                item["source"] = source
                item["titulo"] = titulo
                item["descripcion"] = descripcion
                item["fecha"] = fecha
                item["seccion"] = section 
                item["url"] = response.urljoin(relative_url)
                item["date_saved"] = datetime.now().isoformat()

                yield item
                news_found_count += 1

            except Exception as e:
                self.logger.error(f"Error procesando item de Ahora El Pueblo ({response.url}): {e}", exc_info=True)

        self.logger.info(f"Encontradas {news_found_count} noticias en Ahora El Pueblo - Sección: {section}, Página: {page}")

        if page < AHORAELPUEBLO_PAGES_TO_SCRAPE:
            next_page = page + 1
            next_start_value = start_value + AHORAELPUEBLO_PAGE_INCREMENT
            next_page_url = url_pattern.format(start=next_start_value)

            self.logger.info(f"Solicitando siguiente página Ahora El Pueblo - Sección: {section}, Página conceptual: {next_page} (start={next_start_value}), URL: {next_page_url}")
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse, 
                meta={
                    'page': next_page,
                    'start_value': next_start_value,
                    'source': source,
                    'section': section,
                    'url_pattern': url_pattern
                },
                errback=self.handle_error
            )