import scrapy
from newspaper_collector.items import NewspaperItem
import uuid
import time
from datetime import datetime


class NewspaperSpider(scrapy.Spider):
    name = "newspaper_spider"
    allowed_domains = ["eldeber.com.bo", "lostiempos.com"]

    def start_requests(self):
        # Lanzamos las URLs iniciales para cada periódico
        # Ajusta o agrega más URLS según tu necesidad
        # - Eldeber comenzando en la página 1
        # - LosTiempos en la sección de "últimas noticias"
        yield scrapy.Request(
            url="https://eldeber.com.bo/economia/1",
            callback=self.parse,
            meta={'page': 1, 'source': 'eldeber'}
        )
        yield scrapy.Request(
            url="https://www.lostiempos.com/ultimas-noticias",
            callback=self.parse,
            meta={'page': 1, 'source': 'lostiempos'}
        )
        time.sleep(1)

    def parse(self, response):
        """
        Se verifica si la URL proviene de Eldeber o de Los Tiempos
        y se llama a la función correspondiente.
        """
        if response.status != 200:
            self.logger.warning(f"Failed to fetch {response.url}: {response.status}")
            return
        source = response.meta.get("source", "")

        if "eldeber" in source or "eldeber" in response.url:
            yield from self.parse_eldeber(response)
        elif "lostiempos" in source or "lostiempos" in response.url:
            yield from self.parse_lostiempos(response)

    def parse_eldeber(self, response):
        """
        Lógica de parseo específica para Eldeber.
        Extrae noticias y controla la paginación hasta la página 50.
        """
        page = response.meta.get("page", 1)

        # Selecciona los bloques de noticia
        noticias = response.xpath('//div[contains(@class, "titulo-teaser-2col")]/a/h2/ancestor::article')

        for noticia in noticias:
            item = NewspaperItem()

            titulo = noticia.xpath('.//div[contains(@class, "titulo-teaser-2col")]/a/h2/text()').get(default="").strip()
            descripcion = noticia.xpath('.//div[contains(@class, "entradilla-teaser-2col")]/div/p/text()').get(default="").strip()
            fecha = noticia.xpath('.//div[contains(@class, "fecha-teaser-2col")]/div/time/text()').get(default="").strip()
            seccion = noticia.xpath('.//div[contains(@class, "seccion-teaser-2col")]/div/div/a/text()').get(default="").strip()
            relative_url = noticia.xpath('.//div[contains(@class, "titulo-teaser-2col")]/a/@href').get(default="")

            item["data_id"] = str(uuid.uuid4())
            item["titulo"] = titulo
            item["descripcion"] = descripcion
            item["fecha"] = fecha
            item["seccion"] = seccion
            item["url"] = response.urljoin(relative_url)
            item["date_saved"] = datetime.now().isoformat()

            yield item

        # Control de paginación
        if page < 5:
            next_page = page + 1
            next_page_url = f"https://eldeber.com.bo/economia/{next_page}"
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse,
                meta={'page': next_page, 'source': 'eldeber'}
            )

    def parse_lostiempos(self, response):
        """
        Lógica de parseo específica para Los Tiempos.
        Extrae las noticias y controla la paginación hasta la página 8 (por ejemplo).
        """
        page = response.meta.get("page", 1)

        # Extrae los bloques de noticias
        noticias = response.xpath('//section[contains(@class, "pane-views-panes")]//div[contains(@class, "views-row")]')

        for noticia in noticias:
            item = NewspaperItem()

            titulo = noticia.xpath('.//div[contains(@class, "views-field-title term")]/a/text()').get(default="Sin título").strip()
            resumen = noticia.xpath('.//div[contains(@class, "views-field-field-noticia-sumario")]/span/text()').get(default="Sin resumen").strip()
            fecha = noticia.xpath('.//span[contains(@class, "views-field-field-noticia-fecha")]/span/text()').get(default="Sin fecha").strip()
            seccion = noticia.xpath('.//span[contains(@class, "views-field-seccion")]/span/a/text()').get(default="Sin sección").strip()
            url_noticia = noticia.xpath('.//div[contains(@class, "views-field-title term")]/a/@href').get(default="")

            item["data_id"] = str(uuid.uuid4())
            item["titulo"] = titulo
            item["descripcion"] = resumen  # unificamos 'resumen' en el campo 'descripcion'
            item["fecha"] = fecha
            item["seccion"] = seccion
            item["url"] = response.urljoin(url_noticia)
            item["date_saved"] = datetime.now().isoformat()

            yield item

        # Paginación para Los Tiempos
        if page < 8:
            next_page_href = response.xpath('//li[contains(@class, "pager-next")]/a/@href').get()
            if next_page_href:
                next_page_url = response.urljoin(next_page_href)
                # Simplemente incrementamos el contador de página
                yield scrapy.Request(
                    url=next_page_url,
                    callback=self.parse,
                    meta={'page': page + 1, 'source': 'lostiempos'}
                )
