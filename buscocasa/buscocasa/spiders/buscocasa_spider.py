from datetime import date

from scrapy import Spider
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor


class BuscocasaParseSpider(Spider):
    name = "buscocasa-parse"

    user_table_css = ".taula-usuari tr"
    detail_table_css = ".uk-table:contains('Detalls')"
    characteristics_css = ".fitxa-dreta:contains('Caracteristiques')"

    total_characteristics = ['1 sol propietari', 'Accepten animals', 'Aire condicionat', 'Antena Satelit',
                            'Armaris Encastrats', 'Ascensor', 'Assolellat', 'Box Tancat', 'Calefacció',
                            'Cèntric', 'Cuina Americana', 'Cuina Equipada', 'Domòtica', 'Dutxa Hidromassatge',
                            'Jacuzzi', 'Jardí', 'Llar de Foc', 'Moblat', 'Nou per estrenar', 'Parquet',
                            'Piscina', 'Sistema Alarma', 'Terrassa', 'Traster', 'Vistes agradables']

    def parse(self, response):
        attr = {}

        attr["REF"] = self.ref(response)
        attr["Title"] = self.title(response)
        attr["Location"] = self.location(response)
        attr["User_Id"] = self.user_id(response)
        attr["User_Name"] = self.user_name(response)
        attr["User_Phone"] = self.user_phone(response)
        attr["User_Email"] = self.user_email(response)
        attr["User_Website"] = self.user_website(response)
        attr["User_Address"] = self.user_address(response)
        attr["Description"] = self.description(response)
        attr["Price"] = self.price(response)
        attr["Currency"] = self.currency(response)
        attr["image_urls"] = self.images(response)
        attr["Views"] = self.views(response)
        attr["Favorite"] = self.favorite(response)
        attr["Date_Modified"] = self.date_modified(response)
        attr["Scrapped_Date"] = self.scrapped_date()
        attr["URL"] = response.url
        attr["UNPUBLISHED"] = 0
        attr["SOLD_OUT"] = 0

        self.details(response, attr)
        self.characteristics(response, attr)
        self.trail(response, attr)

        return attr

    def ref(self, response):
        return self.clean(response.css("[name='idItem']::attr(value)").get())

    def title(self, response):
        return self.clean(response.css(".uk-block h2::text").get())

    def location(self, response):
        return self.clean(response.css(".fitxa-titol h1::text").get())

    def user_id(self, response):
        return self.clean(response.css("[name='idUserAnunci']::attr(value)").get())

    def user_name(self, response):
        return self.user_table_element(response, " .uk-icon-user").replace("Contactar amb ",  "")

    def user_phone(self, response):
        return self.user_table_element(response, " .uk-icon-whatsapp", 1)

    def user_email(self, response):
        return self.user_table_element(response, " .uk-icon-envelope", 1)

    def user_website(self, response):
        return self.user_table_element(response, " .uk-icon-globe", 1)

    def user_address(self, response):
        return self.user_table_element(response, " .uk-icon-map-pin")

    def description(self, response):
        return " ".join(self.clean(response.css(".uk-block ::text").getall()))

    def price(self, response):
        raw_price = self.clean(response.css(".uk-clearfix .uk-text-primary::text").get())

        return self.decimal_clean(raw_price.split(" ")[0])

    def currency(self, response):
        return self.clean(response.css(".uk-clearfix .uk-text-primary::text").get().split(" ")[-1])

    def details(self, response, attr):
        table = response.css(self.detail_table_css)
        head_css = "td::text"
        value_css = "td:last-child::text"

        for row in table.css("tr"):
            attr[self.clean(row.css(head_css).get())] = self.decimal_clean(row.css(value_css).get())

    def characteristics(self, response, attr):
        attr.update({chara: 0 for chara in self.total_characteristics})

        table = response.css(self.characteristics_css)

        for row in table.css("ul li"):
            header = self.clean(row.css("::text").get())
            value = 1 if row.css(".uk-icon-check-square") else 0

            attr[header] = value

    def user_table_element(self, response, css, anchor=0):
        attribute = "a" if anchor else "td:last-child"

        return "".join(self.clean([resp.css(f"{attribute}::text").get()
                                   for resp in response.css(self.user_table_css) if resp.css(css)]))

    def images(self, response):
        css = ".uk-thumbnav:last-child li a::attr(href)"
        return response.css(css).getall()

    def views(self, response):
        symbol = "VISITES"
        return self.decimal_clean("".join([self.clean(foot.replace(symbol, ""))
                                  for foot in response.css(".box-footer ::text").getall() if symbol in foot]))

    def favorite(self, response):
        symbol = "FAVORITS"
        return self.decimal_clean("".join([self.clean(foot.replace(symbol, ""))
                                  for foot in response.css(".box-footer ::text").getall() if symbol in foot]))

    def date_modified(self, response):
        symbol = "MODIFICAT"
        return "".join([self.clean(foot.replace(symbol, "")).strip(":").strip()
                        for foot in response.css(".box-footer ::text").getall() if symbol in foot])

    def scrapped_date(self):
        return str(date.today())

    def trail(self, response, attr):
        columns = ["PAIS", "POB", "QUI", "QUE", "COM"]
        trails = self.clean(response.css(".fitxa-dreta .uk-alert:nth-child(1) ::text").getall())

        for ind, col in enumerate(columns):
            attr[col] = trails[ind] if ind < len(trails) else ''

    def clean(self, raw_text):
        if isinstance(raw_text, list):
            return list(filter(None, [self.clean(raw) for raw in raw_text]))

        return str(raw_text).strip()

    def decimal_clean(self, raw_text):
        raw_text = str(raw_text).replace('.', '').strip()

        if ',' in raw_text:
            raw_text = int(round(float(raw_text.replace(',', '.').strip())))

        return raw_text


class BuscocasaCrawlSpider(CrawlSpider):
    name = "buscocasa-crawl"

    start_urls = ["https://www.buscocasa.ad/ca/darrersanuncis?&pn=1"]

    custom_settings = {
        "FEED_EXPORT_ENCODING": "utf-8",
        
    }

    listing_css = [".uk-pagination li a"]
    leads_css = [".data-uk-grid-match li a"]

    parse_spider = BuscocasaParseSpider()
    
    rules = (
        Rule(LinkExtractor(restrict_css=listing_css)),
        Rule(LinkExtractor(restrict_css=leads_css), callback=parse_spider.parse),
    )
