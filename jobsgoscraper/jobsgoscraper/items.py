# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class JobsgoscraperItem(scrapy.Item):
    
    # url = scrapy.Field()
    # title = scrapy.Field()
    # company_name = scrapy.Field()
    # company_url = scrapy.Field()
    # web = scrapy.Field()

    url = scrapy.Field()
    title=scrapy.Field()
    company_name=scrapy.Field()
    company_url = scrapy.Field()
    time_update =scrapy.Field()
    time_expire =scrapy.Field()
    salary =scrapy.Field()
    exp = scrapy.Field()
    job_level = scrapy.Field()
    group_job = scrapy.Field()
    job_type = scrapy.Field()
    benefit =scrapy.Field()
    job_des = scrapy.Field()
    job_req = scrapy.Field()
    city =scrapy.Field()
    address = scrapy.Field()
    web = scrapy.Field()