from scrapy.crawler import CrawlerProcess

from gh_graphql.spiders import issues

process = CrawlerProcess({

})

process.crawl(issues.IssuesSpider)
process.start() #