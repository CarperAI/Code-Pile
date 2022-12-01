* This Scrapy crawler is not production-grade implementation and may not be following best practices.
* This uses Github graphql endpoint to fetch data. API lets us get upto 100 issues along with other metadata(labels, comments, author etc) in a single request.
* When getting issues+comments+other metadata, API is returning something called secondary rate limits even when we haven't breached to 5k/hour request limit. It is not entirely clear on how to mitigate this.
* Graphql requests and responses can be explored here https://docs.github.com/en/graphql/overview/explorer
* Refer the standalone scripts "extract-*" to convert the raw graphql responses into a flat list of issues/comments.

Run scrapy spider using a command like this `python run.py 0|1|2...` 