from scrapy.http import Request
import scrapy
import pandas as pd
from random import randint
import json
import os, sys
import hashlib
import time

# Loads a list of repos and makes requests to Github GraphQL API to get data
# Does pagination of top level issues
# keeps track of the information on what has been already fetched in json files so that scraping can be resumed (TODO: there is probably a better, scrapy native way of tracking and resuming)
# Multiple github tokens can be configured and they can be selected when launching scrapy via run.py. Originally, it was implemented in such a way that tokens are selected automatically, but because of the way scrapy schedules the requests there is a chance that same token can be used concurrently across two reqeusts which could lead to some rate limiting.

class IssuesSpider(scrapy.Spider):
    custom_settings = {
      'CONCURRENT_ITEMS': 1,
      'CONCURRENT_REQUESTS': 1,
      'DOWNLOAD_DELAY': .7
    }      
    name = 'issues'
    allowed_domains = ['api.github.com']
    start_urls = ['http://api.github.com/']
    repo_list = "/data/github-issues_pre-2015-issues-without-content.parquet"
    graphql_query_path = "./issues.graphql"
    issue_query_template = open(graphql_query_path).read()
    graphql_api_url = "https://api.github.com/graphql"
    github_access_tokens = [
        {"token": "abcd", "id": "username"},
        {"token": "efgh", "id": "username2"}
    ]
    output_path = f"/data/{name}-{sys.argv[1]}.jsonl"
    track_file = f"/data/{name}-track-{sys.argv[1]}.jsonl"
    error_file = f"/data/{name}-error-{sys.argv[1]}.jsonl"

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.prepare()

    def prepare(self):
        self.output_writer = open(self.output_path,'a+')
        self.track_writer = open(self.track_file, 'a+')
        self.error_writer = open(self.error_file, "a+")
        self.finished_in_previous_runs = {}
        with open(self.track_file, 'r') as track_fo:
            tracked_list = track_fo.readlines()
            for item in tracked_list:
                item_o = json.loads(item)
                track_item = {
                    "owner": item_o['owner'],
                    "name": item_o['name'],
                    "page": item_o['page']
                }
                track_hash = self.get_hash_for_dict(track_item)
                self.finished_in_previous_runs[track_hash] = item_o

    def get_hash_for_dict(self, d):
        dhash = hashlib.md5()
        encoded = json.dumps(d, sort_keys=True).encode()
        dhash.update(encoded)
        return dhash.hexdigest()        

    def start_requests(self):
        print(sys.argv[1])
        df = pd.read_parquet(self.repo_list)
        df.sort_values(by="repo_stars", ascending=False, inplace=True)
        df.reset_index(level=0, inplace=True)
        no_of_tokens = len(self.github_access_tokens)
        shard = int(sys.argv[1])
        for index, url in df['repo_url'][shard::no_of_tokens].items():
            repo_part = url.replace("https://github.com/", "")
            if len(repo_part.split("/"))  != 2:
                print(repo_part)
            repo_owner, repo_name = repo_part.split("/")
            variables = {
                    "repo_owner": repo_owner,
                    "repo_name": repo_name,
                    "page_size": 100
                }
            req_body = {
                "query": self.issue_query_template,
                "variables": variables
            }
            meta = {"variables": variables, "page": 0, "referrer_policy": "no-referrer"}
            # remainder = index % len(self.github_access_tokens)
            meta['token_idx'] = shard
            request = self.get_request(req_body, meta)
            if request:
                yield request

    def parse(self, response):
        res_json = response.json()
        if "errors" in res_json and len(res_json['errors']) > 0:
            print("Graphql errors found")
            print(res_json['errors'])
            raise Exception("graphql error")
        
        
        self.output_writer.write(response.text + "\n")
        track_content = {
            "owner": response.meta.get("variables")['repo_owner'],
            "name": response.meta.get("variables")['repo_name'],
            "page": response.meta.get("page")
        }
        
        issues = res_json['data']['repository']['issues']
        if issues['pageInfo']['hasNextPage']:
            end_cursor = issues['pageInfo']['endCursor']
            track_content["next_cursor"] = end_cursor
            next_request = self.get_next_page_request(response.meta, end_cursor)
            if next_request:
                yield next_request
        self.track_writer.write(json.dumps(track_content) + "\n")

    def get_request(self, req_body, meta, nested_call=False):
        if not nested_call:
            track_item = {
                "owner": meta.get("variables")['repo_owner'],
                "name": meta.get("variables")['repo_name'],
                "page": meta.get("page")
            }
            track_hash = self.get_hash_for_dict(track_item)
            while track_hash in self.finished_in_previous_runs:
                print(f"Already fetched in previous run: {track_item}")
                stored_track_item = self.finished_in_previous_runs[track_hash]
                if "next_cursor" in stored_track_item:
                    # check if next page is already scraped and proceed accordingly
                    print(f"next page available: {stored_track_item}")
                    track_item['page'] = track_item['page'] + 1
                    track_hash = self.get_hash_for_dict(track_item)
                    req_body['variables']['after_cursor'] = stored_track_item['next_cursor']
                    meta['variables'] = req_body['variables']
                    meta['page'] = track_item['page']
                else:
                    return None

        headers = self.get_req_headers(meta)
        print(json.dumps(meta))
        return Request(method="POST", url=self.graphql_api_url, body=json.dumps(req_body), headers=headers, meta=meta, errback=self.error_callback)

    def get_next_page_request(self, meta, next_page_cursor):
        variables = meta.get("variables").copy()
        new_meta = {"variables": variables, "referrer_policy": "no-referrer", "token_idx": meta.get("token_idx")}
        new_meta["page"] = meta.get("page") + 1
        variables['after_cursor'] = next_page_cursor
        req_body = {
            "query": self.issue_query_template,
            "variables": variables
        }        
        return self.get_request(req_body, new_meta, nested_call=True)
    
    def get_req_headers(self, meta):
        token_idx = meta.get("token_idx")
        if token_idx:
            token = self.github_access_tokens[token_idx]['token']
            user_id = self.github_access_tokens[token_idx]['id']
        else:
            rand_idx = randint(0, len(self.github_access_tokens) - 1)
            token = self.github_access_tokens[rand_idx]['token']
            user_id = self.github_access_tokens[rand_idx]['id']
        headers = {
            "Authorization": "token " + token,
            "User-Agent": user_id
        }
        return headers

    def close(self, spider, reason):
        self.output_writer.close()
        self.track_writer.close()
        self.error_writer.close()
        super().close(spider, reason)

    def error_callback(self, error):
        print(f"Request failed: {error.value.response}")
        if int(error.value.response.status) == 403:
            # TODO: This probably isn't working as expected
            time.sleep(1)
            no_of_retries = error.value.response.request.meta.get("retries", 0)
            if no_of_retries < 5:
                request_o = error.value.response.request
                request_o.meta['retries'] = no_of_retries + 1
                print(f"retrying request: {error.value.response.request.meta}") 
                yield error.value.response.request
        failed_reason = error.value.response.text
        failed_variables = json.loads(error.value.response.request.body)['variables']
        failed_variables["reason"] = failed_reason
        failed_variables['page'] = error.value.response.request.meta.get("page")
        v_str = json.dumps(failed_variables) + "\n"
        self.error_writer.write(v_str)
        print(json.dumps(failed_reason))
        return None
