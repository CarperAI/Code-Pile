import re
from turtle import position
import pandas as pd
import requests
import logging
from io import StringIO
import os
from tqdm import tqdm
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def read_json_file(json_path: str):
    with open(json_path, "r") as f:
        return [json.loads(line) for line in f]


def write_json_file(json_path: str, obj: dict):
    with open(json_path, "w") as jsonFile:
        json.dump(obj, jsonFile)


class GitHubDiffFilter:
    def __init__(self) -> None:
        repo_file_url = "https://raw.githubusercontent.com/EleutherAI/github-downloader/master/github_repositories.csv"
        repo_file_string = StringIO(requests.get(repo_file_url).text)
        self.single_file_count_threshold = 10_000
        self.save_every = 1000
        if not os.path.isdir("tmp"):
            logger.info(f"Sucessfully added `tmp/` path")
            os.mkdir("tmp")
        self.ckpt_path = os.path.join("tmp", "ckpt.json")

        if os.path.exists(self.ckpt_path):
            self.to_start = read_json_file(self.ckpt_path)["index"]
        else:
            self.to_start = 0
        self.repos_df = pd.read_csv(repo_file_string)
        self.top_repos_list = self.get_top_repo_list()
        self.checkpoint_list = []  # Would contain the curr_index of the last saved file.
        logger.info(f"Size of top repos : {len(self.top_repos_list)}")

    def get_top_repo_list(self) -> list:
        return self.repos_df.iloc[:, 0].values.tolist()

    def get_diff_path_list(self, github_diff_path: str) -> list:
        """
        Get all the files in the given path
        returns :
                github_diff_list (list) : GitHub Diff path.
        """
        return os.listdir(github_diff_path)

    def __call__(self, github_diff_path, output_file_path):
        github_diff_files = self.get_diff_path_list(github_diff_path)
        logger.info(f"Starting to process {len(github_diff_files)} files..")
        # index and stuffs.
        content_output_count = 0
        file_index = 0
        output_list = []
        output_file_index = 0

        # Start_ind
        github_diff_files = github_diff_files[self.to_start:]
        for github_diff_file in tqdm(github_diff_files, total=len(github_diff_files)):
            file_index += 1  # File index.

            if file_index == self.save_every:
                write_json_file(self.ckpt_path, {"index": file_index})
            github_diff_file_path = os.path.join(github_diff_path, github_diff_file)
            github_diff_content_list = read_json_file(github_diff_file_path)
            for ind_content in tqdm(github_diff_content_list, total=len(github_diff_content_list), leave=False):
                if ind_content["repo_name"] in self.top_repos_list:
                    output_list.append(ind_content)
                    content_output_count += 1
                    self.checkpoint_list.append(file_index)

                if content_output_count == self.single_file_count_threshold:
                    # If the number of elements in the list is
                    content_df = pd.DataFrame.from_dict(output_list, orient="columns")
                    output_inter_file_path = os.path.join(output_file_path, str(output_file_index) + ".json")
                    content_df.to_json(output_inter_file_path, orient="records", lines=True)
                    output_file_index += 1

            # Final Chunk goes here..
            write_json_file(self.ckpt_path, {"index": file_index})
            content_df = pd.DataFrame.from_dict(output_list, orient="columns")
            output_final_file_path = os.path.join(output_file_path, str(output_file_index) + ".json")
            content_df.to_json(output_final_file_path, orient="records", lines=True)


if __name__ == "__main__":
    gh_diff_filter = GitHubDiffFilter()
