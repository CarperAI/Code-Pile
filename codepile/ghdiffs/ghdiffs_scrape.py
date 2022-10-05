import json
import urllib.request
from pathlib import Path
from pprint import pprint

import dask.bag as db
import pyarrow as pa
from codepile.dataset import (Analyser, Dataset, DatasetInfo, DatasetSources,
                              Processor, RawDataset, Scraper)
from dask.distributed import Client
from unidiff import PatchSet


def process_ind_patch(patch_diff) -> dict:
    """Process patch to get diff data."""
    patch_parsed_diff: dict = {
        "hunks": [],
    }

    patch_parsed_diff["addition_count"] = patch_diff.added
    patch_parsed_diff["deletion_count"] = patch_diff.removed
    patch_parsed_diff["src_file"] = patch_diff.source_file
    patch_parsed_diff["tgt_file"] = patch_diff.target_file
    # patch_parsed_diff["patch_info"] = patch_diff.patch_info
    patch_diff_list = list(patch_diff)
    for patch_diff_ind in patch_diff_list:
        patch_diff_ind = str(patch_diff_ind)
        patch_diff_split = patch_diff_ind.split("@@")
        patch_diff_line = patch_diff_split[2].split("\n")
        patch_diff_line_numbers = [list(map(int, hunk.strip("-+").split(",")))
                                   for hunk in patch_diff_split[1].strip().split(" ")]
        patch_parsed_diff["hunks"].append(patch_diff_line_numbers + patch_diff_line[:-1])
    return patch_parsed_diff


def patch_parse(commit_hash: str, repo_name: str) -> list:
    """Parse a commit to get diff data."""
    diff_url = (f"https://github.com/{repo_name}/commit/{commit_hash}.diff")
    diff = urllib.request.urlopen(diff_url)
    encoding = diff.headers.get_charsets()[0]
    patch = PatchSet(diff, encoding=encoding)
    diff_list: list = []
    for patch_ind in patch:
        # Skip if the file is not a python file.
        # if not patch_ind.target_file.endswith(".py"):
        #     continue
        diff_list.append(process_ind_patch(patch_ind))
    return diff_list


def apply_reverse_patch(diff_list: list, commit_hash: str, repo_name: str, length_threshold: int = 4096) -> list:
    """Apply reverse patch to get before files. Returns list of modified files."""
    files_list: list = []
    repo_owner, repo_name = repo_name.split("/")
    for diff in diff_list:
        if diff["src_file"] == "/dev/null":
            files_list.append([])
            continue
        # Get raw after file.
        file_raw_url = (f"https://raw.githubusercontent.com/{repo_owner}/"
                        f"{repo_name}/{commit_hash}/{diff['tgt_file'][2:]}")
        raw_file = urllib.request.urlopen(file_raw_url)
        raw_file_encoding = raw_file.headers.get_charsets()[0]
        raw_file = [line.decode(raw_file_encoding) for line in raw_file.readlines()]
        # file_length = sum(1 for _ in raw_file)
        # if file_length < length_threshold:
        files_list.append(raw_file)
        # Iterate over hunks for this file and apply the reverse patch.
        for hunk in diff["hunks"]:
            hunk_list = []
            for line in hunk[3:]:
                if line.startswith("-") or line.startswith(" "):
                    hunk_list.append(line[1:] + "\n")
            files_list[-1][hunk[0][0] - 1:hunk[0][0] + hunk[1][1] - 1] = hunk_list

    return files_list


def process_commit(commit_data: dict) -> dict:
    """Process a commit hash and repo name to get the before files and diff dict."""
    # Get dict containing diff data.
    diff_list = patch_parse(commit_data["commit"], commit_data["repo_name"])
    # Get list of files, each of which is a list of strings, one for each line.
    files_list = apply_reverse_patch(diff_list, commit_data["commit"], commit_data["repo_name"])
    commit_data["before_files"] = files_list
    commit_data["diff"] = [json.dumps(diff) for diff in diff_list]
    return commit_data


class GitHubDiffDataset(Dataset):
    def __init__(self, config):
        pass


class GitHubDiffScraper(Scraper):
    def __init__(self, config):
        pass

    def scrape(self):
        pass


if __name__ == "__main__":
    read_path = Path(__file__).parent / "test_file.json"
    client = Client(n_workers=8, threads_per_worker=2)
    schema = pa.schema([
        (pa.field("commit", pa.string())),
        (pa.field("message", pa.string())),
        (pa.field("repo_name", pa.string())),
        (pa.field("before_files", pa.list_(pa.list_(pa.string())))),
        (pa.field("diff", pa.list_(pa.string()))),
    ])
    db.read_text(read_path).map(json.loads).map(process_commit).to_dataframe().to_parquet("test.parquet",
                                                                                          schema=schema)
