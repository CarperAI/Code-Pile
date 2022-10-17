import json
from typing import Any
import urllib.request
import argparse
from pathlib import Path
import dask.bag as db
from codepile.dataset import (Dataset, DatasetInfo, RawDataset, Scraper)
from dask.distributed import Client, progress
from unidiff import PatchSet


def process_ind_patch(patch_diff) -> dict:
    """Process patch to get diff data."""
    patch_parsed_diff: dict = {
        "hunks": [],
        "hunks_process": [],
    }

    patch_parsed_diff["addition_count"] = patch_diff.added
    patch_parsed_diff["deletion_count"] = patch_diff.removed
    patch_parsed_diff["src_file"] = patch_diff.source_file
    patch_parsed_diff["tgt_file"] = patch_diff.target_file
    if patch_parsed_diff["tgt_file"] == "/dev/null":
        patch_parsed_diff["file_extension"] = Path(patch_diff.source_file).suffix
    else:
        patch_parsed_diff["file_extension"] = Path(patch_diff.target_file).suffix
    for patch_diff_ind in patch_diff:
        patch_diff_ind = str(patch_diff_ind)
        patch_diff_split = patch_diff_ind.split("@@")
        patch_diff_line = patch_diff_split[2].split("\n")
        patch_diff_line_numbers = [list(map(int, hunk.strip("-+").split(",")))
                                   for hunk in patch_diff_split[1].strip().split(" ")]
        patch_parsed_diff["hunks_process"].append(patch_diff_line_numbers + patch_diff_line[:-1])
        patch_parsed_diff["hunks"].append(patch_diff_ind)
    patch_parsed_diff["hunks"] = "".join(patch_parsed_diff["hunks"])
    return patch_parsed_diff


def get_before_file(file_diff: dict, commit_hash: str, repo_name: str, length_threshold: int) -> str:
    repo_owner, repo_name = repo_name.split("/")
    if file_diff["src_file"] == "/dev/null":
        raw_file: Any = ["ADDFILE"]
    elif file_diff["tgt_file"] == "/dev/null":
        # If file is deleted, get before file from the raw diff, which will be the full file.
        raw_file = [line[1:] + "\n" for line in file_diff["hunks_process"][0][3:]]
    else:
        # Get raw after file.
        file_raw_url = (f"https://raw.githubusercontent.com/{repo_owner}/"
                        f"{repo_name}/{commit_hash}/{file_diff['tgt_file'][2:]}")
        try:
            raw_file = urllib.request.urlopen(file_raw_url)
            raw_file_encoding = raw_file.headers.get_charsets()[0]
            raw_file = [line.decode(raw_file_encoding) for line in raw_file.readlines()]
            if length_threshold > 0 and len(raw_file) > length_threshold:
                return ""
        except Exception as e:
            # print(e, file_raw_url)
            return ""
        # Iterate over hunks for this file and apply the reverse patch.
        for hunk in file_diff["hunks_process"]:
            hunk_list = []
            for line in hunk[3:]:
                if line.startswith("-") or line.startswith(" "):
                    hunk_list.append(line[1:] + "\n")
            raw_file[hunk[0][0] - 1:hunk[0][0] + hunk[1][1] - 1] = hunk_list
    del file_diff["hunks_process"]  # Deletes this item from the dict in parent functions
    return "".join(raw_file)


class GitHubDiffDataset(Dataset):
    def __init__(self, config):
        self.config = config
        self.scraper = GitHubDiffScraper(self.config)

    def download(self, *args, **kwargs) -> RawDataset:
        return self.scraper.scrape()

    def process(self):
        raise NotImplementedError

    @property
    def info(self) -> DatasetInfo:
        return DatasetInfo(
            id="GitHubDiffDataset",
            description="Dataset of diffs from GitHub")

    @property
    def id(self) -> str:
        return ""


class GitHubDiffScraper(Scraper):
    def __init__(self, config):
        # TODO: Dask multi-node scheduling here
        self.client = Client(n_workers=config.n_workers, threads_per_worker=config.threads_per_worker)
        self.read_path = Path(config.read_path)
        self.save_path = Path(config.save_path)
        self.python_only = config.python_only
        self.diff_length_threshold = config.diff_length_threshold
        self.code_length_threshold = config.code_length_threshold
        self.ignore_deletions = config.ignore_deletions

    def scrape(self) -> RawDataset:
        meta_spec = {'hunks': str, 'addition_count': int, 'deletion_count': int,
                     'src_file': str, 'tgt_file': str, 'file_extension': str,
                     'before_file': str, 'commit': str, 'message': str,
                     'repo_name': str, 'language_name': str, 'author_name': str,
                     'license': str}
        result = (
            db.read_text(self.read_path).map(json.loads)
            .map(self.process_commit).flatten().to_dataframe(meta=meta_spec)
            .to_parquet(self.save_path)
        )
        progress(result)
        dataset = RawDataset(storage_uris=["https://github.com/CarperAI/Code-Pile"], complete=True)
        return dataset

    def process_commit(self, commit_data: dict) -> list[dict]:
        """
        Process a commit dictionary to get the before files and diff dict.

        Args:
            commit_data (dict): Dictionary containing commit hash, repo name, and
            commit message.

        Returns:
            list[dict]: A list of dicts, where each dict contains the data for a
            change to a single file.
        """
        if self.python_only and commit_data["language_name"] != "Python":
            return []
        # Scrape a commit's diff file.
        diff_url = f"https://github.com/{commit_data['repo_name']}/commit/{commit_data['commit']}.diff"
        try:
            diff = urllib.request.urlopen(diff_url)
            encoding = diff.headers.get_charsets()[0]
            patch = PatchSet(diff, encoding=encoding)
            if len(patch) == 0:
                return []
        except Exception as e:
            # print(e, diff_url)
            return []
        commit_list: list[dict] = []
        # Iterate over files within the diff.
        for patch_ind in patch:
            if self.ignore_deletions and patch_ind.target_file == "/dev/null":
                continue
            if self.diff_length_threshold > 0 and sum(len(hunk) for hunk in patch_ind) > self.diff_length_threshold:
                continue
            diff_dict: dict = process_ind_patch(patch_ind)
            diff_dict["before_file"] = get_before_file(diff_dict, commit_data["commit"], commit_data["repo_name"],
                                                       length_threshold=self.code_length_threshold)
            if not diff_dict["before_file"]:
                # Happens if exception is thrown or file is too long.
                continue
            diff_dict["commit"] = commit_data["commit"]
            diff_dict["message"] = commit_data["message"]
            diff_dict["repo_name"] = commit_data["repo_name"]
            diff_dict["language_name"] = commit_data["language_name"]
            diff_dict["author_name"] = commit_data["author"]["name"]
            diff_dict["license"] = commit_data["license"]
            commit_list.append(diff_dict)
        return commit_list


if __name__ == "__main__":
    parser = argparse.ArgumentParser('codepile dataset tool')

    parser.add_argument('--read_path', type=str)
    parser.add_argument('--save_path', type=str)
    parser.add_argument('--n_workers', type=int, default=8)
    parser.add_argument('--threads_per_worker', type=int, default=2)
    parser.add_argument('--python_only', type=bool, default=False)
    parser.add_argument('--diff_length_threshold', type=int, default=1000,
                        help="Maximum number of lines in the diff for a *single* file. Set to 0 for no limit.")
    parser.add_argument('--code_length_threshold', type=int, default=1000,
                        help="Maximum number of lines in code files. Set to 0 for no limit.")
    parser.add_argument('--ignore_deletions', type=bool, default=True,
                        help="Ignore file deletion diffs.")
    config = parser.parse_args()
    ghdiff_dataset = GitHubDiffDataset(config)
    ghdiff_dataset.download()
