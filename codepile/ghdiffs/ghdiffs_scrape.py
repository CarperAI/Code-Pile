import json
import urllib.request
from pathlib import Path
import datetime
import dask.bag as db
from codepile.dataset import (Dataset, DatasetInfo, RawDataset, Scraper)
from dask.distributed import Client
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
    patch_parsed_diff["file_extension"] = Path(patch_diff.target_file).suffix
    # patch_parsed_diff["patch_info"] = patch_diff.patch_info
    patch_diff_list = list(patch_diff)
    for patch_diff_ind in patch_diff_list:
        patch_diff_ind = str(patch_diff_ind)
        patch_diff_split = patch_diff_ind.split("@@")
        patch_diff_line = patch_diff_split[2].split("\n")
        patch_diff_line_numbers = [list(map(int, hunk.strip("-+").split(",")))
                                   for hunk in patch_diff_split[1].strip().split(" ")]
        patch_parsed_diff["hunks_process"].append(patch_diff_line_numbers + patch_diff_line[:-1])
        patch_parsed_diff["hunks"].append(patch_diff_ind)
    patch_parsed_diff["hunks"] = "".join(patch_parsed_diff["hunks"])
    return patch_parsed_diff


def get_before_file(file_diff: dict, commit_hash: str, repo_name: str) -> str:
    repo_owner, repo_name = repo_name.split("/")
    if file_diff["src_file"] == "/dev/null" or file_diff["tgt_file"] == "/dev/null":
        return ""
    # Get raw after file.
    file_raw_url = (f"https://raw.githubusercontent.com/{repo_owner}/"
                    f"{repo_name}/{commit_hash}/{file_diff['tgt_file'][2:]}")
    try:
        raw_file = urllib.request.urlopen(file_raw_url)
        raw_file_encoding = raw_file.headers.get_charsets()[0]
        raw_file = [line.decode(raw_file_encoding) for line in raw_file.readlines()]
    except Exception as e:
        print(e)
        print(file_raw_url)
        return ""
    # file_length = sum(1 for _ in raw_file)
    # if file_length < length_threshold:
    # files_list.append(raw_file)
    # Iterate over hunks for this file and apply the reverse patch.
    for hunk in file_diff["hunks_process"]:
        hunk_list = []
        for line in hunk[3:]:
            if line.startswith("-") or line.startswith(" "):
                hunk_list.append(line[1:] + "\n")
        raw_file[hunk[0][0] - 1:hunk[0][0] + hunk[1][1] - 1] = hunk_list
    del file_diff["hunks_process"]  # Deletes this item from the dict in parent functions
    return "".join(raw_file)


def process_commit(commit_data: dict) -> list[dict]:
    """
    Process a commit dictionary to get the before files and diff dict.

    Args:
        commit_data (dict): Dictionary containing commit hash, repo name, and
        commit message.

    Returns:
        list[dict]: A list of dicts, where each dict contains the data for a
        change to a single file.
    """
    # Scrape a commit's diff file.
    diff_url = f"https://github.com/{commit_data['repo_name']}/commit/{commit_data['commit']}.diff"
    try:
        diff = urllib.request.urlopen(diff_url)
    except Exception as e:
        return []
    encoding = diff.headers.get_charsets()[0]
    patch = PatchSet(diff, encoding=encoding)
    commit_list: list[dict] = []
    for patch_ind in patch:
        # Skip if the file is not a python file.
        # if not patch_ind.target_file.endswith(".py"):
        #     continue
        diff_dict: dict = process_ind_patch(patch_ind)
        diff_dict.update(commit_data)
        diff_dict["before_file"] = get_before_file(diff_dict, commit_data["commit"], commit_data["repo_name"])
        commit_list.append(diff_dict)
    return commit_list


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
        client = Client(n_workers=16, threads_per_worker=1)
        self.read_path = Path(config["read_path"])
        self.save_path = Path(config["save_path"])

    def scrape(self) -> RawDataset:
        _ = (
            db.read_text(self.read_path).map(json.loads)
            .map(process_commit).flatten().to_dataframe()
            .to_parquet(self.save_path)
        )
        dataset = RawDataset(storage_uris=["https://github.com/CarperAI/Code-Pile"], complete=True)
        return dataset


if __name__ == "__main__":
    read_path = Path(__file__).parent / "full_test.json"
    save_path = Path(__file__).parent / "full_test.parquet"
    config = {"read_path": read_path, "save_path": save_path}
    ghdiff_dataset = GitHubDiffDataset(config)
    ghdiff_dataset.download()
    # client = Client(n_workers=16, threads_per_worker=1)
    # db.read_text(read_path).map(json.loads).map(process_commit).flatten().to_dataframe().to_parquet(save_path)
