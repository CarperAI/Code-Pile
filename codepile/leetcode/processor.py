# Only selected tables are extraced from the zip files
# Convert xml to json/parquet and stores them in temp_dir
# Column names are modified to remove "@" at the beginning for convenience
# Transforms "Tags" column of posts table into a list
# Uses pandas "merge", "groupby" and "apply" features do one-one and one-many joins. one-many relationship leads to a nested list of dicts.
# By default unzipping, xml conversion and denormaliztion steps are skipped if the target files are present


import os
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import bz2, shutil
import json
import tarfile
from collections import OrderedDict

from codepile.dataset import Processor

class LeetCodeProcessor(Processor):
    def __init__(self, config):
        self.raw_data_dir = config.raw_data_dir # directory where the downloaded zip files are present for all the sites
        self.tmpdir = config.tmpdir
        self.output_data_dir = config.output_data_dir

        self.tables_to_consider = ["questions", "topics", "comments", "comment_replies"]

        self.include_columns = {
            "questions": OrderedDict([
                ("questionId", "int"), 
                ("acRate", "float64"), 
                ("difficulty", "str"), 
                ("subjects", "object"),  # renamed topics -> subjects to separate
                ("title", "str"), 
                ("content", "str"), 
                ("hints", "object"), 
                ("solution.content", "str"), 
                ("solution.rating.count", "int"), 
                ("solution.rating.average", "float64"), 
                ("stats.totalAccepted", "str"), 
                ("stats.totalSubmission", "str")
            ]),
            "topics": OrderedDict([
                ("questionId", "int"),
                ("topicId", "int"),
                ("title", "str"),
                ("commentCount", "int"),
                ("viewCount", "int"),
                ("post.content", "str"),
                ("post.voteCount", "int"),
                ("post.creationDate", "int"),
                ("post.author.username", "str")
            ]),
            "comments": OrderedDict([
                #("questionId", "int"),
                ("topicId", "int"),
                ("commentId", "int"),
                ("post.content", "str"),
                ("post.voteCount", "int"),
                ("post.creationDate", "int"),
                #("post.updationDate", "int"),
                ("post.author.username", "str")
            ]),
            "comment_replies": OrderedDict([
                #("questionId", "int"),
                #("topicId", "int"),
                ("commentId", "int"),
                #("commentReplyId", "int"),
                ("post.content", "str"),
                ("post.voteCount", "int"),
                ("post.creationDate", "int"),
                #("post.updationDate", "int"),
                ("post.author.username", "str")
            ])
        }
        self.prepare_directories()

    def process(self):
        zip_file = "leetcode.tar.bz2"
        print(f"Processing dump BZIP: {zip_file}")

        src_abs_path = os.path.join(self.raw_data_dir, zip_file)
        print(src_abs_path)
        try:
            tar = tarfile.open(src_abs_path, "r:bz2")
            tar.extractall(self.tmpdir)
            tar.close()
        except Exception as e:
            raise Exception(e, message=f"Failed to extract compressed file '{zip_file}'")

        print(f"Processing tables: {self.tables_to_consider}")

        self.process_steps()

    def prepare_directories(self):
        os.makedirs(self.tmpdir, exist_ok=True)
        os.makedirs(self.output_data_dir, exist_ok=True)

    def load_tables_into_pandas(self, src_dir, tables):
        df_dict = {}
        for table in tables:
            jsonl_path = os.path.join(src_dir, f"{table}.jsonl")
            df = pd.json_normalize([json.loads(x) for x in open(jsonl_path).readlines()])
            df_dict[table] = df
        return df_dict

    def customize_question_table(self, df):
        df["subjects"] = pd.Series([[i["name"] for i in x] for x in df["topicTags"]])
        df_stats = df["stats"].apply(lambda x: json.loads(x)).apply(pd.Series).add_prefix("stats.")
        df = pd.concat([df.reset_index(), df_stats.reset_index()], axis=1)
        df.loc[df["solution.contentTypeId"] != "107", "solution.content"] = ""
        cols_to_remove = list(df.columns.difference(self.include_columns["questions"].keys()))
        df.drop(columns=cols_to_remove, axis=1, inplace=True)
        df["solution.rating.count"].fillna(0, inplace=True)
        df = df.astype(self.include_columns["questions"])
        return df

    def customize_topic_table(self, df):
        df.rename({'id': 'topicId'}, axis=1, inplace=True)
        cols_to_remove = list(df.columns.difference(self.include_columns["topics"].keys()))
        df.drop(columns=cols_to_remove, axis=1, inplace=True)
        df = df.astype(self.include_columns["topics"])
        return df

    def customize_comment_table(self, df):
        df.rename({'id': 'commentId'}, axis=1, inplace=True)
        cols_to_remove = list(df.columns.difference(self.include_columns["comments"].keys()))
        df.drop(columns=cols_to_remove, axis=1, inplace=True)
        df = df.astype(self.include_columns["comments"])
        return df

    def customize_comment_reply_table(self, df):
        df.rename({'id': 'commentReplyId'}, axis=1, inplace=True)
        cols_to_remove = list(df.columns.difference(self.include_columns["comment_replies"].keys()))
        df.drop(columns=cols_to_remove, axis=1, inplace=True)
        df = df.astype(self.include_columns["comment_replies"])
        return df

    def collapse_replies_into_comments(self, df):
        record_keys = list(self.include_columns["comment_replies"].keys())
        record_keys.remove("commentId")
        record_dict_series = df["comment_replies"][record_keys].apply(lambda x: x.to_dict(), axis=1)
        df_comment_reply_dicts = pd.DataFrame({"commentId": df["comment_replies"]["commentId"], "record": record_dict_series})
        df_grouped_comment_replies = df_comment_reply_dicts.groupby("commentId")["record"].apply(list).reset_index()
        df_grouped_comment_replies.columns = ["commentId", "commentReplyDicts"]

        df["comments"] = pd.merge(df["comments"], df_grouped_comment_replies, how="left", on="commentId")
        df["comments"]["commentReplyDicts"] = df["comments"]["commentReplyDicts"].fillna("").apply(list)

    def collapse_comments_into_topics(self, df):
        record_keys = list(self.include_columns["comments"].keys())
        record_keys.remove("commentId")
        record_keys.append("commentReplyDicts")
        record_dict_series = df["comments"][record_keys].apply(lambda x: x.to_dict(), axis=1)
        df_comment_dicts = pd.DataFrame({"topicId": df["comments"]["topicId"], "record": record_dict_series})
        df_grouped_comments = df_comment_dicts.groupby("topicId")["record"].apply(list).reset_index()
        df_grouped_comments.columns = ["topicId", "commentDicts"]

        df["topics"] = pd.merge(df["topics"], df_grouped_comments, how="left", on="topicId")
        df["topics"]["commentDicts"] = df["topics"]["commentDicts"].fillna("").apply(list)

    def get_merged_topics_questions(self, df):
        df["questions"] = df["questions"].add_prefix("question.")
        df["questions"].rename({"question.questionId": "questionId"}, axis=1, inplace=True)
        df["topics"] = df["topics"].add_prefix("topic.")
        df["topics"].rename({"topic.questionId": "questionId"}, axis=1, inplace=True)
        return pd.merge(df["topics"], df["questions"], how="left", on="questionId")

    def process_steps(self):
        df_dict = self.load_tables_into_pandas(self.tmpdir, self.tables_to_consider)
        df_dict["questions"] = self.customize_question_table(df_dict["questions"])
        df_dict["topics"] = self.customize_topic_table(df_dict["topics"])
        df_dict["comments"] = self.customize_comment_table(df_dict["comments"])
        df_dict["comment_replies"] = self.customize_comment_reply_table(df_dict["comment_replies"])

        print("Files read!")

        self.collapse_replies_into_comments(df_dict)
        self.collapse_comments_into_topics(df_dict)

        print("Collapsed comments/replies into topics!")

        df_topic_with_questions = self.get_merged_topics_questions(df_dict)

        print("Final data ready!")

        output_path = os.path.join(self.output_data_dir, "leetcode_topics_with_questions.parquet.gzip")
        df_topic_with_questions.to_parquet(output_path)

