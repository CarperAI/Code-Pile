# Only selected tables are extraced from the zip files
# Convert xml to json/parquet and stores them in temp_dir
# Column names are modified to remove "@" at the beginning for convenience
# Transforms "Tags" column of posts table into a list
# Uses pandas "merge", "groupby" and "apply" features do one-one and one-many joins. one-many relationship leads to a nested list of dicts.
# By default unzipping, xml conversion and denormaliztion steps are skipped if the target files are present

import os
import shutil
from pathlib import Path
from tqdm import tqdm
import py7zr

import pyarrow as pa
import pyarrow.parquet as pq
from lxml import etree
from functools import partial
from more_itertools import chunked


from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,create_map, collect_list, to_json
from pyspark.sql.types import StringType


from codepile.dataset import Processor
from types_meta import *

class StackExchangeProcessor(Processor):
    def __init__(self, config, id):
        self.dump_src_dir = config.raw_data_dir # directory where the downloaded zip files are present for all the sites
        self.temp_dir = config.tmpdir
        self.output_dir = self.temp_dir

        self.exclude_sites = [] # exclude list has precedence over include list
        self.include_sites = [] # "superuser.com", "askubuntu.com"
        self.tables_to_consider = ["Posts", "Comments", "Users"]
        self.intermediate_format = "parquet" # parquet
        self.batch_size = 10000
        self.max_records_per_output_file = 1000000

        self.include_columns = {
            "Posts": ["Id", "PostTypeId", "AcceptedAnswerId", "Body", "OwnerUserId", "ParentId", "Score", "Title", "Tags", "ContentLicense", "AnswerCount", "CommentCount", "ViewCount", "FavoriteCount", "CreationDate"],
            "Users": ["Id", "Reputation", "DisplayName", "AboutMe", "Views", "AccountId", "CreationDate"],
            "Comments": ["Id", "PostId", "Score", "Text", "UserId", "ContentLicense", "CreationDate"]
        }
        self.prepare_directories()
        self.build_schema_meta()
        self.spark_dir = self.temp_dir


    def process(self, raw_data, force_unzip=False, force_xml_conversion=False, force_process=False):
        self.spark = SparkSession.builder.config("spark.worker.cleanup.enabled", "true").config("spark.local.dir", self.spark_dir).config("spark.driver.memory", "16G").master("local[16]").appName('spark-stats').getOrCreate()        
        print(f"Processing tables: {self.tables_to_consider}")
        sites_to_process = set()
        for zip_file in os.listdir(self.dump_src_dir):
            if not zip_file.endswith(".7z"):
                continue
            site = self.site_name_from_zipfile(zip_file)
            if self.skip_site(site):
                continue

            output_site_dir = os.path.join(self.output_dir, site)
            os.makedirs(output_site_dir, exist_ok=True)        
            questions_output_dir = os.path.join(output_site_dir, "questions")
            spark_success_file = os.path.join(questions_output_dir, "_SUCCESS")
            if os.path.exists(spark_success_file) and not force_process:
                print(f"Skipping, site '{site}' is already processed")
                continue            
            
            src_abs_path = os.path.join(self.dump_src_dir, zip_file)
            try:
                self.extract_dump(src_abs_path, site, force_unzip)
                sites_to_process.add(site)
            except Exception as e:
                print(f"Failed to extract compressed file '{zip_file}', {e}")
            
        print(f"Processing {len(sites_to_process)} site/s")
        for site in sites_to_process:
            try:
                print(f"Processing site: {site}")
                self.convert_xml(site, force_xml_conversion)
                self.denormalize_data(site, force_process)
                # delete temporary data
                shutil.rmtree(os.path.join(self.temp_dir, "xml", site))
                shutil.rmtree(os.path.join(self.temp_dir, "parquet", site))
            except Exception as e:
                print(f"Failed to process the site: '{site}'")
                print(e)


    def prepare_directories(self):
        os.makedirs(self.temp_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        xml_temp_dir = os.path.join(self.temp_dir, "xml")
        os.makedirs(xml_temp_dir, exist_ok=True)
        parquet_temp_dir = os.path.join(self.temp_dir, self.intermediate_format)
        os.makedirs(parquet_temp_dir, exist_ok=True)    

    def get_site_intermediate_dir(self, site):
        return os.path.join(self.temp_dir, self.intermediate_format, site)

    def get_site_xml_dir(self, site):
        return os.path.join(self.temp_dir, "xml", site)        

    def site_name_from_zipfile(self, zip_file):
        # downloaded zip files for stackoverflow.com are separate for each table/entity.
        if zip_file.startswith("stackoverflow.com-"):
            return "stackoverflow.com"
        else:
            return Path(zip_file).stem

    def skip_site(self, site):
        return (site in self.exclude_sites) or (len(self.include_sites) > 0 and site not in self.include_sites)

    def extract_dump(self, zip_file, site, force_unzip):
        dest_dir = self.get_site_xml_dir(site)

        os.makedirs(dest_dir, exist_ok=True)

        if site == "stackoverflow.com":
            table = Path(zip_file).stem.replace("stackoverflow.com-", "")
            if table in self.tables_to_consider:
                file_names = [table + ".xml"]
            else:
                file_names = []
        else:
            file_names = list(map(lambda x: x + ".xml", self.tables_to_consider))
        
        files_to_extract = set()

        for fn in file_names:
            if not os.path.exists(os.path.join(dest_dir, fn)) or force_unzip:
                files_to_extract.add(fn)
    
        if len(files_to_extract) > 0:
            print(f"'{site}': extracting: {files_to_extract}")
            with py7zr.SevenZipFile(zip_file, 'r') as archive:
                archive.extract(path=dest_dir, targets=list(files_to_extract))
    
    def build_schema_meta(self):
        post_schema = pa.schema([(k,a()) for k,p,a in post_types])
        post_schema_python = {k:p for k,p,a in post_types}

        user_schema = pa.schema([(k,a()) for k,p,a in user_types])
        user_schema_python = {k:p for k,p,a in user_types}

        comment_schema = pa.schema([(k,a()) for k,p,a in comment_types])
        comment_schema_python = {k:p for k,p,a in comment_types}
        self.python_schemas = {
            "Posts": post_schema_python,
            "Users": user_schema_python,
            "Comments": comment_schema_python
        }
        self.schemas = {
            "Posts": post_schema,
            "Users": user_schema,
            "Comments": comment_schema
        }

    def convert_xml(self, site, force_conversion):
        # most of the xml parsing logic is taken from the work of https://github.com/flowpoint
        def parse_xml(source_xml) -> dict :
            # use lxml because pyarrow readxml has trouble with types
            for event, element in etree.iterparse(source_xml, events=('end',), tag='row'):
                j = dict(element.attrib)
                yield j

                # cleanup this element, and parents, to save memory
                # https://stackoverflow.com/questions/7171140/using-python-iterparse-for-large-xml-files
                element.clear(keep_tail=True)
                while element.getprevious() is not None:
                    del element.getparent()[0]

        def cast_dict_to_schema(schema, data: dict):
            d = dict()
            for k, type_ in schema.items():
                if k in data:
                    d[k] = type_(data[k])
                else:
                    d[k] = None
            return d

        xml_src_dir = self.get_site_xml_dir(site)
        dest_dir = self.get_site_intermediate_dir(site)
        format = self.intermediate_format
        os.makedirs(dest_dir, exist_ok=True)

        for table in self.tables_to_consider:
            schema = self.schemas[table]
            python_schema = self.python_schemas[table]
            target_path = os.path.join(dest_dir, f"{table}.parquet")
            if os.path.exists(target_path) and not force_conversion:
                print(f"table '{table}' is already converted from xml")
                continue

            writer = pq.ParquetWriter(target_path, schema)

            xml_stream = parse_xml(os.path.join(xml_src_dir, f"{table}.xml"))

            corrected_types_steam = map(
                    partial(cast_dict_to_schema, python_schema),
                    xml_stream)

            chunked_stream = chunked(corrected_types_steam, self.batch_size)

            for chunk in tqdm(chunked_stream):
                batch = pa.RecordBatch.from_pylist(
                        chunk,
                        schema=schema)

                writer.write_batch(batch)
            print(f"Finished converting {table} from xml to {format}")

    def load_data_into_spark_dfs(self, src_dir, tables, format="parquet"):
        dfs = {}
        for table in tables:
            table_path = os.path.join(src_dir, table + "." + format)
            if format == "parquet":
                df = self.spark.read.parquet(table_path).select(self.include_columns[table])
            else:
                raise ValueError("Unsuported format")
            dfs[table] = df
        return dfs

    def transform_tags_column(self, df):
        df['Tags'] = df['Tags'].str.replace('><',',').str.replace('<','').str.replace('>','').str.split(',')

    def get_questions_subset(self, df):
        questions_df = df.filter(df['PostTypeId'] == "1")
        return questions_df

    def get_answers_subset(self, df):
        answers_df = df.filter(df['PostTypeId'] == "2")
        return answers_df

    def denormalize_data(self, site, force_process):
        def create_map_args(df):
            args = []
            for c in df.columns:
                args.append(lit(c))
                args.append(col(c))
            return args

        site_temp_dir = self.get_site_intermediate_dir(site)
        output_site_dir = os.path.join(self.output_dir, site)
        os.makedirs(output_site_dir, exist_ok=True)        
        questions_output_dir = os.path.join(output_site_dir, "questions")

        dfs = self.load_data_into_spark_dfs(site_temp_dir, self.tables_to_consider)
        posts_df = dfs['Posts']
        posts_df = posts_df.filter((posts_df.PostTypeId == "1") | (posts_df.PostTypeId == "2"))
        posts_df = posts_df[(posts_df.PostTypeId == "1") | (posts_df.PostTypeId == "2")]
        comments_df = dfs['Comments']
        users_df = dfs['Users']
        users_df = users_df.select(*(col(x).alias('user_' + x) for x in users_df.columns))

        # join user info with posts
        print("Adding user info to posts")
        posts_df = posts_df.join(users_df, posts_df.OwnerUserId == users_df.user_Id, "left")
        #TODO: drop user_Id
        # join user info with comments
        print("Adding user info to comments")
        comments_df = comments_df.join(users_df, comments_df.UserId == users_df.user_Id, "left")

        # group comments by posts and populate a list of dictionaries
        comments_dicted = comments_df.withColumn("dict",
            create_map(create_map_args(comments_df))
        ).select(["Id", "PostId", "dict"])
        comments_grouped = comments_dicted.groupby("PostId").agg(collect_list("dict").alias("comments"))

        # populate posts with comments
        print("Adding comments to posts")
        posts_df = posts_df.join(comments_grouped, posts_df.Id == comments_grouped.PostId, "left").select(posts_df["*"], to_json(comments_grouped["comments"]).alias("comments"))

        questions_df = self.get_questions_subset(posts_df)
        answers_df = self.get_answers_subset(posts_df)


        answers_dicted = answers_df.withColumn("dict",
            create_map(create_map_args(answers_df))
        ).select(["Id", "ParentId", "dict"])

        # group answers by questions
        print("Grouping answers by questions")
        answers_grouped = answers_dicted.groupby("ParentId").agg(collect_list("dict").alias("answers"))

        # populate questions with answers 
        print("Adding answers to questions")
        questions_df = questions_df.join(answers_grouped, questions_df.Id == answers_grouped.ParentId, "left").select(questions_df["*"], to_json(answers_grouped["answers"]).alias("answers"))
        
        questions_df.coalesce(1).write.mode("overwrite").option("maxRecordsPerFile", self.max_records_per_output_file).parquet(questions_output_dir)
        print(f"Finished processing site: '{site}'")