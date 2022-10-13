* Downloads all the dumps https://archive.org/details/stackexchange using internetarchive python module
* Skips some stackoverflow.com files that are not needed for processing right now (PostHistory, Votes, Badges). This is not the case with other sites since all the tables' data is zipped together for them.
* Wherever possible, processor tries to check if a site has already been processed and decides to skip. There are flags exposed in the API to force these steps (force_unzip, force_conversion, force_process)
* In some cases, the files on disk could get corrupted in which case forcing the processing steps would fix the issues.


### Configuration
* processor.py has quite a few configurations that can be tweaked for various reasons. All of these are not exposed as API right now and need to be configured direclty in this file.
* include_sites, exclude_sites is useful to experiment and exclude some sites (for ex,sites that are not in english language) that are needed.
* force_unzip, force_conversion, force_process can be used to force the processor to redo processing.
* Sample config.json to start processing
```
{
    "raw_data_dir": "/home/vanga/se-dump",
    "tmpdir": "/home/vanga/se-temp/",
    "output_data_dir": "/home/vanga/se-temp/"
}
```
* Due to the current structure of the code, raw_data.json file may need to be created manually to run "process" if raw dumps are not downloaded using this repo code. Just create a dummy file like below (actual values don't matter)
```
{
    "storage_uris": [
        "file:///./tests/data/dumps"
    ],
    "metadata": ""
}
```

### Processing:
#### Unzipping
* Only necessary tables are extracted. py7zr can do this.
* This list of tables to consider is hardcoded in processor.py (tables_to_consider) class for now.
#### xml-to-parquet conversion
* Intermediate data is being stored in parquet since it is easier to work with and load into tools like pandas and pyspark for further processing
* pyarrow is being used to stream data to parquet file 
#### combining tables
* pyspark is used to JOIN the tables
* Posts and Comments are joined with Users table.
* Answer posts are grouped by ParentId to generate a column with list of answers
* Comments are grouped by PostId to generate a column with list of comments
* These nested lists are being stored as string. (pyspark supports storing this data as objects also, but there are some challenges doing that using pyspark that need to be figured out)