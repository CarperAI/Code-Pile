# Code-Pile

![pytest](https://github.com/CarperAI/Code-Pile/actions/workflows/python_starter.yml/badge.svg)


This repository contains the processing scripts to scrape/process the code-pile dataset.

## Table of Contents
* Project Description
* How to use the Code-Pile (todo)
* How to Contribute
* Additional Resources

## Project Description
Check out [The code pile proposal](https://carperai.notion.site/Code-Pile-Organization-adfe8babbe07451cbd489a50cc0c985a)

The Code-Pile will be released similar to "the pile" as a folder of .jsonl.zst files, see [lm-dataformat](https://github.com/EleutherAI/lm_dataformat)

## How to use the Code-Pile
It's not finished, ask on discord

## How to Contribute
Think about the most usefull Code-data for the next generation of textual Code Models. 

The most valuable dataset properties (use your own judgment) are:
1. Open License
2. Data quality
3. Dataset size
4. Data variance/variety/nicheness
5. Ease of obtaining/processing

To add a new dataset, open a Issue under given `dataset-request` template. Gather all the related informations appropriate to it. Use the issue to track.

Check if there is existing Code or someone already working on it:
See Additional Resources

1. Eleuthers Pile V1 Repos
2. Ask on Carper #code-pile
3. Ask on Eleuther
4. Consult the linked Spreadsheets below

Then implement it through the following steps:

1. Fork this repo
2. Use the `working` branch
3. Read the shared classes in `datasets.py` and `codepile.py`
4. Create mvp/example for your dataset
5. Create a pull request
6. Keep building the data-domain specific classes and repeat 

Citation Placeholder:
```
@misc{Code-Pile,
  author = {},
  doi = {},
  month = {},
  title = {},
  url = {https://github.com/CarperAI/Code-Pile},
  version = {},
  year = {2022}
}
```

## Additional Resources
* [Preliminary spreadsheet of useful resources](https://docs.google.com/spreadsheets/d/1OrOnv-Cv1wRq0jNk4AegHiMtLk88YQDz5b1TP-o5SE8/edit#gid=0)

Closely related projects:

* [The Pile V2 spreadsheet](https://docs.google.com/spreadsheets/d/1nVxbXj0k-5p9kY_TlY8xMnpsqp_JNlWXpD48L8hXH8E/edit#gid=906372269)
* [Pile V1 stackexchange-dataset repo](https://github.com/EleutherAI/stackexchange-dataset/tree/fc34e85c12a5a2fb41b324db1c416cdac8ca5732)
* [Collins stackexchange gist](https://gist.github.com/craffel/a1e2aff893776d0ef2b0a95ed0fd7e7a)

Previous work:
* [Codeparrot] (https://github.com/huggingface/transformers/tree/main/examples/research_projects/codeparrot)
* ...
