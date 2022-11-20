# STATS MODULE
The module acts as a single point of entry for all the stats related functions to run in large scale datasets. For our own terminology we see there are potentially two types of statistics,
1. `Metadata based` - *EDA* components like distribution to be pivoted against the metadata. This is the most common type of stats that we see in all the dataset.    
*Example* : In stackexchange, we would like to find the distribution of datapoints amongst the different sites under different sites.
2. `Document based` - *NLP components* like wordcloud, topic-modelling, language-detection, etc. These are the stats that are more specific to the document text and are necessarily applicable to all the datasets.
## Basic Structure
The whole module is split into two stages,
1. `src/stat_module.py` - This script provides `GetMeta` function and a `Statistics` class. They extract the meta data and run the stats respectively.    
The `GetMeta` function is used to extract the metadata from the dataset and formats it to a json file to see the fields of the datasets. The `Statistics` module needs to be overidden in `map_fn` and `map_batch_fn` inorder to apply them to the stats of the dataset.

2. `src/viz.py` - This script provides the `Visualize` class which is used to visualize the stats extracted from the `Statistics` class.

## Various EDA components
`src/utils.py` contains of the following selected EDA classes which are used to extract the stats from the dataset. Any `stat-request` should be a class which has two functions,
1. `map_fn` - This function is used to extract the stats from the dataset. It takes in the `row` of the dataset and returns the stats.
2. `map_batch_fn` - This function is used to aggregate the stats from the `map_fn` and returns the aggregated stats over a batch.

These two functions are based off the [batching](https://huggingface.co/docs/datasets/about_map_batch) feature in the processing functions of the huggingface datasets library. 