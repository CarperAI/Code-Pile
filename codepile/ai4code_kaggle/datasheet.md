---
TODO: Add YAML tags here. Copy-paste the tags obtained with the online tagging app: https://huggingface.co/spaces/huggingface/datasets-tagging
---

# Dataset Card for AI4Code Kaggle Competition

## Table of Contents
- [Table of Contents](#table-of-contents)
- [Dataset Description](#dataset-description)
  - [Dataset Summary](#dataset-summary)
  - [Supported Tasks and Leaderboards](#supported-tasks-and-leaderboards)
  - [Languages](#languages)
- [Dataset Structure](#dataset-structure)
  - [Data Instances](#data-instances)
  - [Data Fields](#data-fields)
  - [Data Splits](#data-splits)
- [Dataset Creation](#dataset-creation)
  - [Curation Rationale](#curation-rationale)
  - [Source Data](#source-data)
  - [Annotations](#annotations)
  - [Personal and Sensitive Information](#personal-and-sensitive-information)
- [Considerations for Using the Data](#considerations-for-using-the-data)
  - [Social Impact of Dataset](#social-impact-of-dataset)
  - [Discussion of Biases](#discussion-of-biases)
  - [Other Known Limitations](#other-known-limitations)
- [Additional Information](#additional-information)
  - [Dataset Curators](#dataset-curators)
  - [Licensing Information](#licensing-information)
  - [Citation Information](#citation-information)
  - [Contributions](#contributions)

## Dataset Description

- **Homepage:** https://www.kaggle.com/competitions/AI4Code
- **Repository:** https://github.com/CarperAI/Code-Pile/tree/working/codepile/ai4code_kaggle
- **Paper:** N/A
- **Leaderboard:** https://www.kaggle.com/competitions/AI4Code/leaderboard
- **Point of Contact:** Duy Phung

### Dataset Summary

This dataset is a collection of ~160,000 public Python notebooks from the popular [Kaggle](https://www.kaggle.com/) platform.
[More Information Needed]

### Supported Tasks and Leaderboards

[More Information Needed]

### Languages

The dataset is focused on English and Python.

## Dataset Structure

### Data Instances

Each data instance is a Python notebook, which has been converted from the JSON format to a string. The string contains the code, markdown, and other metadata of the notebook. The string is stored in the `source` field.

### Data Fields

* `id`: a `string` representing the notebook's ID.
* `source`: a string containing the Python notebook converted from JSON format.

### Data Splits

The dataset has a single split, which is the training set.

## Dataset Creation

### Curation Rationale

Jupyter Notebooks are a popular tool for data scientists and machine learning practitioners. They are used to document and share the process of data analysis and machine learning. Therefore, it is important to have a dataset that contains a large number of notebooks to help train models for code generation that can leverage the knowledge contained in these notebooks.

### Source Data

#### Initial Data Collection and Normalization

[More Information Needed]

#### Who are the source language producers?

The source language producers are the users of Kaggle.

### Annotations

#### Annotation process

[More Information Needed]

#### Who are the annotators?

[More Information Needed]

### Personal and Sensitive Information

[More Information Needed]

## Considerations for Using the Data

### Social Impact of Dataset

[More Information Needed]

### Discussion of Biases

[More Information Needed]

### Other Known Limitations

[More Information Needed]

## Additional Information

### Dataset Curators

[More Information Needed]

### Licensing Information

The dataset is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).

### Citation Information

[More Information Needed]

### Contributions

Thanks to [@PhungVanDuy](https://github.com/PhungVanDuy) for adding this dataset.