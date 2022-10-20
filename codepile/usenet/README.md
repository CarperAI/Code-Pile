# UseNet scraper

Download, extract, read, dedup, and clean usenet archives.
[CodePile Issue](https://github.com/CarperAI/Code-Pile/issues/16)

## Usage:

`UsenetDataset.download` is used to download and process usenet forums. `download` has multiple config options:

- (without any kwargs) - Download the entire usenet-comp dataset from InternetArchive and parse it.
- **files** (optional) - A subset of groups to fetch from the archive (See groups below). If empty processes the entire usenet-comp archive.
- **s3** (optional) - Set to true to instead use a cached copy of the usenet-comp dataset stored on S3.

### Groups

Usenet is made up of multiple logical groupings of topics. For instance, comp.lang.java.programming consists of all
questions and answers related to the java programming language.

usenet-comp contains all groups that come under the comp heading. Approx 30 GB of messages.

This scraper allows you to process all comp groups or a subset such as only java groups, c++ related groups, etc.
