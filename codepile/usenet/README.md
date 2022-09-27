# UseNet scraper

Download, extract, read, dedup, and clean usenet archives.
[CodePile Issue](https://github.com/CarperAI/Code-Pile/issues/16)

## Usage:

Run `codepile.usenet.usenet.main(temp, dest)`

- **temp** - location where files will be downloaded and extracted from Internet Archives.
- **dest** - processed xml files stored here.
- **files** - (Optional, Highly Recommended) A subset of files to fetch from the archive for testing purposes. Default is None. Leave blank to download and process ALL files in the archive.
- **ia_id** - Internet Archive identifier, default is '[usenet-comp](https://archive.org/details/usenet-comp)'

**Example Usage:**

`main('temp', 'dest', ['comp.lang.basic.visual.mbox.zip', ], )`

Fetches and processes only the [visual basic](https://archive.org/download/usenet-comp) item from the usenet-comp archives.
