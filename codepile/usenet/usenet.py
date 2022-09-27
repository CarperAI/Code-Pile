from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset

import mailbox

import internetarchive as ia


class UsenetScraper(Scraper):

    def scrape(self, ia_identifier='usenet-comp') -> RawDataset:
        item = ia.get_item(ia_identifier)
        metadata = item.metadata
        ia.download(ia_identifier, checksum=True, verbose=True, destdir=self.target_dir)

        return RawDataset(
            storage_uris=['file:///{self.target_dir}'],
            metadata=str(metadata)
        )


class UsenetProcessor(Processor):

    def process(self):
        pass


class UsenetDataset(Dataset):
    def __init__(self, tempdir, target_dir, *args, **kwargs):
        self.scraper = UsenetScraper(tempdir, target_dir)

    def download(self):
        self.scraper.download()



