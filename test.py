from codepile.stackexchange.stackexchange import StackExchangeDataset

dump_dir = "" # path where zip files need be downloaded or already present
temp_dir = ""
se_dataset = StackExchangeDataset(temp_dir, dump_dir)

se_dataset.process(force_unzip=False)