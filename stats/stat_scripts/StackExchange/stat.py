from stats.src.stat_module import GetMeta
import os
from tqdm import tqdm
dataset_path = "/fsx/shared/hf_data_pilev2_by_cats"
dataset_list = os.listdir(dataset_path)
meta_dict = {}

for dataset in tqdm(dataset_list):
    meta = GetMeta(f"/fsx/shared/hf_data_pilev2_by_cats/{dataset}","datasets").get_meta_and_write()
    print(meta.keys())
    meta_dict[dataset] = meta

import json

def write_json_file(json_file_path:str,data:dict):
    with open(json_file_path,"w") as f:
        json.dump(data,f,indent=2)

write_json_file("stats/meta.json",meta_dict)
