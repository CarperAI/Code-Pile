from stats.src.stat_module import Statistics, master_map_fn
from stats.src.utils import stat_config_map
import multiprocessing as mp
import argparse


parser = argparse.ArgumentParser("StackExchange")
parser.add_argument("--input_path", type=str)
parser.add_argument("--output_path", type=str)

args = parser.parse_args()

dataset_path = args.input_path
analysis_path = args.output_path


stats_class = Statistics(dataset_path)

map_fn = master_map_fn  # Look at the definition to see a dummy map_fn for reference

stats_dataset = stats_class.get_stat_dataset(
    map_fn=map_fn, batched=True, num_proc=mp.cpu_count()
)  # I found out that batched increases the speed by 10x.

stats_dataset.save_to_disk(analysis_path)
