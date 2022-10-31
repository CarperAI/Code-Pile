from datasets import load_dataset

pilev2 = load_dataset('pilev2.py', cache_dir='data_pilev2', subsets=['Ubuntu IRC', 'DM Mathematics'])
print(pilev2)
