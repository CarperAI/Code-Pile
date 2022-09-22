### Download WikiBook dumps dataset 

```bash
wget https://dumps.wikimedia.org/enwikibooks/20220901/enwikibooks-20220901-pages-meta-current.xml.bz2 -O data/enwikibooks-20220820-pages-meta-current.xml.bz2
bzip2 -d data/enwikibooks-20220820-pages-meta-current.xml.bz2
```

### Run WikiExtractor

```bash
sh wikiparser.sh
```

### Preprocess wikibooks and get computing books only
```bash
python wikibooks.py
```

Output will dumps into two files: `all_wikibooks.parquet.gzip` and `computing_wikibooks.parquet.gzip` are all wikibooks data and computing books.