import regex as re

PUNCTUATION_REGEX = re.compile(r"\p{P}")
INTERNAL_HASH = "__dedup_hash__"


from typing import Dict

import numpy as np
import simhash
from collections import Counter, defaultdict, deque
import datasets
from multiprocessing import cpu_count
from typing import Dict, Set
from itertools import product
import os
import logging



logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger(__name__)


def check_num_proc(num_proc: int = -1) -> int:
    """
    Check the number of processors. Return a safe-checked value.

    Parameters
    ----------
    num_proc : int, optional
        Number of processors to use, by default -1

    Returns
    -------
    int
        Number of processors to use

    Raises
    ------
    ValueError
        If the input exceeds the number of processors available
    """
    maximum: int = cpu_count()
    if num_proc > maximum:
        raise ValueError(
            f"{num_proc} exceeds the maximum number ({maximum}) of processors"
        )

    if num_proc == -1:
        num_proc = maximum
    else:
        print(f"Using {num_proc} processors out of {maximum} can be slow")

    return num_proc



def hashing(
    record,
    column: str = "text",
    tokenization: str = "character",
    window_size: int = 4,
    ignore_punctuation: bool = True,
    lowercase: bool = True,
    output: str = INTERNAL_HASH,
) -> Dict[str, int]:
    """Hashing a document with SimHash.

    Parameters
    ----------
    record : [type]
        A dict of feature-value pairs
    column : str, optional
        The column name to use for hashing, by default "text"
    tokenization : str, optional
        Method to use for tokenization, by default "character"
    window_size : int, optional
        The size of the token window, by default 4
    ignore_punctuation : bool, optional
        To ignore punctuation or not, by default True
    lowercase : bool, optional
        To lowercase the text or not, by default True

    Returns
    -------
    Dict[str, int]
        The new hash feature column

    Raises
    ------
    Exception
        Unrecognized tokenization parameter
    """
    document = record[column]
    if lowercase:
        document = document.lower()

    if ignore_punctuation:
        document = PUNCTUATION_REGEX.sub("", document)

    if tokenization == "character":
        tokens = [
            str.encode(document[i : i + window_size])
            for i in range(len(document) - window_size)
        ]
    elif tokenization == "punctuation":
        tokens = PUNCTUATION_REGEX.split(document)
        tokens = [
            str.encode(" ".join(tokens[i : i + window_size]))
            for i in range(len(tokens) - window_size)
        ]
    elif tokenization == "space":
        tokens = document.split(" ")
        tokens = [
            str.encode(" ".join(tokens[i : i + window_size]))
            for i in range(len(tokens) - window_size)
        ]
    else:
        raise Exception(f"Unrecognized tokenization parameter {tokenization}")

    return {output: np.uint64(simhash.compute(map(simhash.unsigned_hash, tokens)))}

def deduplicate(ds, args):
    ds = ds.map(
        hashing,
        fn_kwargs={
            "tokenization": args.tokenization,
            "window_size": args.window_size,
            "column": args.text_column,
            "ignore_punctuation": args.ignore_punctuation,
            "lowercase": args.lowercase,
            "output": INTERNAL_HASH,
        },
        num_proc=check_num_proc(args.num_proc),
        desc="Hashing",
    )
    matches = simhash.find_all(
        ds[INTERNAL_HASH],
        args.num_blocks,
        args.hamming_distance,
    )
    graph = defaultdict(dict)
    dist = Counter()
    examples = defaultdict(set)
    for x, y in matches:
        graph[x][y] = True
        graph[y][x] = True
        dist[simhash.num_differing_bits(x, y)] += 1
        if len(examples[simhash.num_differing_bits(x, y)]) < 3:
            examples[simhash.num_differing_bits(x, y)].add((x, y))


    hash2ids: Dict[int, Set[str]] = defaultdict(set)
    hashes: Set[int] = set(ds[INTERNAL_HASH])
    hash2cluster: Dict[int, int] = {}
    visited: Set[int] = set()
    cluster_id: int = 0

    for id, hash in zip(ds[args.index_column], ds[INTERNAL_HASH]):
        hash2ids[hash].add(id)

    seen = set()
    with open(os.path.join(args.output_dir, "matches.tsv"), "w") as o:
        o.write(f"id1\tid2\tdiff\n")
        for x, y in matches:
            for id1, id2 in product(hash2ids[x], hash2ids[y]):
                if id1 == id2:
                    continue
                if tuple(sorted((id1, id2))) in seen:
                    continue
                seen.add(tuple(sorted((id1, id2))))
                o.write(f"{id1}\t{id2}\t{simhash.num_differing_bits(x, y)}\n")

    # print some match samples
    # datasets.set_progress_bar_enabled(False)
    datasets.logging.disable_progress_bar()
    example_text = []
    for diff in examples:
        for x, y in examples[diff]:
            records = []
            ids = hash2ids[x]
            ids.update(hash2ids[y])
            for text in ds.filter(
                lambda x: x["id"] in ids,
                num_proc=check_num_proc(args.num_proc),
            )["text"]:
                records.append(text)
            example_text.append((diff, records))

    # datasets.set_progress_bar_enabled(True)
    datasets.logging.enable_progress_bar()
    with open(os.path.join(args.output_dir, "examples.txt"), "w") as o:
        for diff, records in example_text:
            o.write("*" * 80 + "\n")
            for text in records:
                o.write(f"\n({diff}) {text}\n")

    while hashes:
        hash = hashes.pop()
        if hash in visited:
            continue

        # BFS to find the cluster
        if hash not in graph:
            hash2cluster[hash] = -1
            continue

        q = deque([hash])
        visited.add(hash)
        hash2cluster[hash] = cluster_id

        while q:
            node = q.popleft()
            for neighbor in graph[node]:
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                q.append(neighbor)
                hash2cluster[neighbor] = cluster_id

        cluster_id += 1

    logger.info(f"Found {cluster_id} clusters and {len(graph)} hashes")

    with open(os.path.join(args.output_dir, "clusters.tsv"), "w") as o:
        o.write(f"id\thash\tcluster\n")
        for id, hash in zip(ds[args.index_column], ds[INTERNAL_HASH]):
            o.write(f"{id}\t{hash}\t{hash2cluster.get(hash, -1)}\n")

    logger.info("Done!")