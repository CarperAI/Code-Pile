from functools import partial
from typing import List, Set, Dict
import hashlib

import os
import logging
from utils import check_num_proc

from filtering import ModifyingDocuments

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger(__name__)


def get_hash(record, text_field):
    """Get hash of content field."""
    text = record[text_field]
    text = ModifyingDocuments.normalization(
                document=text,
                remove_non_printing_characters=True,
                strip=True,
                lower_case=True,
                uniform_whitespace=True,
                replace_digits_with_zeros=True,
                replace_unicode_punctuation=True,
            )
    return {"hash": hashlib.md5(text.strip().encode("utf-8")).hexdigest()}


def is_new_hash(hash_: str, hashes: Set[str]) -> bool:
    """Check if current hash is still in set of unique hashes and remove if true."""
    if hash_ in hashes:
        return False
    else:
        hashes.add(hash_)
        return True

def delete_text_from_duplicates(batch: Dict[str, List], hashes: Set[str], text_field: str) -> Dict[str, List]:
    return {
        **{k: v for k, v in batch.items() if k != "hash"},
        text_field: [text if is_new_hash(hash_, hashes) else "" for text, hash_ in zip(batch[text_field], batch["hash"])]
    }



def exact_deduplicate_dataset(ds, args):
    hashed_ds = ds.map(
        get_hash,
        fn_kwargs={"text_field": args.text_column},
        num_proc=check_num_proc(args.num_proc)
    )
    hashes = set()
    dedup_ds = hashed_ds.map(
            partial(delete_text_from_duplicates, hashes=hashes, text_field=args.text_column),
            num_proc=1,  # VERY IMPORTANT: hashes will be updated, and is not thread safe.
            batched=True,
            batch_size=16,
            remove_columns=['hash', '__index_level_0__']
    )
    return dedup_ds

