import spacy_fastlang
import spacy
from transformers import GPTNeoXTokenizerFast
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LangDetection:
    def __init__(self) -> None:
        self.model = spacy.load("en_core_web_sm")
        self.model.add_pipe("language_detector")

    def detect(self, text: str) -> str:
        doc = self.model(text)
        return doc._.language

    def detect_batch(self, text_list: list[str]) -> list[str]:
        return [self.detect(text) for text in text_list]


class Tokenization:
    def __init__(self, model_name="EleutherAI/gpt-neox-20b") -> None:
        self.tokenizer = GPTNeoXTokenizerFast.from_pretrained(model_name)

        logger.info(f"Sucessfully loaded {model_name}")

    def tokenize(self, text: str) -> list[int]:
        return self.tokenizer.encode(text)

    def tokenize_batch(self, text_list: list[str]) -> list[list[int]]:
        return self.tokenizer.encode(text_list)


stat_config_map: dict[str] = {
    "lang_idt": LangDetection(),
    "tokenize": Tokenization(),
    "len_char": lambda doc: len(doc),
    "len_utf8bytes": lambda doc: len(doc.encode("utf-8")),
    "len_words": lambda doc: len(re.split(r"\s+", doc)),
}
