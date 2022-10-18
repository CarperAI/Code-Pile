import re

import numpy as np

import sentencepiece

import ftfy

def uniform_whitespace(
        document,
        whitespace=[" ", " ", " ", " ", " ", "　", " ", " ", " ", " ", "￼", ""]
    ):
        """There are different whitespace characters."""
        whitespace = set(whitespace)
        document = "".join(
            [char if char not in whitespace else " " for char in document]
        )
        return document

def load_sentencepiece_model(path_sentencepiece_model="en.sp.model"):
    sentencepiece_model = sentencepiece.SentencePieceProcessor()
    sentencepiece_model.load(path_sentencepiece_model)
    
    
def tokenization(document, sentencepiece_model, join_on_whitespace):
    document_tokenized = sentencepiece_model.encode_as_pieces(document)
    if join_on_whitespace:
        document_tokenized = " ".join(document_tokenized)
    return document_tokenized


def fixes_text(document):
    return ftfy.fix_text(document)

def lower_case(document):
    return document.lower()

def document_normalization(document, list_process_fn):
    for func in list_process_fn:
        doc = func(document)
    return doc

def get_words_from_document(document):
    words = document.split(" ")
    return words

def check_number_words(
    document,
    number_words_min_cutoff=10,
    number_words_max_cutoff=100000,
):
    words = get_words_from_document(document)
    cond = (len(words) >= number_words_min_cutoff) and (
        len(words) <= number_words_max_cutoff
    )
    return cond

def check_flagged_words_ratio(
    document,
    flagged_words_max_cutoff=0.1
):
    english_flagged_words = [
            "anal",
            "bareback",
            "bbw",
            "bdsm",
            "blowjob",
            "blowjobs",
            "brazzers",
            "bukkake",
            "camgirl",
            "camwhore",
            "cocksucking",
            "cougar",
            "creampie",
            "cuckold",
            "cum",
            "cumming",
            "cums",
            "cumshot",
            "cumshots",
            "cumslut",
            "cunnilingus",
            "deepthroat",
            "deepthroating",
            "dildo",
            "dildos",
            "dogging",
            "doggystyle",
            "dominatrix",
            "erotic",
            "fellatio",
            "femdom",
            "fingering",
            "fisting",
            "footjob",
            "gangbang",
            "handjob",
            "hentai",
            "horney",
            "horniest",
            "horny",
            "jism",
            "jizz",
            "masterbating",
            "masturbate",
            "masturbating",
            "masturbation",
            "milf",
            "orgies",
            "orgy",
            "pegging",
            "porn",
            "pornhub",
            "porno",
            "pornos",
            "pornstar",
            "pornstars",
            "redtube",
            "rimming",
            "slutty",
            "squirting",
            "strapon",
            "threesome",
            "vibrator",
            "xhamster",
            "xnxx",
            "xvideos",
            "xxx",
            "youporn",
    ]
    words = get_words_from_document(document)
    if not words:
        return False
    flagged_words_ratio = len(
        [word for word in words if word in english_flagged_words]
    ) / len(words)
    return flagged_words_ratio > flagged_words_max_cutoff

def fitering_pipeline(document):
    # lst_norm_funcs = [uniform_whitespace, fixes_text, lower_case]
    # document = document_normalization(document, lst_norm_funcs)
    return (check_number_words(document) == False )| (check_flagged_words_ratio(document))

if __name__=="__main__":
    lst_fn = [uniform_whitespace, fixes_text, lower_case]
    document = """
        Sometimes, camgirl camgirl camgirl camgirl camgirl camgirl camgirl camgirl camgirl camgirl when you look at a 
        function definition in Python, you might see that it takes two strange arguments: *args and **kwargs.
        If you’ve ever wondered what these peculiar variables are, or why your IDE defines them in main(), 
        then this article is for you. You’ll learn how to use args and kwargs in Python to add more flexibility to your functions
    """
    document = document_normalization(document, lst_fn)
    print(check_number_words(document))
    print(check_flagged_words_ratio(document))
