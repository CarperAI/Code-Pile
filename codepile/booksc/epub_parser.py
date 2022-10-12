#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copied verbatim from https://github.com/soskek/bookcorpus

# Requirements:
#
# beautifulsoup4>=4.6.3
# html2text>=2018.1.9
# blingfire>=0.0.9
# progressbar>=2.5
# lxml>=4.3.2

import re
import os
import sys
import urllib
try:
    from urllib import unquote
except:
    from urllib.parse import unquote
import zipfile

import xml.parsers.expat
import html2text
from glob import glob
from pprint import pprint as pp

from natsort import natsorted

import json


class ContainerParser():
    def __init__(self, xmlcontent=None):
        self.rootfile = ""
        self.xml = xmlcontent

    def startElement(self, name, attributes):
        if name == "rootfile":
            self.buffer = ""
            self.rootfile = attributes["full-path"]

    def parseContainer(self):
        parser = xml.parsers.expat.ParserCreate()
        parser.StartElementHandler = self.startElement
        parser.Parse(self.xml, 1)
        return self.rootfile


class BookParser():
    def __init__(self, xmlcontent=None):
        self.xml = xmlcontent
        self.title = ""
        self.author = ""
        self.inTitle = 0
        self.inAuthor = 0
        self.ncx = ""

    def startElement(self, name, attributes):
        if name == "dc:title":
            self.buffer = ""
            self.inTitle = 1
        elif name == "dc:creator":
            self.buffer = ""
            self.inAuthor = 1
        elif name == "item":
            if attributes["id"] == "ncx" or attributes["id"] == "toc" or attributes["id"] == "ncxtoc":
                self.ncx = attributes["href"]

    def characters(self, data):
        if self.inTitle:
            self.buffer += data
        elif self.inAuthor:
            self.buffer += data

    def endElement(self, name):
        if name == "dc:title":
            self.inTitle = 0
            self.title = self.buffer
            self.buffer = ""
        elif name == "dc:creator":
            self.inAuthor = 0
            self.author = self.buffer
            self.buffer = ""

    def parseBook(self):
        parser = xml.parsers.expat.ParserCreate()
        parser.StartElementHandler = self.startElement
        parser.EndElementHandler = self.endElement
        parser.CharacterDataHandler = self.characters
        parser.Parse(self.xml, 1)
        return self.title, self.author, self.ncx


class NavPoint():
    def __init__(self, id=None, playorder=None, level=0, content=None, text=None):
        self.id = id
        self.content = content
        self.playorder = playorder
        self.level = level
        self.text = text


class TocParser():
    def __init__(self, xmlcontent=None):
        self.xml = xmlcontent
        self.currentNP = None
        self.stack = []
        self.inText = 0
        self.toc = []

    def startElement(self, name, attributes):
        # TODO: what to do when no navpoints? Example: https://imgur.com/gAWuSaf
        if name == "navPoint":
            level = len(self.stack)
            self.currentNP = NavPoint(
                attributes["id"], attributes["playOrder"], level)
            self.stack.append(self.currentNP)
            self.toc.append(self.currentNP)
        elif name == "content":
            self.currentNP.content = unquote(attributes["src"])
        elif name == "text":
            self.buffer = ""
            self.inText = 1

    def characters(self, data):
        if self.inText:
            self.buffer += data

    def endElement(self, name):
        if name == "navPoint":
            self.currentNP = self.stack.pop()
        elif name == "text":
            if self.inText and self.currentNP:
                self.currentNP.text = self.buffer
            self.inText = 0

    def parseToc(self):
        parser = xml.parsers.expat.ParserCreate()
        parser.StartElementHandler = self.startElement
        parser.EndElementHandler = self.endElement
        parser.CharacterDataHandler = self.characters
        parser.Parse(self.xml, 1)
        return self.toc

def epub_name_matches(pattern, name):
  rx = re.compile(r'[^a-zA-Z_\-/.]', re.IGNORECASE)
  norm = re.sub(rx, '', name)
  return re.search(pattern, norm)

def epub_toc_file(filelist):
  for name in filelist:
    if epub_name_matches(r'\b(toc|table.?of)', name):
      return name

def extract_markdown_links(text):
  for match in re.finditer(r'(?![!])\[(.*?)\]\((.*?)\)', text):
    yield match.groups()

def extract_html_links(text):
  for match in re.finditer(r'"([^"]+?[.][a-zA-Z]{2,}(?:[#][^"]+)?)"', text):
    yield match.groups()

def html_links(text):
  return [x[0] for x in extract_html_links(text)]

def flatten(xs):
  r = []
  for x in xs:
    if isinstance(x, list):
      r.extend(flatten(x))
    else:
      r.append(x)
  return r

def string_bucket(buckets, strings, flat=False):
  strings = [x for x in strings]
  results = []
  for bucket in buckets:
    if isinstance(bucket, str):
      bucket = bucket.split(',')
    out = []
    for pattern in bucket:
      for s in strings:
        if s not in out and epub_name_matches(pattern, s):
          out.append(s)
    for string in out:
      strings.remove(string)
    results.append(out)
  results.append(strings)
  if flat:
    results = flatten(results)
  return results

def sort_epub_files(filelist):
  *front, outro, chapters, other = string_bucket([
    'cover',
    'title',
    'copyright',
    'toc,table.?of,contents',
    'frontmatter,acknowledge',
    'intro,forward',
    'index,outro,epilogue',
    '[.]htm[l]?$',
    ], natsorted(filelist), flat=False)
  return flatten(front) + chapters + outro + other


def extract_epub_rootfile(file):
  filelist = [x.filename for x in file.filelist]
  if 'META-INF/container.xml' in filelist:
    result = file.read('META-INF/container.xml').decode('utf8')
    result = re.sub("='(.*?)'", r'="\1"', result)
    return result


def extract_epub_opf(file, meta=None):
  if meta is None:
    meta = extract_epub_rootfile(file)
  root = [line for line in meta.split('\n') if '<rootfile ' in line]
  assert len(root) > 0
  rootpath = html_links(root[0])[0]
  assert rootpath.endswith('opf')
  result = file.read(rootpath).decode('utf8')
  result = re.sub("""='(.*?)'""", r'="\1"', result)
  return result


def rmblanklines(text):
  return '\n'.join([x for x in text.split('\n') if len(x.strip()) > 0])


def extract_epub_section(name, file, opf=None):
  if opf is None:
    opf = extract_epub_opf(file)
  result = re.sub(re.compile('.*<{name}.*?>(.*?)</{name}>.*'.format(name=name), re.DOTALL), r'\1', opf)
  return rmblanklines(result)


def extract_epub_guide(file, opf=None):
  return extract_epub_section("guide", file=file, opf=opf)



def extract_epub_manifest(file, opf=None):
  return extract_epub_section("manifest", file=file, opf=opf)

def xmlnode(element, text):
  # strip html comments
  text = re.sub(re.compile(r'<!--.*?-->', re.DOTALL), '', text)
  rx = r'<(?P<tag>{element})\s?(?P<props>.*?)(?:/>|>(?P<value>.*?)</{element}>)'.format(element=element)
  rx = re.compile(rx, re.DOTALL)
  items = re.finditer(rx, text)
  items = [item.groupdict() for item in items]
  for item in items:
    props = dict(re.findall(r'([^\s]*?)="(.*?)"', item['props']))
    #item['props'] = props # ehh, just merge it
    del item['props']
    item.update(props)
  return items

def extract_epub_items(file, opf=None):
  manifest = extract_epub_manifest(file, opf=opf)
  return xmlnode('item', manifest)

def extract_epub_spine(file, opf=None):
  spine = extract_epub_section("spine", file=file, opf=opf)
  return xmlnode('itemref', spine)

# def extract_epub_ids(file, opf=None):
#   items = extract_epub_items(file, opf=opf)
#   return {x['id']: x['href'] for x in items}

def href2filename(file, href, filelist):
  href = href.split('#', 1)[0] # strip anchor
  href = unquote(href) # urldecode
  for name in filelist:
    if name == href or name.endswith('/' + href):
      return name
  sys.stderr.write('href2filename: failed to find href {href!r} in epub {epub!r} with filelist {filelist!r}\n'.format(epub=file.filename, href=href, filelist=filelist))
  if args.debug:
    import pdb; pdb.set_trace()


def htmlfiles(filelist):
  return [filename for filename in filelist if filename.endswith('htm') or filename.endswith('html')]

def extract_epub_order(file, opf=None):
  filelist = sort_epub_files([x.filename for x in file.filelist])
  items = extract_epub_items(file, opf=opf)
  spine = extract_epub_spine(file, opf=opf)
  ids = {x['id']: x['href'] for x in items}
  try:
    found = uniq([href2filename(file, ids[ref['idref']], filelist) for ref in spine])
  except KeyError as e:
    sys.stderr.write('error: KeyError for {!r}: ids is {!r}, filelist is {!r}\n'.format(file.filename, ids, filelist))
    if args.debug:
      import pdb; pdb.set_trace()
    raise e
  found = [x for x in found if x is not None] # href2filename can fail
  for filename in found:
    if filename not in filelist:
      sys.stderr.write('Unknown found filename for {!r}: {!r}, filelist is {!r}\n'.format(file.filename, filename, filelist))
    else:
      filelist.remove(filename)
  # This file seems to be unreferenced by anything else, sometimes.
  if 'META-INF/nav.xhtml' in filelist:
    filelist.remove('META-INF/nav.xhtml')
  hfiles = htmlfiles(filelist)
  if len(hfiles) > 0:
    sys.stderr.write('Leftover HTML files for {!r}: {!r}\n'.format(file.filename, hfiles))
    if args.debug:
      import pdb; pdb.set_trace()
  return found + filelist


def extract_epub_toc(file):
  filelist = [x.filename for x in file.filelist]
  meta = extract_epub_rootfile(file)
  if meta is not None:
    opf = extract_epub_opf(file, meta)
    guide = extract_epub_guide(file, opf)
    links = html_links(toc)
    leftover = sort_epub_files(filelist)
    result = []
    for link in links:
      link = link.split('#', 1)[0] # strip anchor
      for name in leftover:
        if name.endswith('/' + link):
          result.append(name)
          leftover.remove(name)
          break
    return result, leftover
  else:
    return None, None


def epub_html_files(file):
  files, leftover = extract_epub_toc(file)
  if files is not None and leftover is not None:
    return [x for x in files + leftover if x.endswith('htm') or x.endswith('html')]


def uniq(xs):
  r = []
  for x in xs:
    if x not in r:
      r.append(x)
  return r

def subst_1(pattern, replacement, lines, ignore=None):
  for line in lines:
    if ignore is None or not re.match(ignore, line):
      line = re.sub(pattern, replacement, line)
    yield line


def subst(pattern, replacement, lines, ignore=None):
  if isinstance(lines, str):
    return '\n'.join(subst_1(pattern, replacement, lines.split('\n'), ignore=ignore))
  else:
    return subst_1(pattern, replacement, lines, ignore=ignore)


from io import BytesIO


class epub2txt():
    def __init__(self, epubfile=None):
        self.epub = epubfile if epubfile != '-' and not epubfile.startswith('/dev/fd/') else BytesIO(sys.stdin.buffer.read())
        self.epub_name = epubfile

    def convert(self):
        # print "Processing %s ..." % self.epub
        file = zipfile.ZipFile(self.epub, "r")
        # rootfile = ContainerParser(
        #     file.read("META-INF/container.xml")).parseContainer()
        # title, author, ncx = BookParser(file.read(rootfile)).parseBook()
        # ops = "/".join(rootfile.split("/")[:-1])
        # if ops != "":
        #     ops = ops+"/"
        # toc = TocParser(file.read(ops + ncx)).parseToc()


        # filelist = [x.filename for x in file.filelist]
        # tocfile = epub_toc_file(filelist)
        # if not tocfile:
        #   import pdb; pdb.set_trace()
        # toc = file.read(tocfile).decode('utf-8')
        # pp(list(extract_html_links(toc)))
        # import pdb; pdb.set_trace()
        # files = {x.filename: file.read(x).decode('utf-8') for x in file.filelist if x.filename.endswith('htm') or x.filename.endswith('html')}
        # from natsort import natsorted
        # import json
        # file_order = natsorted(list(files.keys()))

        if False:
          files = {x.filename: file.read(x).decode('utf-8') for x in file.filelist if x.filename.endswith('htm') or x.filename.endswith('html')}
          file_order = natsorted(list(files.keys()))
          # file_order = list(files.keys())
        else:
          meta = extract_epub_rootfile(file)
          if meta is None: import pdb; pdb.set_trace()
          opf = extract_epub_opf(file, meta=meta)
          if opf is None: import pdb; pdb.set_trace()
          # file_order = epub_html_files(file)
          file_order = htmlfiles(extract_epub_order(file, opf=opf))
          if file_order is None: import pdb; pdb.set_trace()

        files = {x: file.read(x).decode('utf-8') for x in file_order}

        content = []
        for xmlfile in file_order:
          html = files[xmlfile]
          if not args.quiet:
            sys.stderr.write(self.epub_name+'/'+xmlfile + '\n')
          h = html2text.HTML2Text()
          h.body_width = 0
          text = h.handle(html)
          if not text.endswith('\n'):
            text += '\n'
          filename = self.epub_name+'/'+xmlfile
          #name, ext = os.path.splitext(filename)
          bookname = filename + '.md'
          if not args.no_metadata:
            content.append('<|file name={}|>'.format(json.dumps(bookname)) + '\n')
          content.append(text)
          if not args.no_metadata:
            content.append('<|/file name={}|>'.format(json.dumps(bookname)) + '\n')

        file.close()
        result = ''.join(content)
        # final postprocessing fixups: tables come out all weird, so
        # fix them with a hack.
        result = result.replace('\n\n| \n\n', ' | ')

        if args.ftfy:
          import ftfy
          result = ftfy.fix_text(result)
          # replace unicode … with ... which ftfy doesn't do by default
          # NOTE: this departs from openai's convention of calling
          # ftfy.fix_text() with default arguments. In particular,
          # OpenAI's GPT-2 models do generate unicode ellipses.
          # Nonetheless, we replace unicdoe ellipses with ... to
          # increase the chances of semantic understanding.
          result = result.replace(' …', '...') # first pass: convert "foo  …" to "foo..."
          #result = result.replace(' …', '...') # second pass: convert "foo …" to "foo..."
          result = result.replace('…', '...') # final pass: convert "foo…" to "foo..."

        result = result.split('\n') # split into lines for performance in the following sections.

        ignore_ul_item = r'[*]\s'
        ignore_ol_item = r'[0-9]+[.]\s'
        ignore_li = '(?!(' + ignore_ul_item + ')|(' + ignore_ol_item + '))'
        ignore_code='^[ ]{4,}' + ignore_li + r'[^\s]'

        def sub(pattern, replacement, text):
          return subst(pattern, replacement, text, ignore=ignore_code)

        if args.plain_text:
          #result = unmark(result)
          # get rid of images
          result = sub('[!]\s*[\[].*?[\]][(].*?[)]', ' ', result)
          # remove reference links, e.g.  [3](e9781429926119_bm01.html#end_en12)
          result = sub('\[([0-9]+?)\][(].*?[)]', '', result)
          # replace [foo](www.example.com) with foo
          result = sub('[!]?\[(.*?)\][(].*?[)]', r'\1', result)

        # fix up cases like this:
        #
        #   1\. foo
        #
        #   2\. bar
        #
        # For [\n][0-9]+[\\][.], strip the backslash.
        #result = re.sub(r'[\n]([0-9]+)[\\][.][ ]', r'\1. ', result)
        result = sub(re.compile(r'([0-9]+)[\\][.][ ]', re.DOTALL), r'\1. ', result)

        # convert lines back to text
        result = '\n'.join(result)

        if not args.no_collapse_blanks:
          # replace long runs of blank lines with three blank lines
          rx = re.compile(r'([\r\t ]*[\n]+){2,}', re.DOTALL)
          result = re.sub(rx, r'\n\n', result)

        # fix up cases like this:
        #
        #   ... some text...
        #   ## Chapter 1
        #
        # Put a newline before the "## Chapter 1", to have a blank
        # line before headings.
        result = re.sub(r'\n([^\n]+)[\n]#', r'\n\1\n\n#', result)

        if not args.no_collapse_blanks:
          # replace long runs of blank lines with three blank lines
          rx = re.compile(r'([\r\t ]*[\n]+){3,}', re.DOTALL)
          result = re.sub(rx, r'\n\n\n', result)

        if args.append is not None:
          append = str.encode(args.append).decode('unicode-escape')
          result += append
          
        return result


# # https://stackoverflow.com/questions/761824/python-how-to-convert-markdown-formatted-text-to-text

# from markdown import Markdown
# from io import StringIO


# def unmark_element(element, stream=None):
#     if stream is None:
#         stream = StringIO()
#     if element.text:
#         stream.write(element.text)
#     for sub in element:
#         unmark_element(sub, stream)
#     if element.tail:
#         stream.write(element.tail)
#     return stream.getvalue()


# def unmark(text):
#     # patching Markdown
#     Markdown.output_formats["plain"] = unmark_element
#     __md = Markdown(output_format="plain")
#     __md.stripTopLevelTags = False
#     return __md.convert(text)


#==============================================================================
# Cmdline
#==============================================================================
import argparse

parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter, 
    description="""
TODO
""")
     
parser.add_argument('infile', default='-', nargs='?')
parser.add_argument('outfile', default='-', nargs='?')

parser.add_argument('-v', '--verbose',
    action="store_true",
    help="verbose output" )

parser.add_argument('-n', '--no-metadata',
    action="store_true",
    help="Don't output <|file name=...|>" )

parser.add_argument('-f', '--ftfy',
    action="store_true",
    help="Run text through ftfy.fix_text()" )

parser.add_argument('-a', '--append',
    default=None,
    help="Append this string to the end of the text (useful for adding <|endoftext|>)")

parser.add_argument('-p', '--plain-text',
    action="store_true",
    help="Convert markdown to plain text")
     
parser.add_argument('-q', '--quiet',
    action="store_true",
    help="Don't output ToC info to stderr")

parser.add_argument('-nc', '--no-collapse-blanks',
    action="store_true",
    help="Don't collapse long runs of blank lines into three blank lines" )

parser.add_argument('--debug',
    action="store_true",
    help="pdb.set_trace() on error conditions" )

args = None


import time

def main():
    global args
    if not args:
        args, leftovers = parser.parse_known_args()
        args.args = leftovers
    filenames = glob(args.infile) if '*' in args.infile else [args.infile]
    out = None
    for filename in filenames:
        try:
          txt = epub2txt(filename).convert()
        except:
          sys.stderr.write('Error converting {!r}:\n'.format(filename))
          raise
        if len(txt.strip()) > 0:
          if out is None:
            out = open(args.outfile, "w") if args.outfile is not '-' else sys.stdout
          out.write(txt)
          out.flush()

if __name__ == "__main__":
  main()