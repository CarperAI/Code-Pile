import internetarchive as ia

import shutil

import glob
import os

from .forum import mbox

from xml.etree import ElementTree as ET


USENET_COMP = 'usenet-comp'


def main(temp_dir, dest_dir, ia_id=USENET_COMP, files=None):
    # Downloading the archive (or specific files from the archive) to the temp dir
    if files:
        ia.download(ia_id, destdir=temp_dir, files=files)
    else:
        ia.download(ia_id, destdir=temp_dir)
    # Unzipping all files - stored in temp_dir/ia_id
    unzipped_files = os.path.join(temp_dir, ia_id, 'unzipped')
    for file in glob.glob(os.path.join(temp_dir, ia_id, '*')):
        if file.endswith('.mbox.zip'):
            shutil.unpack_archive(file, unzipped_files)

    # Process each unzipped file and store the result in dest_dir
    for file in glob.glob(os.path.join(unzipped_files, '*')):
        if file.endswith('.mbox'):
            threads = mbox.process_forum(file)
            for thread in threads:
                tree = ET.ElementTree(thread.export_xml())
                tree.write(os.path.join(
                    dest_dir, '{}.xml'.format(thread.get_id())), xml_declaration=True, encoding='utf-8',
                )
