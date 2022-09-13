import os

with open("ghtorrent_urls.txt", "w") as fp:
    for line in fp:
        line = line.replace("\n", "")
        os.system(f"wget http://ghtorrent-downloads.ewi.tudelft.nl/mysql/{line} -O ghtorrent_data/{line}")