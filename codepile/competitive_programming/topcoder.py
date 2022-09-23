from codepile.dataset import DatasetInfo, DatasetSources, RawDataset, Scraper, Processor, Analyser, Dataset
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from tqdm import tqdm

TOPCODER_ARCHIVE_URL = "https://www.topcoder.com/tc?module=ProblemArchive&sr=0&er=10&sc=&sd=&class=&cat=&div1l=&div2l=&mind1s=&mind2s=&maxd1s=&maxd2s=&wr="



COOKIES = {
    'tracking-preferences': '{%22version%22:1%2C%22destinations%22:{%22Google%20Tag%20Manager%22:true%2C%22Heap%22:true%2C%22Hotjar%22:true%2C%22LinkedIn%20Insight%20Tag%22:true%2C%22Quora%20Conversion%20Pixel%22:true}%2C%22custom%22:{%22marketingAndAnalytics%22:true%2C%22advertising%22:true%2C%22functional%22:true}}',
    'ajs_anonymous_id': '6d14e0b7-4ff4-47e4-a52c-2c9adabb28c4',
    '_gcl_au': '1.1.944903799.1663173266',
    '_gacid': '1371554180.1663173267',
    '_gclid': 'null',
    '_gid': 'GA1.2.2122236881.1663915096',
    'km_ai': 'fzPlSffu8M6VCZnDqhHNw%2Fg2XFU%3D',
    '_hjSessionUser_3088126': 'eyJpZCI6ImIxOTM0NDk4LTk0NmMtNThhNS04ZTlhLTA4ZjgyODhkNDcxZiIsImNyZWF0ZWQiOjE2NjMxNzMyNjY5MTUsImV4aXN0aW5nIjp0cnVlfQ==',
    'JSESSIONID': 'pEPNRxbkJU3LqDzl4zn20Q**.tomcat_tc01',
    'tcjwt': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik1rWTNNamsxTWpNeU5Ua3dRalkzTmtKR00wRkZPRVl3TmtJd1FqRXlNVUk0TUVFNE9UQkZOZyJ9.eyJodHRwczovL3RvcGNvZGVyLmNvbS91c2VySWQiOiI0MDI5NjMxNiIsImh0dHBzOi8vdG9wY29kZXIuY29tL2hhbmRsZSI6IlBodW5nLlZhbi5EdXkiLCJodHRwczovL3RvcGNvZGVyLmNvbS9yb2xlcyI6WyJUb3Bjb2RlciBVc2VyIl0sImh0dHBzOi8vdG9wY29kZXIuY29tL3VzZXJfaWQiOiJhdXRoMHw0MDI5NjMxNiIsImh0dHBzOi8vdG9wY29kZXIuY29tL3Rjc3NvIjoiNDAyOTYzMTZ8ZTFmMWIyNWE4NzkwYWNhYWNlYjkzY2IzZmZmYWUwYzM1MTliNGZlNmQxN2RmZjQ3YmEzOGM1M2E4ZjQxNCIsImh0dHBzOi8vdG9wY29kZXIuY29tL2FjdGl2ZSI6dHJ1ZSwiaHR0cHM6Ly90b3Bjb2Rlci5jb20vb25ib2FyZGluZ193aXphcmQiOiJzaG93IiwiaHR0cHM6Ly90b3Bjb2Rlci5jb20vYmxvY2tJUCI6ZmFsc2UsIm5pY2tuYW1lIjoiUGh1bmcuVmFuLkR1eSIsIm5hbWUiOiIxNDEyMDk1QHN0dWRlbnQuaGNtdXMuZWR1LnZuIiwicGljdHVyZSI6Imh0dHBzOi8vcy5ncmF2YXRhci5jb20vYXZhdGFyLzkwZWIxZmRhYmRmNWFjODhjZjk1NDU2YTZiNDBjODRiP3M9NDgwJnI9cGcmZD1odHRwcyUzQSUyRiUyRmNkbi5hdXRoMC5jb20lMkZhdmF0YXJzJTJGMTQucG5nIiwidXBkYXRlZF9hdCI6IjIwMjItMDktMjNUMDY6Mzk6MzEuMjk4WiIsImVtYWlsIjoiMTQxMjA5NUBzdHVkZW50LmhjbXVzLmVkdS52biIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJpc3MiOiJodHRwczovL2F1dGgudG9wY29kZXIuY29tLyIsInN1YiI6ImF1dGgwfDQwMjk2MzE2IiwiYXVkIjoiVVc3Qmhzbm1BUWgwaXRsNTZnMWpVUGlzQk85R29vd0QiLCJpYXQiOjE2NjM5MTUxNzQsImV4cCI6MTY2NDAwMTU3NCwic2lkIjoic3Y4UEp6bWEzNWlBb1UzVG9teEFiakg3S2dKMVRjb2EiLCJub25jZSI6IloyTkVUUzV5V0RkalJYZENTWGhXYWtjM2JHbE5lamhXYlhBeFRsWXlTVUZVTG5kMlpWcDBVbVJXYmc9PSJ9.u3RHMM-rf3c3AhFDlaKIvuHVC_xDHW6sdGoJzfXZ_GqjJcvvqyH9ixV_WC-YwFiI6-nRpKKxdiGKPjMe0d59sgQHpqjQfJdimz6NMUsDqyKK5Cu5Nw5DxbD1HKBVMfKYz2upX44qE0VpbaM9r4QkPy_usbHPMEsuMyP7cE9EXaPZ_n4XkeHsaKlcg1W6ehjoI8oVOo_pODL6f72MUnOphgYFt5QggdDh3VkMpRPgN_2m6YiVxohDsLWHWtmFfTtgqqh8Gaw7gFQfXwJ3NE_E7mxQa647-5bRCgHODmfXAZawvravivrpNgiWDPUpLkcmG3w9tl5_Z2Q7WILvix5Y-A',
    'v3jwt': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik1rWTNNamsxTWpNeU5Ua3dRalkzTmtKR00wRkZPRVl3TmtJd1FqRXlNVUk0TUVFNE9UQkZOZyJ9.eyJodHRwczovL3RvcGNvZGVyLmNvbS91c2VySWQiOiI0MDI5NjMxNiIsImh0dHBzOi8vdG9wY29kZXIuY29tL2hhbmRsZSI6IlBodW5nLlZhbi5EdXkiLCJodHRwczovL3RvcGNvZGVyLmNvbS9yb2xlcyI6WyJUb3Bjb2RlciBVc2VyIl0sImh0dHBzOi8vdG9wY29kZXIuY29tL3VzZXJfaWQiOiJhdXRoMHw0MDI5NjMxNiIsImh0dHBzOi8vdG9wY29kZXIuY29tL3Rjc3NvIjoiNDAyOTYzMTZ8ZTFmMWIyNWE4NzkwYWNhYWNlYjkzY2IzZmZmYWUwYzM1MTliNGZlNmQxN2RmZjQ3YmEzOGM1M2E4ZjQxNCIsImh0dHBzOi8vdG9wY29kZXIuY29tL2FjdGl2ZSI6dHJ1ZSwiaHR0cHM6Ly90b3Bjb2Rlci5jb20vb25ib2FyZGluZ193aXphcmQiOiJzaG93IiwiaHR0cHM6Ly90b3Bjb2Rlci5jb20vYmxvY2tJUCI6ZmFsc2UsIm5pY2tuYW1lIjoiUGh1bmcuVmFuLkR1eSIsIm5hbWUiOiIxNDEyMDk1QHN0dWRlbnQuaGNtdXMuZWR1LnZuIiwicGljdHVyZSI6Imh0dHBzOi8vcy5ncmF2YXRhci5jb20vYXZhdGFyLzkwZWIxZmRhYmRmNWFjODhjZjk1NDU2YTZiNDBjODRiP3M9NDgwJnI9cGcmZD1odHRwcyUzQSUyRiUyRmNkbi5hdXRoMC5jb20lMkZhdmF0YXJzJTJGMTQucG5nIiwidXBkYXRlZF9hdCI6IjIwMjItMDktMjNUMDY6Mzk6MzEuMjk4WiIsImVtYWlsIjoiMTQxMjA5NUBzdHVkZW50LmhjbXVzLmVkdS52biIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJpc3MiOiJodHRwczovL2F1dGgudG9wY29kZXIuY29tLyIsInN1YiI6ImF1dGgwfDQwMjk2MzE2IiwiYXVkIjoiVVc3Qmhzbm1BUWgwaXRsNTZnMWpVUGlzQk85R29vd0QiLCJpYXQiOjE2NjM5MTUxNzQsImV4cCI6MTY2NDAwMTU3NCwic2lkIjoic3Y4UEp6bWEzNWlBb1UzVG9teEFiakg3S2dKMVRjb2EiLCJub25jZSI6IloyTkVUUzV5V0RkalJYZENTWGhXYWtjM2JHbE5lamhXYlhBeFRsWXlTVUZVTG5kMlpWcDBVbVJXYmc9PSJ9.u3RHMM-rf3c3AhFDlaKIvuHVC_xDHW6sdGoJzfXZ_GqjJcvvqyH9ixV_WC-YwFiI6-nRpKKxdiGKPjMe0d59sgQHpqjQfJdimz6NMUsDqyKK5Cu5Nw5DxbD1HKBVMfKYz2upX44qE0VpbaM9r4QkPy_usbHPMEsuMyP7cE9EXaPZ_n4XkeHsaKlcg1W6ehjoI8oVOo_pODL6f72MUnOphgYFt5QggdDh3VkMpRPgN_2m6YiVxohDsLWHWtmFfTtgqqh8Gaw7gFQfXwJ3NE_E7mxQa647-5bRCgHODmfXAZawvravivrpNgiWDPUpLkcmG3w9tl5_Z2Q7WILvix5Y-A',
    'tcsso': '40296316|e1f1b25a8790acaaceb93cb3fffae0c3519b4fe6d17dff47ba38c53a8f414',
    '_cid': 'undefined',
    'knu': 'true',
    'ajs_user_id': '40296316',
    'km_lv': 'x',
    '_conv_r': 's%3Agithub.com*m%3Areferral*t%3A*c%3A',
    'km_vs': '1',
    '_hp2_ses_props.4240734066': '%7B%22ts%22%3A1663953938114%2C%22d%22%3A%22community.topcoder.com%22%2C%22h%22%3A%22%2Fstat%22%7D',
    '_conv_v': 'vi%3A1*sc%3A5*cs%3A1663951715*fs%3A1663173268*pv%3A57*exp%3A%7B100030166.%7Bv.1000199527-g.%7B100026706.1-100026707.1-100027342.1%7D%7D-100030395.%7Bv.1000200193-g.%7B%7D%7D%7D*ps%3A1663941179',
    'kvcd': '1663956451832',
    '_ga_ZYJTERS9DX': 'GS1.1.1663953934.6.1.1663956452.0.0.0',
    '_ga': 'GA1.1.1371554180.1663173267',
    '_hp2_id.4240734066': '%7B%22userId%22%3A%22138067581782230%22%2C%22pageviewId%22%3A%221089055819894497%22%2C%22sessionId%22%3A%223803406840918144%22%2C%22identity%22%3A%2240296316%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3Anull%2C%22isIdentified%22%3A1%7D',
}

HEADERS = {
    'authority': 'community.topcoder.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'accept-language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
    'cache-control': 'max-age=0',
    # Requests sorts cookies= alphabetically
    # 'cookie': 'tracking-preferences={%22version%22:1%2C%22destinations%22:{%22Google%20Tag%20Manager%22:true%2C%22Heap%22:true%2C%22Hotjar%22:true%2C%22LinkedIn%20Insight%20Tag%22:true%2C%22Quora%20Conversion%20Pixel%22:true}%2C%22custom%22:{%22marketingAndAnalytics%22:true%2C%22advertising%22:true%2C%22functional%22:true}}; ajs_anonymous_id=6d14e0b7-4ff4-47e4-a52c-2c9adabb28c4; _gcl_au=1.1.944903799.1663173266; _gacid=1371554180.1663173267; _gclid=null; _gid=GA1.2.2122236881.1663915096; km_ai=fzPlSffu8M6VCZnDqhHNw%2Fg2XFU%3D; _hjSessionUser_3088126=eyJpZCI6ImIxOTM0NDk4LTk0NmMtNThhNS04ZTlhLTA4ZjgyODhkNDcxZiIsImNyZWF0ZWQiOjE2NjMxNzMyNjY5MTUsImV4aXN0aW5nIjp0cnVlfQ==; JSESSIONID=pEPNRxbkJU3LqDzl4zn20Q**.tomcat_tc01; tcjwt=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik1rWTNNamsxTWpNeU5Ua3dRalkzTmtKR00wRkZPRVl3TmtJd1FqRXlNVUk0TUVFNE9UQkZOZyJ9.eyJodHRwczovL3RvcGNvZGVyLmNvbS91c2VySWQiOiI0MDI5NjMxNiIsImh0dHBzOi8vdG9wY29kZXIuY29tL2hhbmRsZSI6IlBodW5nLlZhbi5EdXkiLCJodHRwczovL3RvcGNvZGVyLmNvbS9yb2xlcyI6WyJUb3Bjb2RlciBVc2VyIl0sImh0dHBzOi8vdG9wY29kZXIuY29tL3VzZXJfaWQiOiJhdXRoMHw0MDI5NjMxNiIsImh0dHBzOi8vdG9wY29kZXIuY29tL3Rjc3NvIjoiNDAyOTYzMTZ8ZTFmMWIyNWE4NzkwYWNhYWNlYjkzY2IzZmZmYWUwYzM1MTliNGZlNmQxN2RmZjQ3YmEzOGM1M2E4ZjQxNCIsImh0dHBzOi8vdG9wY29kZXIuY29tL2FjdGl2ZSI6dHJ1ZSwiaHR0cHM6Ly90b3Bjb2Rlci5jb20vb25ib2FyZGluZ193aXphcmQiOiJzaG93IiwiaHR0cHM6Ly90b3Bjb2Rlci5jb20vYmxvY2tJUCI6ZmFsc2UsIm5pY2tuYW1lIjoiUGh1bmcuVmFuLkR1eSIsIm5hbWUiOiIxNDEyMDk1QHN0dWRlbnQuaGNtdXMuZWR1LnZuIiwicGljdHVyZSI6Imh0dHBzOi8vcy5ncmF2YXRhci5jb20vYXZhdGFyLzkwZWIxZmRhYmRmNWFjODhjZjk1NDU2YTZiNDBjODRiP3M9NDgwJnI9cGcmZD1odHRwcyUzQSUyRiUyRmNkbi5hdXRoMC5jb20lMkZhdmF0YXJzJTJGMTQucG5nIiwidXBkYXRlZF9hdCI6IjIwMjItMDktMjNUMDY6Mzk6MzEuMjk4WiIsImVtYWlsIjoiMTQxMjA5NUBzdHVkZW50LmhjbXVzLmVkdS52biIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJpc3MiOiJodHRwczovL2F1dGgudG9wY29kZXIuY29tLyIsInN1YiI6ImF1dGgwfDQwMjk2MzE2IiwiYXVkIjoiVVc3Qmhzbm1BUWgwaXRsNTZnMWpVUGlzQk85R29vd0QiLCJpYXQiOjE2NjM5MTUxNzQsImV4cCI6MTY2NDAwMTU3NCwic2lkIjoic3Y4UEp6bWEzNWlBb1UzVG9teEFiakg3S2dKMVRjb2EiLCJub25jZSI6IloyTkVUUzV5V0RkalJYZENTWGhXYWtjM2JHbE5lamhXYlhBeFRsWXlTVUZVTG5kMlpWcDBVbVJXYmc9PSJ9.u3RHMM-rf3c3AhFDlaKIvuHVC_xDHW6sdGoJzfXZ_GqjJcvvqyH9ixV_WC-YwFiI6-nRpKKxdiGKPjMe0d59sgQHpqjQfJdimz6NMUsDqyKK5Cu5Nw5DxbD1HKBVMfKYz2upX44qE0VpbaM9r4QkPy_usbHPMEsuMyP7cE9EXaPZ_n4XkeHsaKlcg1W6ehjoI8oVOo_pODL6f72MUnOphgYFt5QggdDh3VkMpRPgN_2m6YiVxohDsLWHWtmFfTtgqqh8Gaw7gFQfXwJ3NE_E7mxQa647-5bRCgHODmfXAZawvravivrpNgiWDPUpLkcmG3w9tl5_Z2Q7WILvix5Y-A; v3jwt=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik1rWTNNamsxTWpNeU5Ua3dRalkzTmtKR00wRkZPRVl3TmtJd1FqRXlNVUk0TUVFNE9UQkZOZyJ9.eyJodHRwczovL3RvcGNvZGVyLmNvbS91c2VySWQiOiI0MDI5NjMxNiIsImh0dHBzOi8vdG9wY29kZXIuY29tL2hhbmRsZSI6IlBodW5nLlZhbi5EdXkiLCJodHRwczovL3RvcGNvZGVyLmNvbS9yb2xlcyI6WyJUb3Bjb2RlciBVc2VyIl0sImh0dHBzOi8vdG9wY29kZXIuY29tL3VzZXJfaWQiOiJhdXRoMHw0MDI5NjMxNiIsImh0dHBzOi8vdG9wY29kZXIuY29tL3Rjc3NvIjoiNDAyOTYzMTZ8ZTFmMWIyNWE4NzkwYWNhYWNlYjkzY2IzZmZmYWUwYzM1MTliNGZlNmQxN2RmZjQ3YmEzOGM1M2E4ZjQxNCIsImh0dHBzOi8vdG9wY29kZXIuY29tL2FjdGl2ZSI6dHJ1ZSwiaHR0cHM6Ly90b3Bjb2Rlci5jb20vb25ib2FyZGluZ193aXphcmQiOiJzaG93IiwiaHR0cHM6Ly90b3Bjb2Rlci5jb20vYmxvY2tJUCI6ZmFsc2UsIm5pY2tuYW1lIjoiUGh1bmcuVmFuLkR1eSIsIm5hbWUiOiIxNDEyMDk1QHN0dWRlbnQuaGNtdXMuZWR1LnZuIiwicGljdHVyZSI6Imh0dHBzOi8vcy5ncmF2YXRhci5jb20vYXZhdGFyLzkwZWIxZmRhYmRmNWFjODhjZjk1NDU2YTZiNDBjODRiP3M9NDgwJnI9cGcmZD1odHRwcyUzQSUyRiUyRmNkbi5hdXRoMC5jb20lMkZhdmF0YXJzJTJGMTQucG5nIiwidXBkYXRlZF9hdCI6IjIwMjItMDktMjNUMDY6Mzk6MzEuMjk4WiIsImVtYWlsIjoiMTQxMjA5NUBzdHVkZW50LmhjbXVzLmVkdS52biIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJpc3MiOiJodHRwczovL2F1dGgudG9wY29kZXIuY29tLyIsInN1YiI6ImF1dGgwfDQwMjk2MzE2IiwiYXVkIjoiVVc3Qmhzbm1BUWgwaXRsNTZnMWpVUGlzQk85R29vd0QiLCJpYXQiOjE2NjM5MTUxNzQsImV4cCI6MTY2NDAwMTU3NCwic2lkIjoic3Y4UEp6bWEzNWlBb1UzVG9teEFiakg3S2dKMVRjb2EiLCJub25jZSI6IloyTkVUUzV5V0RkalJYZENTWGhXYWtjM2JHbE5lamhXYlhBeFRsWXlTVUZVTG5kMlpWcDBVbVJXYmc9PSJ9.u3RHMM-rf3c3AhFDlaKIvuHVC_xDHW6sdGoJzfXZ_GqjJcvvqyH9ixV_WC-YwFiI6-nRpKKxdiGKPjMe0d59sgQHpqjQfJdimz6NMUsDqyKK5Cu5Nw5DxbD1HKBVMfKYz2upX44qE0VpbaM9r4QkPy_usbHPMEsuMyP7cE9EXaPZ_n4XkeHsaKlcg1W6ehjoI8oVOo_pODL6f72MUnOphgYFt5QggdDh3VkMpRPgN_2m6YiVxohDsLWHWtmFfTtgqqh8Gaw7gFQfXwJ3NE_E7mxQa647-5bRCgHODmfXAZawvravivrpNgiWDPUpLkcmG3w9tl5_Z2Q7WILvix5Y-A; tcsso=40296316|e1f1b25a8790acaaceb93cb3fffae0c3519b4fe6d17dff47ba38c53a8f414; _cid=undefined; knu=true; ajs_user_id=40296316; km_lv=x; _conv_r=s%3Agithub.com*m%3Areferral*t%3A*c%3A; km_vs=1; _hp2_ses_props.4240734066=%7B%22ts%22%3A1663953938114%2C%22d%22%3A%22community.topcoder.com%22%2C%22h%22%3A%22%2Fstat%22%7D; _conv_v=vi%3A1*sc%3A5*cs%3A1663951715*fs%3A1663173268*pv%3A57*exp%3A%7B100030166.%7Bv.1000199527-g.%7B100026706.1-100026707.1-100027342.1%7D%7D-100030395.%7Bv.1000200193-g.%7B%7D%7D%7D*ps%3A1663941179; kvcd=1663956451832; _ga_ZYJTERS9DX=GS1.1.1663953934.6.1.1663956452.0.0.0; _ga=GA1.1.1371554180.1663173267; _hp2_id.4240734066=%7B%22userId%22%3A%22138067581782230%22%2C%22pageviewId%22%3A%221089055819894497%22%2C%22sessionId%22%3A%223803406840918144%22%2C%22identity%22%3A%2240296316%22%2C%22trackerVersion%22%3A%224.0%22%2C%22identityField%22%3Anull%2C%22isIdentified%22%3A1%7D',
    'sec-ch-ua': '"Google Chrome";v="105", "Not)A;Brand";v="8", "Chromium";v="105"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36',
}






class TopCoder(Scraper):

    def get_all_problems(self):

        html = requests.get(TOPCODER_ARCHIVE_URL).text
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table', {'class': 'paddingTable2'})
        table = None
        for tb in tables:
            if "SRM" in str(tb):
                table = tb
                break
        # find all tr contain tag <a>
        all_tr = table.find_all('tr')
        all_tr_a = []
        for tr in all_tr:
            if tr.find('a'):
                all_tr_a.append(tr)
        all_tr_a = all_tr_a[2:] # skip two first decorator <tr>
        
        lst_url = []
        lst_name = []

        for tr  in all_tr_a:
            url = tr.find_all('a')[0].get('href')
            if 'problem_statement' in url:
                url = url.replace('amp;', '')
                url = 'https://community.topcoder.com/' + url
                name = tr.find_all('a')[0].text.strip()
                lst_url.append(url)
                lst_name.append(name)
        return lst_url, lst_name


    def get_problem_solution(self, url):
        html = requests.get(url).text
        soup = BeautifulSoup(html, 'html.parser')
        problem_statement = soup.find('td', {'class': 'problemText'}).text.strip()
        solution_url = 'https://community.topcoder.com/' + soup.find_all('td', {'class':"statText"})[-1].find('a').get('href')
        html = requests.get(solution_url).text
        soup = BeautifulSoup(html, 'html.parser')
        sol_urls = []
        for td in soup.find_all('td', {'class':"statText"}):
            if td.find('a'):
                url = 'https://community.topcoder.com/' + td.find('a').get('href')
                if 'stat?c=problem_solution' in url:
                    sol_urls.append(url)
        solutions = []
        for url in sol_urls: # to crawl solution require login need to get cookies from browser
        
            params = {
                'c': 'problem_solution'
            }
            elements = url.split('&')[1:]
            for e in elements:
                k, v = e.split('=')
                params[k] = v
            html = requests.get(url, params=params, headers=HEADERS, cookies=COOKIES).text
            solutions.append(html)
        return problem_statement, solutions


    def scrape(self) -> RawDataset:
        all_url, all_name = self.get_all_problems()
        all_problem = []
        all_solution = []
        for (i, url) in tqdm(enumerate(all_url), total=len(all_url)):
            problem_statement, solutions = self.get_problem_solution(url)
            all_problem.append(problem_statement)
            all_solution.append(solutions)
            if (i + 1) % 100 == 0:
                df = pd.DataFrame({'problem': all_problem, 'solution': all_solution, 'url': all_url, 'name': all_name})
                df.to_parquet(os.path.join(self.target_dir, 'topcoder.parquet'))
        df = pd.DataFrame({'problem': all_problem, 'solution': all_solution, 'url': all_url, 'name': all_name})
        df.to_parquet(os.path.join(self.target_dir, 'topcoder.parquet'))
        return RawDataset(storage_uris=['file:///{self.target_dir}'])



class TopCoderDataset(Dataset):
    def __init__(self, tempdir, target_dir):
        self.scraper = TopCoder(tempdir, target_dir)
    def download(self):
        self.scraper.scrape()



if __name__=="__main__":

    topcoder = TopCoderDataset('data/', 'data/')
    topcoder.download()