from bs4 import BeautifulSoup as bs
import requests
import hashlib
import json
import os
import sys
import queue
import threading
import shutil
import glob
import typing
import enforce
import string
import numpy as np


class Author:
    def __init__(self,
                 name: str,
                 url: str,
                 publications: typing.Union[typing.List[str], None]) -> None:
        self.name = name
        self.url = url
        self.id = hashlib.md5(url.encode()).hexdigest()
        if publications is None:
            self.publications: typing.List[str] = []
        else:
            self.publications = publications

    def add_publication(self, publication_id):
        self.publications.append(publication_id)


class HostPublication(object):
    def __init__(self, name: str, url: typing.Union[str, None]) -> None:
        self.url: str = url
        hash_input: bytes = bytes(name.encode()) if url is None else self.url
        self.id: str = hashlib.md5(hash_input).hexdigest()
        self.name = name


class Publication(object):
    def __init__(self,
                 publication_url: str,
                 authors: typing.List[Author],
                 abstract: str,
                 title: str,
                 host_publication: HostPublication
                 ) -> None:
        self.publication_url: str = publication_url
        self.id = hashlib.md5(publication_url.encode()).hexdigest()
        self.authors: typing.List[str] = [x.id for x in authors]
        self.abstract: str = abstract
        self.title: str = title
        self.host_publication: str = host_publication.id


class DownloadWorker(threading.Thread):
    def __init__(self, download_queue):
        threading.Thread.__init__(self)
        self.queue = download_queue

    def run(self):
        while True:
            page_index: int = self.queue.get()
            try:
                get_page_urls(page_index)
            except RuntimeError as e:
                print(f"{e} for Page #{page_index}")
                continue
            finally:
                self.queue.task_done()


def get_page_urls(page_number: int, finding_new: bool = False) -> typing.Union[None, dict]:
    response = requests.get(f"http://research-repository.uwa.edu.au/en/publications/search.html"
                            f"?pageSize=500&"
                            f"page={page_number}")
    if response.status_code != 200:
        raise RuntimeError('Http Response != 200 OK.')
    page = response.text
    soup = bs(page, 'lxml')
    list_items = soup.select('li.portal_list_item')
    page_urls = {}
    for li in list_items:
        link = li.find("a", {"class": "link"})['href']
        type_classification = li.find("span", {"class": "type_classification"}).text
        publication_id: str = hashlib.md5(link.encode()).hexdigest()
        page_urls[publication_id] = {
            "url": link, "type_classification": type_classification
        }
    if not finding_new:
        with open(f'temp_urls/publication_urls_{page_number}.json', 'w') as f:
            json.dump(page_urls, f, indent=2)

            sys.stdout.write(f"{len(os.listdir('temp_urls'))} Pages Scraped, "
                             f"Page #{page_number} Scraped... "
                             f"{len(page_urls)} Publications Scraped.    \r")
            sys.stdout.flush()
        return None
    else:
        return page_urls


def get_all_publication_urls() -> None:
    # remove the current temp directory and create a new one
    if os.path.exists('temp_urls'):
        shutil.rmtree('temp_urls')
    os.mkdir('temp_urls')

    # get the number of pages
    page = requests.get('http://research-repository.uwa.edu.au/en/publications/search.html?pageSize=500').text
    soup = bs(page, 'lxml')
    paging: bs.element.Tag = soup.find('span', {'class': 'portal_navigator_paging'})
    pages: int = int(paging.select('a span')[-1].text)

    print(f'Starting Url Scraping... {pages} Pages to Scrape')

    # create the queue used for multi-threading
    q: queue.Queue = queue.Queue()
    for i in range(5):
        worker: threading.Thread = DownloadWorker(q)
        worker.setDaemon(True)
        worker.start()

    # add the page numbers to the queue
    for i in range(pages):
        q.put(i)

    # run the queue
    q.join()

    # combine all the json files to create the publications document
    publications: dict = {}
    for temp_file in glob.glob('temp_urls/*'):
        page_urls: dict = json.load(open(temp_file))
        publications.update(page_urls)
    with open('publications_urls.json', 'w') as f:
        json.dump(publications, f, indent=2)

    shutil.rmtree('temp_urls')


def check_for_new_publications() -> None:
    with open('publications_urls.json', 'r') as f:
        publications = json.load(f)

    page_index = 0
    new_publications: int = 0
    new_publication_ids: typing.List[str] = []
    while True:
        page_urls: dict = get_page_urls(page_index, finding_new=True)
        duplicate: int = 0
        for publication_id, publication_meta in page_urls.items():
            if publication_id not in publications:
                publications[publication_id] = publication_meta
                new_publications += 1
                new_publication_ids.append(publication_id)
            else:
                duplicate += 1
        # assert that all the new publications found are in the publications dict.
        assert (all(x in publications for x in page_urls.keys()))
        json.dump(publications, open('publications_urls.json', 'w'), indent=2)
        if duplicate != 0:
            break
        page_index += 1
    print(f"Found {new_publications} New Publications... New Publication ids: {new_publication_ids}")


def get_type_classifications():
    publication_urls = json.load(open('data/publications_urls.json'))
    type_classifications = {}
    for _, pub in publication_urls.items():
        type_classification = pub['type_classification']
        if type_classification not in type_classifications:
            type_classifications[type_classification] = 0
        type_classifications[type_classification] += 1
    for type_classification, frequency in type_classifications.items():
        print(f"Classification: {type_classification}, Frequency: {frequency}")


def get_publication(url: str) -> typing.Tuple[typing.Union[Publication, str],
                                              typing.Union[typing.List[Author], None],
                                              typing.Union[HostPublication, None]]:
    response = requests.get(url)
    if response.status_code != 200:
        raise RuntimeError('Status Code != 200')
    page = response.text
    soup = bs(page, 'lxml')
    title: str = soup.find("h2", {"class": "title"}).text.lower()
    authors: typing.List[Author] = [Author(x.text,
                                           x['href'],
                                           publications=None) for x in soup.find_all("a", {"class": "person"})]
    try:
        abstract_vanilla: str = soup.find("div", {"class": "textblock"}).text
    except AttributeError:
        return hashlib.md5(url.encode()).hexdigest(), None, None
    abstract = remove_punctuation(abstract_vanilla).lower()
    table_rows = soup.find_all("tr")
    table = {}
    for row in table_rows:
        table[row.find("th").text] = {"text": row.find("td").text,
                                      "href": row.find("td")["href"] if "href" in row.find("td") else None}
    if 'Title of host publication' in table:
        host_publication_meta = table['Title of host publication']
    elif 'Journal' in table:
        host_publication_meta = table['Journal']
    else:
        return hashlib.md5(url.encode).hexdigest(), None, None
    host_publication = HostPublication(host_publication_meta["text"], host_publication_meta["href"])
    publication = Publication(url, authors, abstract, title, host_publication)
    return publication, authors, host_publication


def get_publications():
    publication_urls = json.load(open('publications_urls.json', 'r'))

    # if the publications file exists load the publications
    publications: typing.Dict[str, dict] = {}
    if os.path.exists('data/publications.npy'):
        publications = np.load('data/publications.npy').item()

    # if the host publcations file eists load the host publcationss
    host_publications: typing.Dict[str, dict] = {}
    if os.path.exists('data/host_publications.npy'):
        host_publications = np.load('data/host_publications.npy').item()

    # if the authors file exists, load the authors
    authors: typing.Dict[str, dict] = {}
    if os.path.exists('data/authors.npy'):
        authors = np.load('data/authors.npy').item()

    cached_urls = [x['publication_url'] for _, x in publications.items()]

    for i, (_, publication_url) in enumerate(publication_urls.items()):
        if i == 10000:
            break
        # check if the data for that particular url has already been scraped
        if publication_url['url'] in cached_urls:
            continue
        sys.stdout.write(f"Fetching Publication #{i} of {len(publication_urls)}   \r")
        sys.stdout.flush()
        if publication_url['type_classification'] not in ['Article', 'Conference Paper']:
            continue
        if len(publications) % 100 == 0:
            print('Data Written to Disk...                                   ')
            write_data_to_disk(authors, host_publications, publications)
        publication_data: typing.Tuple[typing.Union[str,
                                                    Publication],
                                       typing.Union[None,
                                                    typing.List[Author]],
                                       typing.Union[None,
                                                    HostPublication]] = get_publication(publication_url['url'])
        if any([x is None for x in publication_data]):
            publication_id, _, _ = publication_data
            publications[publication_id] = {"publication_url": publication_url['url']}
            continue
        publication, publication_authors, host_publication = publication_data
        publications[publication.id] = vars(publication)
        # add the host publication if not already contained
        if host_publication.id not in host_publications:
            host_publications[host_publication.id] = vars(host_publication)
        # for all authors, add the author if not already added
        for author in publication_authors:
            if author.id not in authors:
                authors[author.id] = vars(author)

    write_data_to_disk(authors, host_publications, publications)


def write_data_to_disk(authors, host_publications, publications):
    np.save('data/publications.npy', publications)
    json.dump(publications, open('data/publications.json', 'w'), indent=2)
    # save the host publications to disk
    np.save('data/host_publications.npy', host_publications)
    json.dump(host_publications, open('data/host_publications.json', 'w'), indent=2)
    # save the authors to disk
    np.save('data/authors.npy', authors)
    json.dump(authors, open('data/authors.json', 'w'), indent=2)


@enforce.runtime_validation
def remove_punctuation(input_string: str) -> str:
    translator = str.maketrans('', '', string.punctuation)
    return input_string.translate(translator)


get_publications()
