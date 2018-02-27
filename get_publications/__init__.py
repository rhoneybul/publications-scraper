from bs4 import BeautifulSoup as bs

def get_all_urls():
    url = 'http://research-repository.uwa.edu.au/en/publications/search.html?page=0&pageSize=500'
    soup = bs(url, 'lxml')
    list_items = soup.select('li.portal_list_item')
    print(list_items)