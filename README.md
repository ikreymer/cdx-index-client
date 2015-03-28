# CommonCrawl Index Client


A sample command line tool retrieving a list of urls in bulk using the CommonCrawl Index API (http://index.commoncrawl.org)

The tool takes advantage on the pagination API and python `multiprocessing` module to load multiple parts of the index in chunks.

This may be especially useful for prefix/domain extraction.

To use, first install dependencies: `pip install -r requirements.txt`

For example, fetch all urls from the index starting with `http://iana.org/`
`python cc-index-client.py http://iana.org/*`

It is often good idea to check how big the dataset is:
`python cc-index-client.py *.io --num-pages`

will print the number of pages that will be fetched to get a list of urls in the '*.io' domain.

Fetch a list of urls from the index which are part of the *.io domain (note that this may be a lot!)

`python cc-index-client.py *.io`


Run `python cc-index-client.py --help` for a full list of options.

