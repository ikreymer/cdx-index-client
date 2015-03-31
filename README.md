# CommonCrawl Index Client (CDX Index API Client)

A simple python command line tool for retrieving a list of urls in bulk using the CommonCrawl Index API at http://index.commoncrawl.org (or any other web archive CDX Server API).

The tool takes advantage of the [CDX Server Pagination API](https://github.com/ikreymer/pywb/wiki/CDX-Server-API#pagination-api) and the Python `multiprocessing` support
to load pages (chunks) of a large url index in parallel.

This may be especially useful for prefix/domain extraction.

To use, first install dependencies: `pip install -r requirements.txt` (The script has been tested on Python 2.7.x)

For example, fetch all entries in the index for url `http://iana.org/` from index `CC-MAIN-2015-06`, run:
`./cdx-index-client.py CC-MAIN-2015-06 http://iana.org/`

It is often good idea to check how big the dataset is:
`./cdx-index-client.py CC-MAIN-2015-06 *.io --show-num-pages`

will print the number of pages that will be fetched to get a list of urls in the '*.io' domain.

This will give a relative size of the query. A query with thousands of pages may take a long time!

Then, you might fetch a list of urls from the index which are part of the *.io domain, as follows:

`./cdx-index-client.py CC-MAIN-2015-06 *.io -fl url -z`

The `-fl` flag specifies that only the `url` should be fetched, instead of the entire index row.

The `-z` flag indicates to store the data compressed.

For the above query, the output will be stored in `domain-io-N.gz` where for each page `N` (padded to number of digits)

To for a full listing of options, run: `./cdx-index-client.py --help`

## Additional Use Cases

While this tool was designed specifically for use with the index at http://index.commoncrawl.org, it can also be used with any other running cdx server, including pywb, OpenWayback and IA Wayback.

The client uses a common subset of [pywb CDX Server API](https://github.com/ikreymer/pywb/wiki/CDX-Server-API) and the original [IA Wayback CDX Server API](https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server) and so should work with either of these tools.

To specify a custom api endpoint, simply use the `--cdx-server-url` flag. For example, to connect to a locally running server, you can run:

`./cdx-index-client.py example.com/* --cdx-server-url http://localhost:8080/pywb-cdx`
