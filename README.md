# CommonCrawl Index Client (CDX Index API Client)

A simple python command line tool for retrieving a list of urls in bulk using the CommonCrawl Index API at http://index.commoncrawl.org (or any other web archive CDX Server API).

## Examples

The tool takes advantage of the [CDX Server Pagination API](https://github.com/ikreymer/pywb/wiki/CDX-Server-API#pagination-api) and the Python `multiprocessing` support
to load pages (chunks) of a large url index in parallel.

This may be especially useful for prefix/domain extraction.

To use, first install dependencies: `pip install -r requirements.txt` (The script has been tested on Python 2.7.x)

For example, fetch all entries in the index for url `http://iana.org/` from index `CC-MAIN-2015-06`, run:
`./cdx-index-client.py -c CC-MAIN-2015-06 http://iana.org/`

It is often good idea to check how big the dataset is:
`./cdx-index-client.py -c CC-MAIN-2015-06 *.io --show-num-pages`

will print the number of pages that will be fetched to get a list of urls in the '*.io' domain.
This will give a relative size of the query. A query with thousands of pages may take a long time!

Then, you might fetch a list of urls from the index which are part of the *.io domain, as follows:

`./cdx-index-client.py -c CC-MAIN-2015-06 *.io --fl url -z`

The `-fl` flag specifies that only the `url` should be fetched, instead of the entire index row.

The `-z` flag indicates to store the data compressed.

For the above query, the output will be stored in `domain-io-N.gz` where for each page `N` (padded to number of digits)

## Usage Options

Below is the current list of options, also available by running `./cdx-index-client.py -h`

```
usage: CDX Index API Client [-h] [-n] [-p PROCESSES] [--fl FL] [-j] [-z]
                            [-o OUTPUT_PREFIX] [-d DIRECTORY]
                            [--page-size PAGE_SIZE]
                            [-c COLL | --cdx-server-url CDX_SERVER_URL]
                            [--timeout TIMEOUT] [--max-retries MAX_RETRIES]
                            [-v] [--pages [PAGES [PAGES ...]]]
                            [--header [HEADER [HEADER ...]]] [--in-order]
                            url

positional arguments:
  url                   url to query in the index: For prefix, use:
                        http://example.com/* For domain query, use:
                        *.example.com

optional arguments:
  -h, --help            show this help message and exit
  -n, --show-num-pages  Show Number of Pages only and exit
  -p PROCESSES, --processes PROCESSES
                        Number of worker processes to use
  --fl FL               select fields to include: eg, --fl url,timestamp
  -j, --json            Use json output instead of cdx(j)
  -z, --gzipped         Storge gzipped results, with .gz extensions
  -o OUTPUT_PREFIX, --output-prefix OUTPUT_PREFIX
                        Custom output prefix, append with -NN for each page
  -d DIRECTORY, --directory DIRECTORY
                        Specify custom output directory
  --page-size PAGE_SIZE
                        size of each page in blocks, >=1
  -c COLL, --coll COLL  The index collection to use
  --cdx-server-url CDX_SERVER_URL
                        Set endpoint for CDX Server API
  --timeout TIMEOUT     HTTP read timeout before retry
  --max-retries MAX_RETRIES
                        Number of retry attempts
  -v, --verbose         Verbose logging of debug msgs
  --pages [PAGES [PAGES ...]]
                        Get only the specified result page(s) instead of all
                        results
  --header [HEADER [HEADER ...]]
                        Add custom header to request
  --in-order            Fetch pages in order (default is to shuffle page list)
```

If --coll and --cdx-server-url are unset cdx-index-client.py does a loop over all index available in http://index.commoncrawl.org

## Additional Use Cases

While this tool was designed specifically for use with the index at http://index.commoncrawl.org, it can also be used with any other running cdx server, including pywb, OpenWayback and IA Wayback.

The client uses a common subset of [pywb CDX Server API](https://github.com/ikreymer/pywb/wiki/CDX-Server-API) and the original [IA Wayback CDX Server API](https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server) and so should work with either of these tools.

To specify a custom api endpoint, simply use the `--cdx-server-url` flag. For example, to connect to a locally running server, you can run:

`./cdx-index-client.py example.com/* --cdx-server-url http://localhost:8080/pywb-cdx`
