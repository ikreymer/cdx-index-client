#!/usr/bin/env python

from argparse import ArgumentParser
from Queue import Empty
from multiprocessing import Process, Queue, Value, cpu_count

import requests
import shutil
import urllib
import sys
import signal
import random
import os

import logging

from urlparse import urljoin
from bs4 import BeautifulSoup

DEF_API_BASE = 'http://index.commoncrawl.org/'


def get_index_urls(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "lxml")
    return [urljoin(url, a.attrs.get("href") + "-index") for a in soup.select("a") if "/CC-MAIN-" in a.attrs.get("href")]


def get_num_pages(api_url, url, page_size=None):
    """ Use the showNumPages query
    to get the number of pages in the result set
    """
    query = {'url': url,
             'showNumPages': True}

    if page_size:
        query['pageSize'] = page_size

    query = urllib.urlencode(query)

    # Get the result
    session = requests.Session()
    r = session.get(api_url + '?' + query)
    pages_info = r.json()

    if isinstance(pages_info, dict):
        return pages_info['pages']
    elif isinstance(pages_info, int):
        return pages_info
    else:
        msg = 'Num pages query returned invalid data: ' + r.text
        raise Exception(msg)


def fetch_result_page(job_params):
    """ query the api, getting the specified
    results page and write to output file
    for that page
    """
    api_url = job_params['api_url']
    url = job_params['url']
    page = job_params['page']
    num_pages = job_params['num_pages']
    output_prefix = job_params['output_prefix']
    timeout = job_params['timeout']
    gzipped = job_params['gzipped']
    headers = job_params['headers']
    dir_ = job_params['dir']

    query = {'url': url,
             'page': page}

    if job_params.get('json'):
        query['output'] = 'json'

    if job_params.get('fl'):
        query['fl'] = job_params['fl']

    if job_params.get('page_size'):
        query['pageSize'] = job_params['page_size']

    query = urllib.urlencode(query)

    # format filename to number of digits
    nd = len(str(num_pages))
    format_ = '%0' + str(nd) + 'd'
    page_str = format_ % page
    filename = output_prefix + page_str

    logging.debug('Fetching page {0} ({2} of {1})'.format(
        page_str, num_pages, page + 1))

    # Add any custom headers that may have been specified
    req_headers = {}
    if headers:
        for h in headers:
            n, v = h.split(':', 1)
            n = n.strip()
            v = v.strip()
            req_headers[n] = v

    # Get the result
    session = requests.Session()
    r = session.get(api_url + '?' + query, headers=req_headers,
                    stream=True, timeout=timeout)

    if r.status_code == 404:
        logging.error('No Results for for this query')
        r.close()
        return

    if r.status_code != 200:
        r.raise_for_status()
        r.close()
        return

    # use dir, if provided
    if dir_:
        if not os.path.isdir(dir_):
            os.makedirs(dir_)
        filename = os.path.join(dir_, filename)

    if not gzipped:
        with open(filename, 'w+b') as fh:
            for chunk in r.iter_content(1024):
                fh.write(chunk)
    else:
        if r.headers.get('content-encoding') == 'gzip':
            filename += '.gz'

        with open(filename, 'w+b') as fh:
            shutil.copyfileobj(r.raw, fh)

    logging.debug('Done with "{0}"'.format(filename))


def do_work(job_queue, counter=None):
    """ Process work function, read more fetch page jobs
    from queue until all jobs are finished
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    while not job_queue.empty():
        try:
            job = job_queue.get_nowait()
            fetch_result_page(job)

            num_done = 0
            with counter.get_lock():
                counter.value += 1
                num_done = counter.value

            logging.info('{0} page(s) of {1} finished'.format(num_done,
                                                              job['num_pages']))
        except Empty:
            pass

        except KeyboardInterrupt:
            break

        except Exception:
            if not job:
                raise

            retries = job.get('retries', 0)
            if retries < job['max_retries']:
                logging.error('Retrying Page {0}'.format(job['page']))
                job['retries'] = retries + 1
                job_queue.put_nowait(job)
            else:
                logging.error('Max retries exceeded for page {0}'.
                              format(job['page']))


def run_workers(num_workers, jobs, shuffle):
    """ Queue up all jobs start workers with job_queue
    catch KeyboardInterrupt to allow interrupting all workers
    Not using Pool to better hande KeyboardInterrupt gracefully
    Adapted from example at:
    http://bryceboe.com/2012/02/14/python-multiprocessing-pool-and-keyboardinterrupt-revisited/
    """

    # Queue up all jobs
    job_queue = Queue()
    counter = Value('i', 0)

    # optionally shuffle queue
    if shuffle:
        jobs = list(jobs)
        random.shuffle(jobs)

    for job in jobs:
        job_queue.put(job)

    workers = []

    for i in xrange(0, num_workers):
        tmp = Process(target=do_work,
                      args=(job_queue, counter))
        tmp.start()
        workers.append(tmp)

    try:
        for worker in workers:
            worker.join()
    except KeyboardInterrupt:
        logging.info('Received Ctrl-C, interrupting all workers')
        for worker in workers:
            worker.terminate()
            worker.join()
        raise


def get_args():
    url_help = """
    url to query in the index:
    For prefix, use:
    http://example.com/*

    For domain query, use:
    *.example.com
    """

    field_list_help = """
    select fields to include: eg, --fl url,timestamp
    """

    parser = ArgumentParser('CDX Index API Client')

    parser.add_argument('url',
                        help=url_help)

    parser.add_argument('-n', '--show-num-pages', action='store_true',
                        help='Show Number of Pages only and exit')

    parser.add_argument('-p', '--processes', type=int,
                        help='Number of worker processes to use')

    parser.add_argument('--fl',
                        help=field_list_help)

    parser.add_argument('-j', '--json', action='store_true',
                        help='Use json output instead of cdx(j)')

    parser.add_argument('-z', '--gzipped', action='store_true',
                        help='Storge gzipped results, with .gz extensions')

    parser.add_argument('-o', '--output-prefix',
                        help='Custom output prefix, append with -NN for each page')

    parser.add_argument('-d', '--directory',
                        help='Specify custom output directory')

    parser.add_argument('--page-size', type=int,
                        help='size of each page in blocks, >=1')

    group = parser.add_mutually_exclusive_group()
    group.add_argument('-c', '--coll',
                       help=('The index collection to use or '+
                        '"all" to use all available indexes. '+
                       'The default value is the most recent available index'))

    group.add_argument('--cdx-server-url',
                       help='Set endpoint for CDX Server API')

    parser.add_argument('--timeout', default=30, type=int,
                        help='HTTP read timeout before retry')

    parser.add_argument('--max-retries', default=5, type=int,
                        help='Number of retry attempts')

    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Verbose logging of debug msgs')

    parser.add_argument('--pages', type=int, nargs='*',
                        help=('Get only the specified result page(s) instead ' +
                              'of all results'))

    parser.add_argument('--header', nargs='*',
                        help='Add custom header to request')

    parser.add_argument('--in-order', action='store_true',
                        help='Fetch pages in order (default is to shuffle page list)')

    r = parser.parse_args()

    if not r.coll and not r.cdx_server_url:
        api_urls = get_index_urls(DEF_API_BASE)
        r.cdx_server_url = api_urls[0]

    # Logging
    if r.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(format='%(asctime)s: [%(levelname)s]: %(message)s',
                        level=level)

    logging.getLogger("requests").setLevel(logging.WARNING)

    return r


def read_index(r, prefix=None):

    if r.cdx_server_url:
        api_url = r.cdx_server_url
    else:
        api_url = DEF_API_BASE + r.coll + '-index'

    logging.debug('Getting Index From ' + api_url)

    logging.debug('Getting Num Pages...')
    num_pages = get_num_pages(api_url, r.url, r.page_size)

    # Num Pages Only Query
    if r.show_num_pages:
        print(num_pages)
        return

    if num_pages == 0:
        print('No results found for: ' + r.url)

    # set output
    if not r.output_prefix:
        if r.url.startswith('*'):
            output_prefix = 'domain-' + r.url.strip('*.')
        elif r.url.endswith('*'):
            output_prefix = 'prefix-' + r.url.strip('*')
        elif r.url.startswith(('http://', 'https://', '//')):
            output_prefix = r.url.split('//', 1)[-1]
        else:
            output_prefix = r.url

        output_prefix = output_prefix.strip('/')
        output_prefix = output_prefix.replace('/', '-')
        output_prefix = urllib.quote(output_prefix) + '-'
    else:
        output_prefix = r.output_prefix

    if prefix:
        output_prefix += prefix

    def get_page_job(page):
        job = {}
        job['api_url'] = api_url
        job['url'] = r.url
        job['page'] = page
        job['num_pages'] = num_pages
        job['output_prefix'] = output_prefix
        job['fl'] = r.fl
        job['json'] = r.json
        job['page_size'] = r.page_size
        job['timeout'] = r.timeout
        job['max_retries'] = r.max_retries
        job['gzipped'] = r.gzipped
        job['headers'] = r.header
        job['dir'] = r.directory
        return job

    if r.pages:
        page_list = r.pages
        logging.info('Fetching pages {0} of {1}'.format(r.pages, r.url))
        num_pages = len(page_list)
    else:
        page_list = range(0, num_pages)
        logging.info('Fetching {0} pages of {1}'.format(num_pages, r.url))

    if num_pages == 1:
        fetch_result_page(get_page_job(page_list[0]))
        return

    # set num workers based on proesses
    if not r.processes:
        try:
            num_workers = cpu_count() * 2
        except NotImplementedError:
            num_workers = 4
    else:
        num_workers = r.processes

    num_workers = min(num_workers, num_pages)

    # generate page jobs
    job_list = map(get_page_job, page_list)

    run_workers(num_workers, job_list, not r.in_order)


def main():
    r = get_args()
    if r.coll == "all":
        api_urls = get_index_urls(DEF_API_BASE)
        for api_url in api_urls:
            r.cdx_server_url = api_url
            prefix = (api_url.split('/')[-1])[0:-6] + '-'
            read_index(r, prefix)
    else:
        read_index(r)


if __name__ == "__main__":
    main()
