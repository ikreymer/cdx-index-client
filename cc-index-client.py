#!/usr/bin/python

from argparse import ArgumentParser
from Queue import Empty
from multiprocessing import Process, Queue, Value

import requests
import urllib
import sys
import signal
import random

import logging


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
    return pages_info['pages']

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

    query = {'url': url,
             'page': page}

    if not job_params.get('cdxj'):
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

    logging.debug('Fetching page {0} ({2} of {1})'.format(page_str, num_pages, page + 1))

    # Get the result
    session = requests.Session()
    r = session.get(api_url + '?' + query, stream=True, timeout=timeout)

    if r.status_code == 404:
        logging.error('No Results for for this query')
        r.close()
        return

    if r.status_code != 200:
        logging.error(r.text)
        r.close()
        return

    #print('Begin writing page {0} -> "{2}"'.format(page_str, num_pages, filename))
    with open(filename, 'w+b') as fh:
        for chunk in r.iter_content(1024):
            fh.write(chunk)

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

        except requests.exceptions.ConnectionError:
            if not job:
                raise

            retries = job.get('retries', 0)
            if retries < job['max_retries']:
                logging.debug('Retrying Page {0}'.format(job['page']))
                job['retries'] = retries + 1
                job_queue.put_nowait(job)
            else:
                logging.error('Max retries exceeded for page {0}'.
                              format(job['page']))


def run_workers(num_workers, jobs):
    """ Queue up all jobs start workers with job_queue
    catch KeyboardInterrupt to allow interrupting all workers
    Not using Pool to better hande KeyboardInterrupt gracefully
    Adapted from example at:
    http://bryceboe.com/2012/02/14/python-multiprocessing-pool-and-keyboardinterrupt-revisited/
    """

    # Queue up all jobs
    job_queue = Queue()
    counter = Value('i', 0)

    # shuffle queue to spread load (make this an option?)
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
        logging.error('Received Ctrl-C, interrupting all workers')
        for worker in workers:
            worker.terminate()
            worker.join()


def main():
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

    parser = ArgumentParser()
    parser.add_argument('url', help=url_help)
    parser.add_argument('--page-size', type=int)
    parser.add_argument('-n', '--show-num-pages', action='store_true')
    parser.add_argument('-p', '--processes', type=int)
    parser.add_argument('--fl', help=field_list_help)
    parser.add_argument('-c', '--cdxj', action='store_true', default=True)
    parser.add_argument('-o', '--output-prefix')
    parser.add_argument('--collection',
                        default='CC-MAIN-2015-06')

    parser.add_argument('--api-url',
                        default='http://index.commoncrawl.org/')

    parser.add_argument('--timeout', default=30, type=int,
                        help='HTTP read timeout before retry')

    parser.add_argument('--max-retries', default=5, type=int,
                        help='Number of retry attempts')

    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Verbose logging of debug msgs')

    parser.add_argument('--pages', type=int, nargs='*',
                        help='Get only specified page(s)')

    # Logging
    r = parser.parse_args()

    if r.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO


    logging.basicConfig(format='%(asctime)s: [%(levelname)s]: %(message)s',
                        level=level)

    logging.getLogger("requests").setLevel(logging.WARNING)


    api_url = r.api_url + r.collection + '-index'

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
            prefix = 'domain-' + r.url.strip('*.')
        elif r.url.endswith('*'):
            prefix = 'prefix-' + r.url.strip('*')
        else:
            prefix = r.url

        prefix = prefix.strip('/')
        prefix = prefix.replace('/', '-')
        prefix = urllib.quote(prefix) + '-'
    else:
        prefix = r.output_prefix

    def get_page_job(page):
        job = {}
        job['api_url'] = api_url
        job['url'] = r.url
        job['page'] = page
        job['num_pages'] = num_pages
        job['output_prefix'] = prefix
        job['fl'] = r.fl
        job['cdxj'] = r.cdxj
        job['page_size'] = r.page_size
        job['timeout'] = r.timeout
        job['max_retries'] = r.max_retries
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

    run_workers(num_workers, job_list)


if __name__ == "__main__":
    main()
