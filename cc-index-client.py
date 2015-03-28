from argparse import ArgumentParser
from Queue import Empty
from multiprocessing import Pool, Process, Queue
from requests.adapters import HTTPAdapter

import requests
import urllib
import sys
import signal
import random


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
    r = requests.get(api_url + '?' + query)
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

    print('Fetching page {0} of {1}'.format(page_str, num_pages))

    # Get the result
    r = requests.get(api_url + '?' + query, stream=True, timeout=timeout)

    if r.status_code == 404:
        print('No Results for for this query')
        r.close()
        return

    if r.status_code != 200:
        print(r.text)
        r.close()
        return

    #print('Begin writing page {0} -> "{2}"'.format(page_str, num_pages, filename))
    with open(filename, 'w+b') as fh:
        for chunk in r.iter_content(1024):
            fh.write(chunk)

    print('Done with "{0}"'.format(filename))


def do_work(job_queue):
    """ Process work function, read more fetch page jobs
    from queue until all jobs are finished
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    while not job_queue.empty():
        try:
            job = job_queue.get_nowait()
            #print(job)
            fetch_result_page(job)
        except Empty:
            pass

        except requests.exceptions.ConnectionError:
            if job:
                retries = job.get('retries', 0)
                if retries < 5:
                    print('Retrying Page {0}'.format(job['page']))
                    job['retries'] = retries + 1
                    job_queue.put_nowait(job)


def run_workers(num_workers, jobs):
    """ Queue up all jobs start workers with job_queue
    catch KeyboardInterrupt to allow interrupting all workers
    Not using Pool to better hande KeyboardInterrupt gracefully
    Adapted from example at:
    http://bryceboe.com/2012/02/14/python-multiprocessing-pool-and-keyboardinterrupt-revisited/
    """

    # Queue up all jobs
    job_queue = Queue()
    jobs = list(jobs)
    random.shuffle(jobs)
    for job in jobs:
        job_queue.put(job)

    workers = []

    for i in xrange(0, num_workers):
        tmp = Process(target=do_work,
                      args=(job_queue,))
        tmp.start()
        workers.append(tmp)

    try:
        for worker in workers:
            worker.join()
    except KeyboardInterrupt:
        print 'Received Ctrl-C, interrupting all workers'
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
    parser.add_argument('--num-pages', dest='num_pages_only', action='store_true')
    parser.add_argument('-p', '--processes', type=int, default=10)
    parser.add_argument('--fl', help=field_list_help)
    parser.add_argument('-c', '--cdxj', action='store_true', default=True)
    parser.add_argument('-o', '--output-prefix', default='results-')
    parser.add_argument('--collection',
                        default='CC-MAIN-2015-06')

    parser.add_argument('--api-url',
                        default='http://index.commoncrawl.org/')

    parser.add_argument('--timeout', help='http timeout before retrying')

    r = parser.parse_args()

    api_url = r.api_url + r.collection + '-index'

    print('Getting Num Pages...')
    num_pages = get_num_pages(api_url, r.url, r.page_size)
    print('Found {0}'.format(num_pages))

    # Num Pages Only Query
    if r.num_pages_only:
        print(num_pages)
        return

    if num_pages == 0:
        print('No results found for: ' + r.url)
        sys.exit(1)

    def generate_fetch_job():
        for page in xrange(0, num_pages):
            job = {}
            job['api_url'] = api_url
            job['url'] = r.url
            job['page'] = page
            job['num_pages'] = num_pages
            job['output_prefix'] = r.output_prefix
            job['fl'] = r.fl
            job['cdxj'] = r.cdxj
            job['page_size'] = r.page_size
            job['timeout'] = r.timeout
            yield job

    run_workers(r.processes, generate_fetch_job())


if __name__ == "__main__":
    main()
