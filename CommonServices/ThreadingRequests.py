# modified fetch function with semaphore
import random
import asyncio
import traceback

from aiohttp import ClientSession
import logging
import json

#Create and configure logger 
logging.basicConfig(filename="newfile.log", format='%(asctime)s %(message)s', filemode='w') 
logger=logging.getLogger()
logger.setLevel(logging.DEBUG)

async def fetch(url, session):
    async with session.get(url) as response:
        try:
            delay = response.headers.get("DELAY")
            date = response.headers.get("DATE")
            print("{}:{} with delay {}".format(date, response.url, delay))
            return json.loads(await response.text())
        except json.decoder.JSONDecodeError as jsone:
            traceback.print_exc()
            logger.exception(jsone)
            return None

async def bound_fetch(sem, url, session):
    # Getter function with semaphore.
    async with sem:
        return await fetch(url, session)

async def run(getUrls):
    tasks = []
    # create instance of Semaphore
    sem = asyncio.Semaphore(1000)

    # Create client session that will ensure we dont open new connection
    # per each request.
    async with ClientSession() as session:
        for url in getUrls:
            # pass Semaphore and session to every GET request
            task = asyncio.ensure_future(bound_fetch(sem, url, session))
            tasks.append(task)
        return await asyncio.gather(*tasks)

def IOBoundThreading(getUrls):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(getUrls))
    responses=loop.run_until_complete(future)
    return responses
    


