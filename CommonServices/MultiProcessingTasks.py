import concurrent.futures
import time

def CPUBonundThreading(methodToBeCalled,dataToBeThreadedOn):
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(methodToBeCalled, dataToBeThreadedOn)
    # Returns an object of results. This result can be anything from strings, to list of objects to a dict
    # Depends on methodToBeCalled. What it's returning
    return results
    