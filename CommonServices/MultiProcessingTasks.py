import concurrent.futures
import time


def CPUBonundThreading(methodToBeCalled, dataToBeThreadedOn):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(methodToBeCalled, dataToBeThreadedOn)
    # Returns an object of results. This result can be anything from strings, to list of objects to a dict
    # Depends on methodToBeCalled. What it's returning
    return results


def multi_processing_method(method_for, iterable_for, max_workers=2):
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        future = executor.map(method_for, iterable_for)
    return future
