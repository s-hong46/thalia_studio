import queue
from collections import defaultdict

_queues = defaultdict(queue.Queue)


def publish_event(draft_id: str, event: str, data: str):
    _queues[str(draft_id)].put((event, data))


def get_queue(draft_id: str):
    return _queues[str(draft_id)]
