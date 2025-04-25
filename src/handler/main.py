from handler.handler import handle_event


def main(event, context) -> None:
    return handle_event(event, context)
