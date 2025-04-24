from handler import main


def test_handle():
    dummy_event = {}
    dummy_context = object()
    assert main(dummy_event, dummy_context) is None
