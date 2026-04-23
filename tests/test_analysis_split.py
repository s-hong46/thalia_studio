from app.services.analysis_service import split_paragraphs


def test_split_paragraphs():
    text = (
        "A line that is quite long to pass threshold.\n\n"
        "Another long paragraph to test with enough characters to pass."
    )
    assert split_paragraphs(text) == [
        "A line that is quite long to pass threshold.",
        "Another long paragraph to test with enough characters to pass.",
    ]
