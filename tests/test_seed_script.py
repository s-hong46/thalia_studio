from scripts.seed_anti_examples import ANTI_EXAMPLES


def test_has_seed_items():
    assert len(ANTI_EXAMPLES) >= 10
