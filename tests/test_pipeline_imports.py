def test_vestiaire_wrapper_is_importable():
    from src.scrapers.vestiaire import scrape_vestiaire_dior

    assert callable(scrape_vestiaire_dior)


def test_run_pipeline_imports_cleanly():
    import run_pipeline

    assert hasattr(run_pipeline, "run_full_analytical_pipeline")
