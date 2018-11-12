from nesta.production.batchables.health_data.nih_abstract_mesh_data.run import clean_abstract


def test_remove_newlines():
    text = "this is my test text\n it has\n so many \n newlines\n"
    clean_text = clean_abstract(text)
    assert '\n' not in clean_text


def test_remove_tabs():
    text = "this is \t some more \t test text this \t time with \t tabs\t"
    clean_text = clean_abstract(text)
    assert '\t' not in clean_text


def test_remove_multiple_spaces():
    text = "   there are    so          many multiple spaces in this t   e   x  t"
    clean_text = clean_abstract(text)
    assert '  ' not in clean_text
