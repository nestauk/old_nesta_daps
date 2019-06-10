from nesta.packages.crunchbase.utils import parse_investor_names

def test_parse_investor_names():
    examples = ['{First,Second,"Third",Fourth,"Fifth","Sixth","Seventh"}',
                '{"First",Second,"Third",Fourth,"Fifth","Sixth","Seventh"}',
                '{"First",Second,Third,Fourth,Fifth,Sixth,Seventh}',
                '{First,Second,Third,Fourth,Fifth,Sixth,Seventh}',
                '{"First","Second","Third","Fourth","Fifth","Sixth","Seventh"}']
    expected = sorted(["First","Second","Third","Fourth",
                       "Fifth","Sixth","Seventh"])
    for eg in examples:
        result = parse_investor_names(eg)
        assert sorted(result) == expected
