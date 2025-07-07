from shared.utils.url_util import canonical_host


def test_canonical_host():  # happy path
    assert (
        canonical_host("https://dominos.posist.com/order.aspx?id=123&utm=xyz")
        == "dominos.posist.com"
    )
    assert (
        canonical_host("http://dominos.posist.com:80/order.aspx#top")
        == "dominos.posist.com"
    )
    assert (
        canonical_host("https://DOMINOS.POSIST.COM/anything/else/")
        == "dominos.posist.com"
    )
    assert canonical_host("https://www.mfgabc.COM/anything/else/") == "www.mfgabc.com"
    assert canonical_host("https://www1.mfgabc.COM/anything/else/") == "www1.mfgabc.com"
    assert canonical_host("https://www2.mfgabc.COM/anything/else/") == "www2.mfgabc.com"
    assert (
        canonical_host("https://www.m.mfgabc.COM/anything/else/") == "www.m.mfgabc.com"
    )
    assert canonical_host("https://mfgabc.COM/anything/else/") == "mfgabc.com"
    assert canonical_host("www.mfgabc.COM") == "www.mfgabc.com"


def test_canonical_host_invalid():
    assert canonical_host("https://") is None
    assert canonical_host("http://") is None
    assert canonical_host("https://:80/") is None
    assert canonical_host("http://:80/") is None
    assert canonical_host("invalid-url") is None
    assert canonical_host("") is None
    assert canonical_host(" / ") is None
    assert canonical_host(None) is None
