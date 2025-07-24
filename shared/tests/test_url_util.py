from shared.utils.url_util import normalize_host


def test_normalize_host():  # happy path
    assert (
        normalize_host("https://dominos.posist.com/order.aspx?id=123&utm=xyz")
        == "dominos.posist.com"
    )
    assert (
        normalize_host("http://dominos.posist.com:80/order.aspx#top")
        == "dominos.posist.com"
    )
    assert (
        normalize_host("https://DOMINOS.POSIST.COM/anything/else/")
        == "dominos.posist.com"
    )
    assert normalize_host("https://www.mfgabc.COM/anything/else/") == "mfgabc.com"
    assert (
        normalize_host(normalize_host("https://www.mfgabc.COM/anything/else/"))
        == "mfgabc.com"
    )

    assert normalize_host("https://www1.mfgabc.COM/anything/else/") == "www1.mfgabc.com"
    assert (
        normalize_host(normalize_host("https://www1.mfgabc.COM/anything/else/"))
        == "www1.mfgabc.com"
    )

    assert normalize_host("https://www2.mfgabc.COM/anything/else/") == "www2.mfgabc.com"
    assert (
        normalize_host(normalize_host("https://www2.mfgabc.COM/anything/else/"))
        == "www2.mfgabc.com"
    )

    assert normalize_host("https://www.m.mfgabc.COM/anything/else/") == "m.mfgabc.com"
    assert (
        normalize_host(normalize_host("https://www.m.mfgabc.COM/anything/else/"))
        == "m.mfgabc.com"
    )

    assert normalize_host("https://mfgabc.COM/anything/else/") == "mfgabc.com"
    assert (
        normalize_host(normalize_host("https://mfgabc.COM/anything/else/"))
        == "mfgabc.com"
    )

    assert normalize_host("www.mfgabc.COM") == "mfgabc.com"
    assert normalize_host(normalize_host("www.mfgabc.COM")) == "mfgabc.com"

    assert normalize_host("mfgabc.COM") == "mfgabc.com"
    assert normalize_host(normalize_host("mfgabc.COM")) == "mfgabc.com"


def test_normalize_host_invalid():
    assert normalize_host("https://") is None
    assert normalize_host("http://") is None
    assert normalize_host("https://:80/") is None
    assert normalize_host("http://:80/") is None
    assert normalize_host("invalid-url") is None
    assert normalize_host("") is None
    assert normalize_host(" / ") is None
    assert normalize_host(None) is None
