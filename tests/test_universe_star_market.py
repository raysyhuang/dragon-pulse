from src.core.universe import exclude_star_market_tickers, is_star_market_ticker


def test_star_market_symbol_detection_uses_six_digit_prefix():
    assert is_star_market_ticker("688006.SH")
    assert is_star_market_ticker("688009")
    assert is_star_market_ticker("689009.SH")
    assert not is_star_market_ticker("600688.SH")
    assert not is_star_market_ticker("300688.SZ")
    assert not is_star_market_ticker("600689.SH")
    assert not is_star_market_ticker("300689.SZ")


def test_exclude_star_market_tickers_preserves_order():
    assert exclude_star_market_tickers(
        ["600000.SH", "688006.SH", "000001.SZ", "688009.SH", "689009.SH"]
    ) == ["600000.SH", "000001.SZ"]
