import pandas as pd
import generate_ai_strategy as g

def test_financial_fallback(monkeypatch):
    dates=pd.date_range('2025-03-31',periods=5,freq='QE')[::-1]
    class Ticker:
        info={'marketCap':1000}
        quarterly_income_stmt=pd.DataFrame([[10]*5,[20]*5,[2]*5,[10]*5],index=['Net Income','EBIT','Tax Provision','Pretax Income'],columns=dates)
        quarterly_cashflow=pd.DataFrame([[5]*5,[-7]*5],index=['Free Cash Flow','Capital Expenditure'],columns=dates)
        quarterly_balance_sheet=pd.DataFrame([[200]*5],index=['Invested Capital'],columns=dates)
    monkeypatch.setattr(g.yf,'Ticker',lambda _:Ticker())
    result=g.yf_fundamentals('TEST')
    assert result['income_quality']==.5
    assert result['fcf_yield']==.02
    assert result['roic']==.32
    assert len(result['capex_quarterly'])==5

def test_missing_historical_capex_is_not_zero_growth():
    rows=[{'date':'2026-06-30','capex':10}]*5
    result=g.compute_capex_trend(rows)
    assert result['ttm']==40
    assert result['yoy_pct'] is None
