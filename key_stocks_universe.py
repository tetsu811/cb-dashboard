"""
重點個股掃描宇宙（Universe）
大約 800 檔：S&P 500 + 重點 NASDAQ 100 + 半導體／AI／成長股精選

用於每日掃描「放量 + 型態」以主動找出值得關注的個股（例如 MXL、POWI 等
不在 SPDR sector top-10 持股內的中小型股）。

清單更新方式：
  - 每季檢查 SP500 成分股變動
  - 半導體／AI 類別可隨熱點更新
  - 不需要 100% 準確，涵蓋 >= 80% 常見交易標的即可
"""

# ── S&P 500 核心（截至 2025 年資料，略有偏移可接受） ────────────────────────
SP500 = [
    "A","AAL","AAPL","ABBV","ABNB","ABT","ACGL","ACN","ADBE","ADI","ADM","ADP","ADSK","AEE",
    "AEP","AES","AFL","AIG","AIZ","AJG","AKAM","ALB","ALGN","ALL","ALLE","AMAT","AMCR","AMD",
    "AME","AMGN","AMP","AMT","AMZN","ANET","ANSS","AON","AOS","APA","APD","APH","APO","APTV",
    "ARE","ATO","AVB","AVGO","AVY","AWK","AXON","AXP","AZO","BA","BAC","BALL","BAX","BBY",
    "BDX","BEN","BF-B","BG","BIIB","BK","BKNG","BKR","BLDR","BLK","BMY","BR","BRK-B","BRO",
    "BSX","BWA","BX","BXP","C","CAG","CAH","CARR","CAT","CB","CBOE","CBRE","CCI","CCL",
    "CDNS","CDW","CE","CEG","CF","CFG","CHD","CHRW","CHTR","CI","CINF","CL","CLX","CMCSA",
    "CME","CMG","CMI","CMS","CNC","CNP","COF","COO","COP","COR","COST","CPAY","CPB","CPRT",
    "CPT","CRL","CRM","CRWD","CSCO","CSGP","CSX","CTAS","CTRA","CTSH","CTVA","CVS","CVX",
    "CZR","D","DAL","DAY","DD","DE","DECK","DELL","DFS","DG","DGX","DHI","DHR","DIS","DLR",
    "DLTR","DOC","DOV","DOW","DPZ","DRI","DTE","DUK","DVA","DVN","DXCM","EA","EBAY","ECL",
    "ED","EFX","EG","EIX","EL","ELV","EMN","EMR","ENPH","EOG","EPAM","EQIX","EQR","EQT",
    "ERIE","ES","ESS","ETN","ETR","EVRG","EW","EXC","EXPD","EXPE","EXR","F","FANG","FAST",
    "FCX","FDS","FDX","FE","FFIV","FI","FICO","FIS","FITB","FMC","FOX","FOXA","FRT","FSLR",
    "FTNT","FTV","GD","GDDY","GE","GEHC","GEN","GEV","GILD","GIS","GL","GLW","GM","GNRC",
    "GOOG","GOOGL","GPC","GPN","GRMN","GS","GWW","HAL","HAS","HBAN","HCA","HD","HES","HIG",
    "HII","HLT","HOLX","HON","HPE","HPQ","HRL","HSIC","HST","HSY","HUBB","HUM","HWM","IBM",
    "ICE","IDXX","IEX","IFF","INCY","INTC","INTU","INVH","IP","IPG","IQV","IR","IRM","ISRG",
    "IT","ITW","IVZ","J","JBHT","JBL","JCI","JKHY","JNJ","JNPR","JPM","K","KDP","KEY","KEYS",
    "KHC","KIM","KKR","KLAC","KMB","KMI","KMX","KO","KR","KVUE","L","LDOS","LEN","LH","LHX",
    "LIN","LKQ","LLY","LMT","LNT","LOW","LRCX","LULU","LUV","LVS","LYB","LYV","MA","MAA",
    "MAR","MAS","MCD","MCHP","MCK","MCO","MDLZ","MDT","MET","META","MGM","MHK","MKC","MKTX",
    "MLM","MMC","MMM","MNST","MO","MOH","MOS","MPC","MPWR","MRK","MRNA","MRO","MS","MSCI",
    "MSFT","MSI","MTB","MTCH","MTD","MU","NCLH","NDAQ","NDSN","NEE","NEM","NFLX","NI","NKE",
    "NOC","NOW","NRG","NSC","NTAP","NTRS","NUE","NVDA","NVR","NWS","NWSA","NXPI","O","ODFL",
    "OKE","OMC","ON","ORCL","ORLY","OTIS","OXY","PANW","PARA","PAYC","PAYX","PCAR","PCG",
    "PEG","PEP","PFE","PFG","PG","PGR","PH","PHM","PKG","PLD","PLTR","PM","PNC","PNR","PNW",
    "PODD","POOL","PPG","PPL","PRU","PSA","PSX","PTC","PWR","PYPL","QCOM","QRVO","RCL","REG",
    "REGN","RF","RJF","RL","RMD","ROK","ROL","ROP","ROST","RSG","RTX","RVTY","SBAC","SBUX",
    "SCHW","SHW","SJM","SLB","SMCI","SNA","SNPS","SO","SOLV","SPG","SPGI","SRE","STE","STLD",
    "STT","STX","STZ","SW","SWK","SWKS","SYF","SYK","SYY","T","TAP","TDG","TDY","TEL","TER",
    "TFC","TFX","TGT","TJX","TMO","TMUS","TPR","TRGP","TRMB","TROW","TRV","TSCO","TSLA","TSN",
    "TT","TTWO","TXN","TXT","TYL","UAL","UBER","UDR","UHS","ULTA","UNH","UNP","UPS","URI","USB",
    "V","VICI","VLO","VLTO","VMC","VRSK","VRSN","VRTX","VST","VTR","VTRS","VZ","WAB","WAT",
    "WBA","WBD","WDC","WEC","WELL","WFC","WM","WMB","WMT","WRB","WSM","WST","WTW","WY",
    "WYNN","XEL","XOM","XYL","YUM","ZBH","ZBRA","ZTS",
]

# ── 半導體（Russell 2000 / 小中型股，不在 SP500 裡） ──────────────────────────
SEMI_EXTRA = [
    "MXL",       # MaxLinear — 使用者點名
    "RMBS",      # Rambus
    "POWI",      # Power Integrations
    "CEVA",      # Ceva
    "ALGM",      # Allegro MicroSystems
    "FORM",      # FormFactor
    "ACLS",      # Axcelis Technologies
    "AMKR",      # Amkor
    "CAMT",      # Camtek
    "COHU",      # Cohu
    "IDCC",      # InterDigital
    "IMMR",      # Immersion
    "KLIC",      # Kulicke & Soffa
    "LSCC",      # Lattice Semi
    "MTSI",      # MACOM
    "NAVI",
    "ONTO",      # Onto Innovation
    "PI",        # Impinj
    "SANM",      # Sanmina
    "SIMO",      # Silicon Motion
    "SITM",      # SiTime
    "SLAB",      # Silicon Labs
    "SMTC",      # Semtech
    "SYNA",      # Synaptics
    "UCTT",      # Ultra Clean Holdings
    "VECO",      # Veeco
    "WOLF",      # Wolfspeed
    "AEHR",      # Aehr Test
    "AMBA",      # Ambarella
    "CRUS",      # Cirrus Logic
    "DIOD",      # Diodes
    "HIMX",      # Himax
    "NVMI",      # Nova
    "PLAB",      # Photon Dynamics / Photronics
    "VSH",       # Vishay
    "IIVI","COHR",    # Coherent (IIVI 舊代碼)
    "ARM",       # Arm Holdings
    "NVTS",      # Navitas
    "INDI",      # indie Semiconductor
    "MBLY",      # Mobileye
    "TSEM",      # Tower Semi
    "ASX",       # ASE
    "UMC","TSM","ASML","LSCC","MRVL",
]

# ── AI / 雲端 / SaaS ──────────────────────────────────────────────────────────
AI_SOFTWARE = [
    "AI",        # C3.ai
    "BBAI",      # BigBear.ai
    "SOUN",      # SoundHound
    "IOT",       # Samsara
    "PATH",      # UiPath
    "S",         # SentinelOne
    "NET",       # Cloudflare
    "DDOG",      # Datadog
    "MDB",       # MongoDB
    "ZS",        # Zscaler
    "OKTA",      # Okta
    "TEAM",      # Atlassian
    "ZM",        # Zoom
    "DOCU",      # DocuSign
    "SNOW",      # Snowflake
    "CFLT",      # Confluent
    "BILL",      # Bill.com
    "APPN",      # Appian
    "ESTC",      # Elastic
    "TWLO",      # Twilio
    "RBLX",      # Roblox
    "U",         # Unity
    "PLNT",      # Planet Fitness (non-AI, removed)
    "PL",        # Planet Labs
    "RDDT",      # Reddit
    "SMR",       # NuScale Power
    "OKLO",      # Oklo
    "SERV",      # Serve Robotics
    "DUOL",      # Duolingo
    "TSLA","META","GOOGL","MSFT","NVDA","AMD","AAPL","AMZN","AVGO","ORCL","CRM","NOW",
    "VRT",       # Vertiv
    "ETN","IOT",
    "CRDO",      # Credo Tech
    "AEVA","LAZR","MVIS","INVZ","HSAI",  # LiDAR/感測
    "RKLB",      # Rocket Lab
    "ASTS",      # AST SpaceMobile
    "IRDM",      # Iridium
    "GSAT",      # Globalstar
    "TTD",       # Trade Desk
    "APP",       # AppLovin
    "META","PYPL","SHOP","MELI","SE","NTLA","EDIT","BEAM","CRSP",
    "ACHR",      # Archer
    "JOBY",      # Joby
    "EH",        # EHang
    "PLUG",      # Plug Power
    "BE",        # Bloom Energy
    "ENPH",      # Enphase
    "SEDG",      # SolarEdge
    "RUN",       # Sunrun
    "FSLR",      # First Solar
    "TAN",
    "CLSK","MARA","RIOT","HUT","BTBT","BITF","IREN","CIFR","WULF",  # 礦工
    "COIN","HOOD","MSTR","ETHA",
    "ASTL","ASTS","ACHR","JOBY","LILM","EVTL","ARCH",
    "ASAN","FVRR","UPST","SOFI","AFRM","LC",
    "DASH","DOCU","NTNX","HUBS","TYL","PATH","PYCR","NEWR","DT","BAND","RNG","FROG","DOCN","GTLB","AKAM","TENB","RPD","VRNS","S","CRWD","PANW","FTNT","CYBR","OKTA","ZS","NET","DDOG","MDB","SNOW","CFLT","PLTR","AI","BBAI","SOUN","IOT",
    "NVDA","META","GOOG","MSFT","AMZN","AVGO","AAPL","TSM","ASML","ORCL","ADBE","CRM","NOW","ARM","QCOM","INTC","AMD","MU","LRCX","KLAC","AMAT","MRVL","PLTR","SNOW","CRWD","PANW","FTNT","NFLX","DDOG","UBER","SHOP","SQ","PYPL",
]

# ── 生技 / 醫療（熱門波動大的） ────────────────────────────────────────────
BIOTECH_EXTRA = [
    "NVAX","OCGN","BIIB","VRTX","REGN","GILD","MRNA","AMGN",
    "SRPT","BMRN","ALNY","INCY","PTCT","EXEL","NKTR",
    "EDIT","BEAM","CRSP","NTLA","DNA","PACB","ILMN","TWST","CDNA",
    "SMMT","IONS","VKTX","RXRX","TEVA","BHC","JAZZ","HRMY",
    "CRDF","CERO","ATXI","BPMC","ALVR","MGNX","TGTX","RCKT","ARCT","CRSP",
    "CORT","MYGN","VEEV","CRL","NTRA","DNA","RVMD",
]

# ── 消費 / 零售（中小型熱門） ─────────────────────────────────────────────
CONSUMER_EXTRA = [
    "LULU","DECK","CROX","ONON","BIRK","HOKA","ANF","URBN","AEO","FIVE","DOLE","FRSH",
    "RH","WSM","W","ETSY","CHWY","CVNA","RIVN","LCID","NIO","XPEV","LI","BYND","OATLY","PLNT",
    "PTON","HBI","LEVI","NKE","UA","UAA","KATE","CPRI","DKS","FL",
    "TOST","SHAK","CMG","MCD","DPZ","WING","CAVA","SG","YUMC","PNRA",
    "BROS","KTB","DVA","BJ","COST","WMT","TGT","DG","DLTR",
    "CELH","STZ","KO","PEP","MNST","KDP",
]

# ── 金融 / 保險 / REIT 熱門（非 SP500） ──────────────────────────────────
FIN_EXTRA = [
    "SOFI","HOOD","UPST","AFRM","LC","COIN","MSTR","BKKT","PYPL",
    "MARA","RIOT","CLSK","HUT","BITF","BTBT","IREN","WULF","CIFR",
    "BMY","LLY","NVO","JAZZ","DVA","BRK-A","BRK-B","TRV","CB",
    "BX","KKR","APO","CG","TPG","OWL","ARES","HLNE",
]

# ── 能源 / 原物料（中小型） ──────────────────────────────────────────────
ENERGY_EXTRA = [
    "SMR","OKLO","CEG","VST","NRG","LEU","UEC","UUUU","DNN","CCJ","URA","NXE","URG",
    "TELL","LNG","CQP","NEP","ARCH","BTU","AMR","HCC","METC","NEX","FTI","NFE",
    "FCEL","BE","PLUG","BLDP","HYZN",
    "ASTL","SUN","OXY","APA","FANG","MRO","DVN","EOG","PXD","CTRA","HES",
]

# ── 工業 / 國防 / 航太（熱門） ────────────────────────────────────────────
INDUSTRIAL_EXTRA = [
    "RKLB","ASTS","IRDM","LUNR","ACHR","JOBY","LILM","EH","EVEX","SARO","BKSY",
    "AVAV","KTOS","LMT","NOC","RTX","GD","LDOS","HII","LHX","BA","HWM","TDG","TXT",
    "SPCE","MAXR","SPIR","PL","SATS","GSAT","ORBC","ASTL",
]

# ── 匯總去重 ──────────────────────────────────────────────────────────────
def get_universe():
    seen = set()
    out = []
    for group in (SP500, SEMI_EXTRA, AI_SOFTWARE, BIOTECH_EXTRA,
                  CONSUMER_EXTRA, FIN_EXTRA, ENERGY_EXTRA, INDUSTRIAL_EXTRA):
        for sym in group:
            if sym not in seen:
                seen.add(sym)
                out.append(sym)
    return out


if __name__ == "__main__":
    u = get_universe()
    print(f"universe size: {len(u)}")
    print(u[:20])
