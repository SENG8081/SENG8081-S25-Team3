
CREATE TABLE raw_eth_prices_bnz (
    PriceDate    DATE            NOT NULL,               -- corresponds to Date
    Close_ETH    DECIMAL(18,8)   NULL,
    High_ETH     DECIMAL(18,8)   NULL,
    Low_ETH      DECIMAL(18,8)   NULL,
    Open_ETH     DECIMAL(18,8)   NULL,
    Volume_ETH   BIGINT          NULL,
    Coin         VARCHAR(20)     NOT NULL,               -- 'Ethereum'
    RetrievedAt  DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_raw_eth_prices PRIMARY KEY (PriceDate, Coin)
);

\
CREATE TABLE raw_btc_prices_bnz (
    PriceDate    DATE            NOT NULL,
    Close_BTC    DECIMAL(18,8)   NULL,
    High_BTC     DECIMAL(18,8)   NULL,
    Low_BTC      DECIMAL(18,8)   NULL,
    Open_BTC     DECIMAL(18,8)   NULL,
    Volume_BTC   BIGINT          NULL,
    Coin         VARCHAR(20)     NOT NULL,  -- 'Bitcoin'
    RetrievedAt  DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_raw_btc_prices PRIMARY KEY (PriceDate, Coin)
);


CREATE TABLE raw_eth_prices_sil (
        PriceDate             DATE            NOT NULL,
        Open_ETH              DECIMAL(18,8)   NULL,
        High_ETH              DECIMAL(18,8)   NULL,
        Low_ETH               DECIMAL(18,8)   NULL,
        Close_ETH             DECIMAL(18,8)   NULL,
        Volume_ETH            BIGINT          NULL,
        DailyReturn           FLOAT           NULL,
        Volume_ETH_Millions   FLOAT           NULL,
        Coin                  VARCHAR(20)     NOT NULL,  -- 'Ethereum'
        RetrievedAt           DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_raw_eth_prices_sil PRIMARY KEY (PriceDate, Coin)
    );


CREATE TABLE raw_btc_prices_sil (
        PriceDate             DATE            NOT NULL,
        Open_BTC              DECIMAL(18,8)   NULL,
        High_BTC              DECIMAL(18,8)   NULL,
        Low_BTC               DECIMAL(18,8)   NULL,
        Close_BTC             DECIMAL(18,8)   NULL,
        Volume_BTC            BIGINT          NULL,
        DailyReturn           FLOAT           NULL,
        Volume_BTC_Millions   FLOAT           NULL,
        Coin                  VARCHAR(20)     NOT NULL,  -- 'Bitcoin'
        RetrievedAt           DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_raw_btc_prices_sil PRIMARY KEY (PriceDate, Coin)
    );



CREATE TABLE gold_btc_prices (
  PriceDate            DATE            NOT NULL,
  Open_BTC             FLOAT           NULL,
  High_BTC             FLOAT           NULL,
  Low_BTC              FLOAT           NULL,
  Close_BTC            FLOAT           NULL,
  Volume_BTC           BIGINT          NULL,
  DailyReturn          FLOAT           NULL,
  Volume_BTC_Millions  FLOAT           NULL,
  Coin                 VARCHAR(16)     NOT NULL,
  SMA7_BTC             FLOAT           NULL,
  SMA30_BTC            FLOAT           NULL,
  SMA90_BTC            FLOAT           NULL,
  Vol7_BTC             FLOAT           NULL,
  Vol30_BTC            FLOAT           NULL,
  Vol90_BTC            FLOAT           NULL,
  CONSTRAINT PK_gold_btc_prices PRIMARY KEY (PriceDate, Coin)
);

CREATE TABLE gold_eth_prices (
  PriceDate            DATE            NOT NULL,
  Open_ETH             FLOAT           NULL,
  High_ETH             FLOAT           NULL,
  Low_ETH              FLOAT           NULL,
  Close_ETH            FLOAT           NULL,
  Volume_ETH           BIGINT          NULL,
  DailyReturn          FLOAT           NULL,
  Volume_ETH_Millions  FLOAT           NULL,
  Coin                 VARCHAR(16)     NOT NULL,
  SMA7_ETH             FLOAT           NULL,
  SMA30_ETH            FLOAT           NULL,
  SMA90_ETH            FLOAT           NULL,
  Vol7_ETH             FLOAT           NULL,
  Vol30_ETH            FLOAT           NULL,
  Vol90_ETH            FLOAT           NULL,
  CONSTRAINT PK_gold_eth_prices PRIMARY KEY (PriceDate, Coin)
);


ALTER TABLE dbo.gold_btc_prices
ADD
    LogReturn_BTC        FLOAT        NULL,
    CumulativeReturn_BTC FLOAT        NULL,
    Drawdown_BTC         FLOAT        NULL,
    VolAvg30_BTC         FLOAT        NULL,
    [Year]               INT          NULL,
    [Month]              TINYINT      NULL,
    DayOfWeek            TINYINT      NULL;
GO

-- Add missing feature columns to gold_eth_prices
ALTER TABLE dbo.gold_eth_prices
ADD
    LogReturn_ETH        FLOAT        NULL,
    CumulativeReturn_ETH FLOAT        NULL,
    Drawdown_ETH         FLOAT        NULL,
    VolAvg30_ETH         FLOAT        NULL,
    [Year]               INT          NULL,
    [Month]              TINYINT      NULL,
    DayOfWeek            TINYINT      NULL;
GO


CREATE TABLE dbo.platinum_crypto_horizon (
  Coin         VARCHAR(16) NOT NULL,
  HorizonLabel VARCHAR(16) NOT NULL,   -- e.g. '7 Days', '1 Year', '7 Years'
  TargetDate   DATE        NOT NULL,
  Forecast     FLOAT       NULL,
  ReturnPct    FLOAT       NULL,       -- implied % return vs. last actual
  CONSTRAINT PK_platinum_crypto_horizon PRIMARY KEY (Coin, HorizonLabel)
);

