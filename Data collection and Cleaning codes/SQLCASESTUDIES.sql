CREATE DATABASE CryptoDB;
GO
USE CryptoDB;
GO

CREATE TABLE BitcoinData (
    [Date] DATE,
    [Open] FLOAT,
    [High] FLOAT,
    [Low] FLOAT,
    [Close] FLOAT,
    [Volume] FLOAT,
    [MarketCap] FLOAT
);
GO

-- Create table for Ethereum data
CREATE TABLE EthereumData (
    [Date] DATE,
    [Open] FLOAT,
    [High] FLOAT,
    [Low] FLOAT,
    [Close] FLOAT,
    [Volume] FLOAT,
    [MarketCap] FLOAT
);
GO
-- Add Ticker column to BitcoinData
ALTER TABLE BitcoinData
ADD [Ticker] NVARCHAR(10);

-- Add Ticker column to EthereumData
ALTER TABLE EthereumData
ADD [Ticker] NVARCHAR(10);

-- View all Bitcoin data
SELECT * FROM BitcoinData;

-- View all Ethereum data
SELECT * FROM EthereumData;



