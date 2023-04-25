# Data Source: [Insider Trading Dataset](https://www.kaggle.com/datasets/layline/insidertrading)

# Create derivatives table
```
CREATE EXTERNAL TABLE IF NOT EXISTS derivatives (
    URL STRING,
    accessionNumber STRING,
    filingDate DATE,
    filerCik STRING,
    transactionType STRING,
    tableRow INT,
    securityTitle STRING,
    securityTitleFn STRING,
    conversionOrExercisePrice DOUBLE, 
    conversionOrExercisePriceFn STRING,
    transactionDate DATE,
    transactionDateFn STRING,
    deemedExecutionDate DATE,
    deemedExecutionDateFn STRING,
    transactionFormType STRING,
    transactionCode STRING,
    equitySwapInvolved INT,
    transactionCodeFn STRING,
    transactionTimeliness STRING,
    transactionTimelinessFn STRING,
    transactionShares BIGINT,
    transactionSharesFn STRING,
    transactionTotalValue DOUBLE,
    transactionTotalValueFn STRING,
    transactionPricePerShare DOUBLE,
    transactionPricePerShareFn STRING,
    transactionAcquiredDisposedCode STRING,
    transactionAcquiredDisposedCdFn STRING,
    exerciseDate DATE,
    exerciseDateFn STRING,
    expirationDate DATE,
    expirationDateFn STRING,
    underlyingSecurityTitle STRING,
    underlyingSecurityTitleFn STRING,
    underlyingSecurityShares BIGINT,
    underlyingSecuritySharesFn STRING,
    underlyingSecurityValue DOUBLE,
    underlyingSecurityValueFn STRING,
    sharesOwnedFollowingTransaction BIGINT,
    sharesOwnedFolwngTransactionFn STRING,
    valueOwnedFollowingTransaction DOUBLE,
    valueOwnedFolwngTransactionFn STRING,
    directOrIndirectOwnership STRING,
    directOrIndirectOwnershipFn STRING,
    natureOfOwnership STRING,
    natureOfOwnershipFn STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/hthakka4/derivative/'
TBLPROPERTIES ('skip.header.line.count'='1')
;
```



# Create cleaned derivatives table

```
CREATE TABLE cleaned_derivatives AS
SELECT 
    filingDate,
    transactionType,
    UPPER(securityTitle) AS securityTitle,
    conversionOrExercisePrice,
    transactionDate,
    transactionFormType,
    transactionCode,
    equitySwapInvolved,
    transactionShares,
    transactionTotalValue,
    transactionPricePerShare,
    exerciseDate,
    expirationDate,
    underlyingSecurityTitle,
    underlyingSecurityShares,
    underlyingSecurityValue,
    sharesOwnedFollowingTransaction,
    valueOwnedFollowingTransaction,
    directOrIndirectOwnership,
    natureOfOwnership
FROM 
    derivatives
WHERE
    transactionDate NOT LIKE "NULL"
;
```

# Derivative Trading Queries


## Security whose derivative was most frequently traded 

```
INSERT OVERWRITE DIRECTORY '/user/hthakka4/tmp/'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT 
    securityTitle AS securityTitle, 
    COUNT(securityTitle) AS number_of_transactions 
FROM 
    cleaned_derivatives 
GROUP BY 
    securityTitle 
ORDER BY 
    number_of_transactions DESC 
LIMIT 5
;
```

For output:
```
hdfs dfs -get tmp/000000_0 query1.csv
```

## Security whose derivative had the highest average transaction value (Minimum 100 transactions)

```
INSERT OVERWRITE DIRECTORY '/user/hthakka4/tmp/'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT 
    securityTitle AS securityTitle, 
    AVG(transactionTotalValue) AS average_transaction_value,
    COUNT(securityTitle) AS number_of_transactions
FROM 
    cleaned_derivatives 
GROUP BY 
    securityTitle 
HAVING 
    number_of_transactions > 100
ORDER BY 
    average_transaction_value DESC 

LIMIT 5
;
```

For output:
```
hdfs dfs -get tmp/000000_0 query2.csv
```

# Create Non - Derivatives table
```
CREATE EXTERNAL TABLE IF NOT EXISTS non_derivatives (
    URL STRING,
    accessionNumber STRING,
    filingDate DATE,
    filerCik STRING,
    transactionType STRING,
    tableRow INT,
    securityTitle STRING,
    securityTitleFn STRING,
    transactionDate DATE,
    transactionDateFn STRING,
    deemedExecutionDate DATE,
    deemedExecutionDateFn STRING,
    transactionFormType STRING, 
    transactionCode STRING,
    equitySwapInvolved INT,
    transactionCodeFn STRING,
    transactionTimeliness STRING,
    transactionTimelinessFn STRING,
    transactionShares DOUBLE,
    transactionSharesFn STRING,
    transactionPricePerShare DOUBLE,
    transactionPricePerShareFn STRING,
    transactionAcquiredDisposedCode STRING,
    transactionAcquiredDisposedCdFn STRING,
    sharesOwnedFollowingTransaction DOUBLE,
    sharesOwnedFolwngTransactionFn STRING,
    valueOwnedFollowingTransaction DOUBLE,
    valueOwnedFolwngTransactionFn STRING,
    directOrIndirectOwnership STRING,
    directOrIndirectOwnershipFn STRING,
    natureOfOwnership STRING,
    natureOfOwnershipFn STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/hthakka4/non-derivative/'
TBLPROPERTIES ('skip.header.line.count'='1')
;
```

# Create Cleaned Non - Derivatives table
```
CREATE TABLE cleaned_non_derivatives AS
SELECT 
    filingDate,
    transactionType,
    UPPER(securityTitle) AS securityTitle,
    transactionDate,
    transactionShares,
    transactionPricePerShare,
    sharesOwnedFollowingTransaction,
    valueOwnedFollowingTransaction,
    directOrIndirectOwnership,
    natureOfOwnership
FROM 
    non_derivatives
WHERE
    transactionDate NOT LIKE "NULL"
;
```

# Non - Derivative Trading Queries

## Most frequently non-derivatives were traded on
```
INSERT OVERWRITE DIRECTORY '/user/hthakka4/tmp/'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT 
    securityTitle, 
    MAX(transactionPricePerShare) as max_val
FROM 
    cleaned_non_derivatives 
WHERE 
    securityTitle NOT LIKE "NULL"

GROUP BY 
    securityTitle

ORDER BY 
    max_val DESC 
LIMIT 5
;
```

For output:
```
hdfs dfs -get tmp/000000_0 query3.csv
```

# Derivative + Non - Derivative Line Chart

```
INSERT OVERWRITE DIRECTORY '/user/hthakka4/tmp/'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT
    transactionDate,
    transactionType,
    COUNT(*)
FROM
    cleaned_derivatives
GROUP BY
    transactionDate,
    transactionType
;
```

For output:
```
hdfs dfs -get tmp/000000_0 query4.csv
```


```
INSERT OVERWRITE DIRECTORY '/user/hthakka4/tmp/'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT
    transactionDate,
    transactionType,
    COUNT(*)
FROM
    cleaned_non_derivatives
GROUP BY
    transactionDate,
    transactionType
;
```

For output:
```
hdfs dfs -get tmp/000000_0 query5.csv
```

# Reporting Owners' data
```
CREATE EXTERNAL TABLE IF NOT EXISTS reporting_owners (

    URL STRING,
    accessionNumber STRING,
    filingDate DATE,
    filerCik STRING,
    rptOwnerCik STRING,
    rptOwnerName STRING,
    rptOwnerStreet1 STRING,
    rptOwnerStreet2 STRING,
    rptOwnerCity STRING,
    rptOwnerState STRING,
    rptOwnerZipCode STRING,
    rptOwnerStateDescription STRING,
    isDirector INT,
    isOfficer INT,
    isTenPercentOwner INT,
    isOther INT,
    officerTitle STRING,
    otherText STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION '/user/hthakka4/reporting-owners/'
TBLPROPERTIES ('skip.header.line.count'='1')
;
```

# Reporting Owner's Cleaned Data
```
CREATE TABLE cleaned_reporting_owners AS
SELECT 
    rptOwnerName,
    rptOwnerStreet1,
    rptOwnerStreet2,
    rptOwnerCity,
    rptOwnerState,
    rptOwnerZipCode,
    isDirector,
    isOfficer,
    isTenPercentOwner,
    isOther
FROM 
    reporting_owners
WHERE
    rptOwnerState NOT LIKE "NULL"
    AND
    rptOwnerCity NOT LIKE "NULL"
;
```

# Number of trades reported by area
```
INSERT OVERWRITE DIRECTORY '/user/hthakka4/tmp/'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT 
    concat_ws(',', rptOwnerState, rptOwnerCity, rptOwnerStreet1, rptOwnerStreet2, rptOwnerZipCode),
    COUNT(*)
FROM 
    cleaned_reporting_owners
GROUP BY
    concat_ws(',', rptOwnerState, rptOwnerCity, rptOwnerStreet1, rptOwnerStreet2, rptOwnerZipCode)
ORDER BY
    COUNT(*) DESC
LIMIT 200
;
```

For output:
```
hdfs dfs -get tmp/000000_0 query6.csv
```