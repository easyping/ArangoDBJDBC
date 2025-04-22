### SQL - AQL Functions

## String

| SQL        | Return-Type | AQL              | Aggregate | Comment |
|------------|-------------|------------------|-----------|---------|
| ASCII      | String      |                  |           |         |
| CHAR       | String      | TO_CHAR          |           |         |
| CHARINDEX  | String      |                  |           |         |
| CONCAT     | String      | CONCAT           |           |         |
| CONCAT_WS  | String      | CONCAT_SEPARATOR |           |         |
| DATALENGTH | Integer     |                  |           |         |
| DIFFERENCE | Integer     |                  |           |         |
| FORMAT     | String      |                  |           |         |
| LEFT       | String      | LEFT             |           |         |
| LEN        | Integer     | LENGTH           | x         |         |
| LOWER      | String      | LOWER            |           |         |
| LTRIM      | String      | LTRIM            |           |         |
| NCHAR      | String      |                  |           |         |
| PATINDEX   | Integer     |                  |           |         |
| QUOTENAME  | String      |                  |           |         |
| REPLACE    | String      | REGEX_REPLACE    |           |         |
| REPLICATE  | String      | REPEAT           |           |         |
| REVERSE    | String      | RESERVE          |           |         |
| RIGHT      | String      | RIGHT            |           |         |
| RTRIM      | String      | RTRIM            |           |         |
| SOUNDEX    | String      | SOUNDEX          |           |         |
| SPACE      | String      |                  |           |         |
| STR        | String      |                  |           |         |
| STUFF      | String      |                  |           |         |
| SUBSTRING  | String      | SUBSTRING        |           |         |
| TRANSLATE  | String      |                  |           |         |
| TRIM       | String      | TRIM             |           |         |
| UNICODE    | String      |                  |           |         |
| UPPER      | String      | UPPER            |           |         |

## Numeric

| SQL     | Return-Type | AQL     | Aggregate | Comment   |
|---------|-------------|---------|-----------|-----------|
| ABS     | Number      | ABS     |           |           |
| ACOS    | Number      | ACOS    |           |           |
| ASIN    | Number      | ASIN    |           |           |
| ATAN    | Number      | ATAN    |           |           |
| ATN2    | Number      | ATAN2   |           |           |
| AVG     | Number      | AVG     | x         |           |
| CEILING | Number      | CEIL    |           |           |
| COUNT   | Number      | COUNT   | x         |           |
| COS     | Number      | COS     |           |           |
| COT     | Number      |         |           |           |
| DEGREES | Number      | DEGREES |           |           |
| EXP     | Number      | EXP     |           |           |
| FLOOR   | Number      | FLOOR   |           |           |
| LOG     | Number      | LOG     |           |           |
| LOG10   | Number      | LOG10   |           |           |
| MAX     | Number      | MAX     | x         | also Date |
| MIN     | Number      | MIN     | x         | also Date |
| PI      | Number      | PI      |           |           |
| POWER   | Number      | POW     |           |           |
| RADIANS | Number      | RADIANS |           |           |
| RAND    | Number      | RAND    |           |           |
| SIGN    | Number      |         |           |           |
| SIN     | Number      | SIN     |           |           |
| SQRT    | Number      | SQRT    |           |           |
| SQUARE  | Number      |         |           |           |
| SUM     | Number      | SUM     | x         |           |
| TAN     | Number      | TAN     |           |           |

## Date

| SQL               | Return-Type | AQL            | Aggregate | Comment                     |
|-------------------|-------------|----------------|-----------|-----------------------------|
| CURRENT_TIMESTAMP | Timestamp   | DATE_NOW()     |           |                             |
| DATEADD           | Timestamp   | DATE_ADD       |           | Parameter: 1->3, 2->2, 3->1 |
| DATEDIFF          | Number      | DATE_DIFF      |           | Parameter: 1->3, 2->1, 3->2 |
| DATEFROMPARTS     | Timestamp   | DATE_TIMESTAMP |           |                             |
| DATENAME          | String      |                |           |                             |
| DATEPART          | Number      |                |           |                             |
| DAY               | Number      | DATE_DAY       |           |                             |
| GETDATE           | Timestamp   | DATE_NOW       |           |                             |
| GETUTCDATE        | Timestamp   | DATE_ISO8601   |           |                             |
| ISDATE            | Boolean     | IS_DATESTRING  |           |                             |
| MONTH             | Number      | DATE_MONTH     |           |                             |
| SYSDATETIME       | Timestamp   | DATE_NOW       |           |                             |
| YEAR              | Number      | DATE_YEAR      |           |                             |

## Misc                                             

| SQL       | Return-Type | AQL       | Aggregate | Comment                  |
|-----------|-------------|-----------|-----------|--------------------------|
| ISNULL    | Boolean     | IS_NULL   |           | Parameter: 1->ignorieren |
| ISNUMERIC | Boolean     | IS_NUMBER |           |                          |

