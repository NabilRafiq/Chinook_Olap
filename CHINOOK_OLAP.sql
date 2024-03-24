-- Create Database Olap
CREATE DATABASE `Chinook_olap`;
use Chinook_olap;

-- Question 1 Customer_Dim Create Table
CREATE TABLE Customer_Dim (
    CustomerID INT PRIMARY KEY,
    FirstName NVARCHAR(100),
    LastName NVARCHAR(100),
    Address NVARCHAR(255),
    PostalCode NVARCHAR(20),
    Phone NVARCHAR(20),
    Fax NVARCHAR(20),
    Email NVARCHAR(255),
    CreateDate DATETIME,
    UpdateDate DATETIME
);
DELIMITER //
-- Question 1 Customer_Dim Insert Record

CREATE PROCEDURE InsertCustomerDimFromChinookOLTP()
BEGIN
    INSERT INTO Customer_Dim (CustomerID, FirstName, LastName, Address, PostalCode, Phone, Fax, Email, CreateDate, UpdateDate)
    SELECT 
        CustomerId,
        FirstName,
        LastName,
        Address,
        PostalCode,
        Phone,
        Fax,
        Email,
        NOW() AS CreateDate, -- Current timestamp for create date
        NOW() AS UpdateDate -- Current timestamp for update date
    FROM Chinook_oltp.customer ;
END //

DELIMITER ;
CALL InsertCustomerDimFromChinookOLTP();
select * from customer_dim;


-- Question 2 Track_Dim Create Table
CREATE TABLE Track_Dim (
    TrackID INT PRIMARY KEY,
    TrackName NVARCHAR(255),
    AlbumTitle NVARCHAR(255),
    ArtistName NVARCHAR(255),
    GenreName NVARCHAR(255),
    CreateDate DATETIME,
    UpdateDate DATETIME
);
DELIMITER //
-- Question 2 Track_Dim Insert Record

CREATE PROCEDURE InsertTrackDimFromChinookOLTP()
BEGIN
    -- Insert records into Track_Dim based on Chinook OLTP data
    INSERT INTO Track_Dim (TrackID, TrackName, AlbumTitle, ArtistName, GenreName, CreateDate, UpdateDate)
    SELECT 
        t.TrackId AS TrackID,
        t.Name AS TrackName,
        a.Title AS AlbumTitle,
        ar.Name AS ArtistName,
        g.Name AS GenreName,
        NOW() AS CreateDate, -- Current timestamp for create date
        NOW() AS UpdateDate -- Current timestamp for update date
    FROM chinook_oltp.track t
    JOIN chinook_oltp.album a ON t.AlbumId = a.AlbumId
    JOIN chinook_oltp.artist ar ON a.ArtistId = ar.ArtistId
    JOIN chinook_oltp.genre g ON t.GenreId = g.GenreId;
END //

DELIMITER ;
call InsertTrackDimFromChinookOLTP();
select * from Track_Dim;

-- Question 3 Date_Dim 

DELIMITER //
DROP PROCEDURE IF EXISTS chinook_olap.create_dim_date //
CREATE PROCEDURE chinook_olap.create_dim_date(IN start_date DATE, IN end_date DATE)
BEGIN

	DROP TABLE IF EXISTS chinook_olap.numbers_small;
	CREATE TABLE chinook_olap.numbers_small (number INT);
	INSERT INTO chinook_olap.numbers_small VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

	DROP TABLE IF EXISTS chinook_olap.numbers;
	CREATE TABLE chinook_olap.numbers (number BIGINT);
	INSERT INTO chinook_olap.numbers
	SELECT thousands.number * 1000 + hundreds.number * 100 + tens.number * 10 + ones.number
	FROM chinook_olap.numbers_small thousands, chinook_olap.numbers_small hundreds, chinook_olap.numbers_small tens, chinook_olap.numbers_small ones
	LIMIT 1000000;

	-- Create Date Dimension table
	DROP TABLE IF EXISTS chinook_olap.date_dim;
	CREATE TABLE chinook_olap.date_dim (
	date_id          BIGINT PRIMARY KEY,
	date             DATE NOT NULL,
	year             INT,
	month            CHAR(10),
	month_of_year    CHAR(2),
	day_of_month     INT,
	day              CHAR(10),
	day_of_week      INT,
	weekend          CHAR(10) NOT NULL DEFAULT "Weekday",
	day_of_year      INT,
	week_of_year     CHAR(2),
	quarter  INT,
	previous_day     date ,
	next_day         date ,
	UNIQUE KEY `date` (`date`));

	-- First populate with ids and Date
	-- Change year start and end to match your needs. The above sql creates records for year 2010.
	INSERT INTO chinook_olap.date_dim (date_id, date)
	SELECT year(DATE_ADD( start_date, INTERVAL number DAY ))*10000+
	month(DATE_ADD( start_date, INTERVAL number DAY ))*100+
	day(DATE_ADD( start_date, INTERVAL number DAY ))date_id
	, 

	DATE_ADD( start_date, INTERVAL number DAY )
	FROM chinook_olap.numbers
	WHERE DATE_ADD( start_date, INTERVAL number DAY ) BETWEEN start_date AND end_date
	ORDER BY number;
	SET SQL_SAFE_UPDATES = 0;
	-- Update other columns based on the date.
	UPDATE chinook_olap.date_dim SET
	year            = DATE_FORMAT( date, "%Y" ),
	month           = DATE_FORMAT( date, "%M"),
	month_of_year   = DATE_FORMAT( date, "%m"),
	day_of_month    = DATE_FORMAT( date, "%d" ),
	day             = DATE_FORMAT( date, "%W" ),
	day_of_week     = DAYOFWEEK(date),
	weekend         = IF( DATE_FORMAT( date, "%W" ) IN ('Saturday','Sunday'), 'Weekend', 'Weekday'),
	day_of_year     = DATE_FORMAT( date, "%j" ),
	week_of_year    = DATE_FORMAT( date, "%V" ),
	quarter         = QUARTER(date),
	previous_day    = DATE_ADD(date, INTERVAL -1 DAY),
	next_day        = DATE_ADD(date, INTERVAL 1 DAY);

	drop table if exists chinook_olap.numbers;
	drop table if exists chinook_olap.numbers_small;
    
END //
DELIMITER ;
-- select minimum date from invoice
select min(invoicedate) as mindate from chinook_oltp.invoice;
-- select maximum date from invoice
select max(invoicedate) as maxdate from chinook_oltp.invoice;

-- passed max and minimum date obtained as parameter
CALL chinook_olap.create_dim_date('2009-01-01', '2013-12-22');
select * from date_dim;


-- Question 4 Invoice_Fact Create Table

CREATE TABLE Invoice_Fact (
    InvoiceID INT,
    CustomerID INT,
    TrackID INT,
    SaleDateID BIGINT, -- Adjust data type according to Date_Dim
    TotalQuantity INT,
    TotalAmount DECIMAL(10, 2), -- Assuming TotalAmount is a decimal value with 2 decimal places

    PRIMARY KEY (InvoiceID, TrackID), -- Composite primary key

    FOREIGN KEY (CustomerID) REFERENCES Customer_Dim(CustomerID),
    FOREIGN KEY (TrackID) REFERENCES Track_Dim(TrackID),
    FOREIGN KEY (SaleDateID) REFERENCES Date_Dim(date_id)
);

-- Question 4 Invoice_Fact Insert Record

DELIMITER //

CREATE PROCEDURE InsertIntoInvoiceFact()
BEGIN
    -- Insert records into Invoice_Fact table
    INSERT INTO Invoice_Fact (InvoiceID, CustomerID, TrackID, SaleDateID, TotalQuantity, TotalAmount)
    SELECT 
        i.InvoiceID,
        c.CustomerID,
        t.TrackID,
        d.date_id,
        il.Quantity AS TotalQuantity,
        (il.UnitPrice * il.Quantity) AS TotalAmount
    FROM chinook_oltp.invoice i
    JOIN chinook_oltp.invoiceline il ON i.invoiceid = il.invoiceid
    JOIN customer_dim c ON i.customerid = c.customerid
    JOIN track_dim t ON il.trackid = t.trackid
    JOIN Date_Dim d ON i.invoicedate = d.date;
END //

DELIMITER ;
call InsertIntoInvoiceFact();
select * from invoice_fact;


-- Question 5 Chinook_DataMart

CREATE VIEW Chinook_Datamart AS
SELECT 
    IFNULL(IFNULL(IFNULL(IFNULL(f.InvoiceID, c.CustomerID), d.date_id), t.TrackID), '') AS ID,
    f.InvoiceID,
    c.CustomerID,
    t.TrackID,
    d.date_id AS SaleDateID,
    f.TotalQuantity,
    f.TotalAmount,
    c.FirstName,
    c.LastName,
    c.Address,
    c.PostalCode,
    c.Phone,
    c.Fax,
    c.Email,
    d.date AS SaleDate,
    d.year,
    d.month,
    d.month_of_year,
    d.day_of_month,
    d.day,
    d.day_of_week,
    d.weekend,
    d.day_of_year,
    d.week_of_year,
    d.quarter,
    d.previous_day,
    d.next_day,
    t.TrackName,
    t.AlbumTitle,
    t.ArtistName,
    t.GenreName
FROM Invoice_Fact f
LEFT JOIN Customer_Dim c ON f.CustomerID = c.CustomerID
LEFT JOIN Date_Dim d ON f.SaleDateID = d.date_id
LEFT JOIN Track_Dim t ON f.TrackID = t.TrackID;
select * from Chinook_Datamart