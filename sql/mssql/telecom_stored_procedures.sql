-- Log ETL execution
CREATE PROCEDURE sp_LogETLRun
    @SourceName NVARCHAR(100),
    @Partition NVARCHAR(100)
AS
BEGIN
    INSERT INTO ETL_ExecutionLog(SourceName, Partition) 
    VALUES (@SourceName, @Partition);
END;
GO


-- Validate customer has active contract
CREATE FUNCTION fn_HasActiveContract (@CustomerID INT)
RETURNS BIT
AS
BEGIN
    DECLARE @Has BIT = 0;

    IF EXISTS (
        SELECT 1 FROM Contract
        WHERE CustomerID = @CustomerID
        AND Status = 'Active'
    )
        SET @Has = 1;

    RETURN @Has;
END;
GO


-- Trigger to automatically close contracts
CREATE TRIGGER trg_CloseContracts
ON Contract
AFTER UPDATE
AS
BEGIN
    UPDATE Contract
    SET Status = 'Expired'
    WHERE EndDate < CAST(GETDATE() AS DATE)
    AND Status = 'Active';
END;
GO


