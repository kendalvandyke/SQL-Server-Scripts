/*********************************************************************************************
Replication - Validate Merge Publication Identity Range Values v1.00 (2010-11-01)
(C) 2010, Kendal Van Dyke

Feedback: mailto:kendal.vandyke@gmail.com

License: 
	This query is free to download and use for personal, educational, and internal 
	corporate purposes, provided that this header is preserved. Redistribution or sale 
	of this query, in whole or in part, is prohibited without the author's express 
	written consent.
	
Note: 
	Execute this query on the DISTRIBUTOR

*********************************************************************************************/

USE distribution	-- Change this if your distribution database has a different name
GO
DECLARE @article SYSNAME
DECLARE @artid UNIQUEIDENTIFIER
DECLARE @subid UNIQUEIDENTIFIER
DECLARE @range_begin NUMERIC(38, 0)
DECLARE @range_end NUMERIC(38, 0)
DECLARE @next_range_begin NUMERIC(38, 0)
DECLARE @next_range_end NUMERIC(38, 0)

DECLARE curMergeIdentArticle CURSOR LOCAL FAST_FORWARD
FOR
    SELECT  sysmergearticles.name ,
            MSmerge_identity_range.artid ,
            MSmerge_identity_range.subid ,
            MSmerge_identity_range.range_begin ,
            MSmerge_identity_range.range_end ,
            MSmerge_identity_range.next_range_begin ,
            MSmerge_identity_range.next_range_end
    FROM    MSmerge_identity_range WITH ( NOLOCK )
            INNER JOIN sysmergearticles WITH ( NOLOCK ) ON MSmerge_identity_range.artid = sysmergearticles.artid
            INNER JOIN sysmergesubscriptions WITH ( NOLOCK ) ON MSmerge_identity_range.subid = sysmergesubscriptions.subid
    WHERE   MSmerge_identity_range.is_pub_range = 0
            AND sysmergesubscriptions.subscriber_server = @@SERVERNAME
OPEN curMergeIdentArticle
FETCH NEXT FROM curMergeIdentArticle INTO @article, @artid, @subid,
    @range_begin, @range_end, @next_range_begin, @next_range_end
WHILE @@fetch_status = 0 
    BEGIN

        SELECT  @article AS [article] ,
                @subid AS [subid] ,
                @artid AS [artid] ,
                @range_begin AS [range_begin] ,
                @range_end AS [range_end] ,
                @next_range_begin AS [next_range_begin] ,
                @next_range_end AS [next_range_end] ,
                IDENT_CURRENT(@article) AS [cur_ident_val]
 
        IF @range_begin IS NOT NULL
            AND @range_end IS NOT NULL
            AND @next_range_begin IS NOT NULL
            AND @next_range_end IS NOT NULL 
            BEGIN
                IF IDENT_CURRENT(@article) = @range_end 
                    BEGIN
                        DBCC CHECKIDENT (@article, RESEED, @next_range_begin) WITH no_infomsgs
                    END
                ELSE 
                    IF IDENT_CURRENT(@article) >= @next_range_end 
                        BEGIN
                            EXEC sys.sp_MSrefresh_publisher_idrange @article,
                                @subid, @artid, 2, 1
                            IF @@error <> 0 
                                PRINT 'ERROR'
                        END
            END
        FETCH NEXT FROM curMergeIdentArticle INTO @article, @artid, @subid,
            @range_begin, @range_end, @next_range_begin, @next_range_end

    END
CLOSE curMergeIdentArticle
DEALLOCATE curMergeIdentArticle


--exec sys.sp_MSrefresh_publisher_idrange 'CM_Category_Map_Term_Score', 'E55BE74B-2EE6-479A-9A4E-013B4EC42AEA', 'FF432420-B9EC-4CE9-942A-123B7B348E98', 2, 1
