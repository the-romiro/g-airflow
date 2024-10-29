MERGE INTO dbo.fSegmentacao_Simon AS TARGET
USING (SELECT ? as data, ? as equipamento, ? as segmentacao, ? as updated_at) AS SOURCE 
ON target.equipuipamento = source.equipamento AND target.data = source.data
WHEN MATCHED THEN
    UPDATE SET 
        target.segmentacao = source.segmentacao, 
        target.updated_at = source.updated_at   
WHEN NOT MATCHED THEN
    INSERT (data, equipamento, segmentacao, updated_at)
    VALUES(source.data, source.equipamento, source.segmentacao, source.updated_at)