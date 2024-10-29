MERGE INTO dbo.fSegmentacao_Simon AS TARGET
USING (VALUES(?, ?, ?, ?)) AS SOURCE (data, equipamento, segmentacao, updated_at)
ON target.equipuipamento = source.equipamento AND target.data = source.data
WHEN MATCHED THEN
    UPDATE SET target.segmentacao = source.segmentacao
    UPDATE SET target.updated_at = source.updated_at
WHEN NOT MATCHED THEN
    INSERT (data, equipamento, segmentacao, updated_at)
    VALUES(source.data, source.equipamento, source.segmentacao, source.updated_at)