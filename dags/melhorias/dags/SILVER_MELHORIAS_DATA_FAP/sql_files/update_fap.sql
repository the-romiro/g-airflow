SET XACT_ABORT ON;
SET NOCOUNT ON;

BEGIN TRY
  BEGIN TRANSACTION;

  --------------------------------------------------------------------------
  -- 1) Atualiza mel_aprovacao.dt_fap onde status = 'Aprovado' e dt_fap nulo
  --------------------------------------------------------------------------
  UPDATE [dbengenharia].[dbo].[mel_aprovacao]
  SET
    dt_fap = CAST(dt_aprovacao_eng AS DATE)
  WHERE
    1 = 1
    AND status = 'Aprovado'
    AND dt_fap IS NULL;

  --------------------------------------------------------------------------
  -- 2) Atualiza mel_ganhos.dt_fap
  --------------------------------------------------------------------------
  UPDATE g
  SET
    g.dt_fap = CAST(a.dt_aprovacao_eng AS DATE)
  FROM [dbengenharia].[dbo].[mel_ganhos] AS g
    INNER JOIN [dbengenharia].[dbo].[mel_aprovacao] AS a
    ON g.id_solicitacao_assinatura = a.[ID]
  WHERE
    1 = 1
    AND a.status = 'Aprovado'
    AND g.dt_fap IS NULL;

  COMMIT TRANSACTION;
END TRY
BEGIN CATCH
  IF XACT_STATE() <> 0
    ROLLBACK TRANSACTION;
  THROW;
END CATCH;
